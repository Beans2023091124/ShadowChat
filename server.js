import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

import express from "express";
import { Server } from "socket.io";

let DatabaseSync = null;

try {
  ({ DatabaseSync } = await import("node:sqlite"));
} catch {
  DatabaseSync = null;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 40 * 1024 * 1024
});

const PORT = Number(process.env.PORT) || 3000;

const MAX_MESSAGE_LENGTH = 1200;
const MAX_ATTACHMENT_COUNT = 4;
const MAX_ATTACHMENT_SIZE = 5 * 1024 * 1024;
const MAX_DATA_URL_LENGTH = 7_000_000;
const MAX_AVATAR_DATA_URL_LENGTH = 2_500_000;
const MAX_CHAT_NICKNAME_LENGTH = 48;
const MAX_CHAT_WALLPAPER_LENGTH = 2048;
const MAX_CALL_CHAT_LENGTH = 500;
const MAX_CALL_CHAT_MESSAGES = 160;
const MAX_GROUP_MEMBER_ADDS_PER_ACTION = 8;
const MAX_LINK_PREVIEW_URL_LENGTH = 2048;
const MAX_LINK_PREVIEW_TITLE_LENGTH = 140;
const MAX_LINK_PREVIEW_DESCRIPTION_LENGTH = 240;
const MAX_LINK_PREVIEW_SITE_NAME_LENGTH = 80;
const MAX_LINK_PREVIEW_AUTHOR_LENGTH = 80;
const LINK_PREVIEW_FETCH_TIMEOUT_MS = 4500;
const LINK_PREVIEW_MAX_REDIRECTS = 3;
const LINK_PREVIEW_MAX_HTML_BYTES = 220_000;
const LINK_PREVIEW_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const CHAT_THEMES = new Set(["default", "slate", "forest", "sunset", "night"]);
const DATA_DIR = process.env.SHADOW_CHAT_DATA_DIR
  ? path.resolve(process.env.SHADOW_CHAT_DATA_DIR)
  : path.join(__dirname, "data");
const DB_PATH = process.env.SHADOW_CHAT_DB_PATH
  ? path.resolve(process.env.SHADOW_CHAT_DB_PATH)
  : path.join(DATA_DIR, "shadow-chat.sqlite");
const STATE_SNAPSHOT_PATH = process.env.SHADOW_CHAT_STATE_PATH
  ? path.resolve(process.env.SHADOW_CHAT_STATE_PATH)
  : path.join(DATA_DIR, "shadow-chat-state.json");
const STATE_ENCRYPTION_SECRET = String(process.env.SHADOW_CHAT_STATE_ENCRYPTION_KEY ?? "");

app.use(express.static(path.join(__dirname, "public")));

const users = new Map();
const onlineUsers = new Map();
const socketToUser = new Map();
const chats = new Map();
const activeCalls = new Map();
const sessions = new Map();
const sessionDetails = new Map();
const socketToSession = new Map();
const sessionToSockets = new Map();
const sessionsByUser = new Map();
const linkPreviewCache = new Map();
const linkPreviewRequests = new Map();
let persistenceScheduled = false;

let persistenceBackend = "json";
let selectStateStatement = null;
let upsertStateStatement = null;

try {
  fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
  fs.mkdirSync(path.dirname(STATE_SNAPSHOT_PATH), { recursive: true });
} catch (error) {
  const reason = error instanceof Error ? error.message : String(error);
  console.warn(
    `Local persistence directories unavailable (${reason}). Continuing without disk persistence.`
  );
}

if (DatabaseSync) {
  try {
    const db = new DatabaseSync(DB_PATH);
    db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS app_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `);

    selectStateStatement = db.prepare(`
      SELECT value
      FROM app_state
      WHERE key = 'state_json'
    `);

    upsertStateStatement = db.prepare(`
      INSERT INTO app_state (key, value)
      VALUES ('state_json', ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `);
    persistenceBackend = "sqlite";
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    console.warn(`SQLite persistence unavailable (${reason}). Falling back to JSON snapshot.`);
  }
} else {
  console.warn("SQLite module unavailable. Falling back to JSON snapshot.");
}

function normalizeUserName(value) {
  return String(value ?? "").trim().toLowerCase();
}

function cleanDisplayName(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 24);
}

function cleanGroupName(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 36);
}

function cleanPassword(value) {
  return String(value ?? "").slice(0, 64);
}

const PASSWORD_HASH_BYTES = 64;
const PASSWORD_SALT_BYTES = 16;
const ENCRYPTED_STATE_PREFIX = "enc:v1";
const stateEncryptionKey = STATE_ENCRYPTION_SECRET
  ? crypto.createHash("sha256").update(STATE_ENCRYPTION_SECRET).digest()
  : null;

function hashPassword(password, salt = crypto.randomBytes(PASSWORD_SALT_BYTES).toString("hex")) {
  const passwordHash = crypto.scryptSync(password, salt, PASSWORD_HASH_BYTES).toString("hex");
  return {
    passwordSalt: salt,
    passwordHash
  };
}

function setUserPassword(user, password) {
  const { passwordSalt, passwordHash } = hashPassword(password);
  user.passwordSalt = passwordSalt;
  user.passwordHash = passwordHash;
  delete user.password;
}

function isHashedPassword(user) {
  return Boolean(
    user &&
      typeof user.passwordHash === "string" &&
      typeof user.passwordSalt === "string" &&
      /^[a-f0-9]{128}$/i.test(user.passwordHash) &&
      /^[a-f0-9]{32}$/i.test(user.passwordSalt)
  );
}

function verifyUserPassword(user, password) {
  if (!user || !password) {
    return false;
  }

  if (isHashedPassword(user)) {
    try {
      const candidateHash = crypto.scryptSync(password, user.passwordSalt, PASSWORD_HASH_BYTES).toString(
        "hex"
      );
      const candidateBuffer = Buffer.from(candidateHash, "hex");
      const storedBuffer = Buffer.from(user.passwordHash, "hex");

      if (candidateBuffer.length !== storedBuffer.length) {
        return false;
      }

      return crypto.timingSafeEqual(candidateBuffer, storedBuffer);
    } catch {
      return false;
    }
  }

  return cleanPassword(user.password) === password;
}

function maybeMigrateLegacyPassword(user) {
  if (!user || isHashedPassword(user)) {
    return false;
  }

  const legacyPassword = cleanPassword(user.password);
  if (legacyPassword.length < 4) {
    return false;
  }

  setUserPassword(user, legacyPassword);
  return true;
}

function encryptStatePayload(serializedState) {
  if (!stateEncryptionKey) {
    return serializedState;
  }

  try {
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv("aes-256-gcm", stateEncryptionKey, iv);
    const encrypted = Buffer.concat([cipher.update(serializedState, "utf8"), cipher.final()]);
    const authTag = cipher.getAuthTag();

    return [
      ENCRYPTED_STATE_PREFIX,
      iv.toString("base64"),
      authTag.toString("base64"),
      encrypted.toString("base64")
    ].join(":");
  } catch (error) {
    console.error("Failed to encrypt state for persistence:", error);
    return null;
  }
}

function decryptStatePayload(storedValue) {
  const rawValue = String(storedValue ?? "");
  const prefix = `${ENCRYPTED_STATE_PREFIX}:`;

  if (!rawValue.startsWith(prefix)) {
    return rawValue;
  }

  if (!stateEncryptionKey) {
    console.error(
      "Encrypted state detected, but SHADOW_CHAT_STATE_ENCRYPTION_KEY is not set. State restore was skipped."
    );
    return null;
  }

  const segments = rawValue.split(":");
  if (segments.length !== 4) {
    console.error("Encrypted state format is invalid and was ignored.");
    return null;
  }

  const [, ivBase64, authTagBase64, encryptedBase64] = segments;

  try {
    const iv = Buffer.from(ivBase64, "base64");
    const authTag = Buffer.from(authTagBase64, "base64");
    const encrypted = Buffer.from(encryptedBase64, "base64");
    const decipher = crypto.createDecipheriv("aes-256-gcm", stateEncryptionKey, iv);
    decipher.setAuthTag(authTag);
    const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);
    return decrypted.toString("utf8");
  } catch (error) {
    console.error("Failed to decrypt persisted state. Check SHADOW_CHAT_STATE_ENCRYPTION_KEY.", error);
    return null;
  }
}

function cleanAlias(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 24);
}

function cleanChatNickname(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, MAX_CHAT_NICKNAME_LENGTH);
}

function normalizeChatTheme(value) {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();

  return CHAT_THEMES.has(normalized) ? normalized : "default";
}

function normalizeChatWallpaper(value) {
  const raw = String(value ?? "").trim();

  if (!raw) {
    return null;
  }

  if (raw.length > MAX_CHAT_WALLPAPER_LENGTH) {
    return null;
  }

  if (raw.startsWith("data:image/")) {
    return raw;
  }

  try {
    const parsed = new URL(raw);

    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return parsed.toString();
    }
  } catch {}

  return null;
}

function normalizeViewerChatPreference(rawValue) {
  const source = rawValue && typeof rawValue === "object" ? rawValue : {};
  const nickname = cleanChatNickname(source.nickname);
  const theme = normalizeChatTheme(source.theme);
  const wallpaper = normalizeChatWallpaper(source.wallpaper);

  if (!nickname && theme === "default" && !wallpaper) {
    return null;
  }

  return {
    nickname: nickname || null,
    theme,
    wallpaper: wallpaper || null
  };
}

function cleanFileName(value) {
  const fallback = "file";
  const cleaned = String(value ?? "")
    .trim()
    .replace(/[<>:"/\\|?*\u0000-\u001f]/g, "_")
    .slice(0, 80);

  return cleaned || fallback;
}

function decodeHtmlEntities(value) {
  return String(value ?? "").replace(
    /&(#\d+|#x[0-9a-f]+|[a-z]+);/gi,
    (match, entityRaw) => {
      const entity = String(entityRaw ?? "").toLowerCase();

      if (entity === "amp") {
        return "&";
      }
      if (entity === "lt") {
        return "<";
      }
      if (entity === "gt") {
        return ">";
      }
      if (entity === "quot") {
        return "\"";
      }
      if (entity === "apos" || entity === "#39") {
        return "'";
      }
      if (entity.startsWith("#x")) {
        const codePoint = Number.parseInt(entity.slice(2), 16);
        if (!Number.isFinite(codePoint) || codePoint < 0 || codePoint > 0x10ffff) {
          return match;
        }
        return String.fromCodePoint(codePoint);
      }
      if (entity.startsWith("#")) {
        const codePoint = Number.parseInt(entity.slice(1), 10);
        if (!Number.isFinite(codePoint) || codePoint < 0 || codePoint > 0x10ffff) {
          return match;
        }
        return String.fromCodePoint(codePoint);
      }

      return match;
    }
  );
}

function cleanLinkPreviewText(value, maxLength) {
  const normalized = decodeHtmlEntities(String(value ?? ""))
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!normalized) {
    return "";
  }

  return normalized.slice(0, Math.max(1, Number(maxLength) || 120));
}

function trimUrlToken(rawToken) {
  let token = String(rawToken ?? "");

  while (token.length > 0 && /[),.!?:;]$/.test(token)) {
    token = token.slice(0, -1);
  }

  return token;
}

function isPrivateOrDisallowedHostname(hostname) {
  const host = String(hostname ?? "").trim().toLowerCase();

  if (!host) {
    return true;
  }

  if (
    host === "localhost" ||
    host.endsWith(".localhost") ||
    host.endsWith(".local") ||
    host.endsWith(".internal") ||
    host.endsWith(".home") ||
    host.endsWith(".lan")
  ) {
    return true;
  }

  const ipv6ZoneStripped = host.includes("%") ? host.slice(0, host.indexOf("%")) : host;
  const ipVersion = net.isIP(ipv6ZoneStripped);

  if (ipVersion === 6) {
    if (
      ipv6ZoneStripped === "::1" ||
      ipv6ZoneStripped.startsWith("fe80:") ||
      ipv6ZoneStripped.startsWith("fc") ||
      ipv6ZoneStripped.startsWith("fd")
    ) {
      return true;
    }

    if (ipv6ZoneStripped.startsWith("::ffff:")) {
      return isPrivateOrDisallowedHostname(ipv6ZoneStripped.slice(7));
    }

    return false;
  }

  if (ipVersion === 4) {
    const octets = ipv6ZoneStripped.split(".").map((part) => Number.parseInt(part, 10));

    if (octets.length !== 4 || octets.some((part) => !Number.isFinite(part) || part < 0 || part > 255)) {
      return true;
    }

    const [a, b] = octets;

    if (a === 0 || a === 10 || a === 127) {
      return true;
    }

    if (a === 169 && b === 254) {
      return true;
    }

    if (a === 172 && b >= 16 && b <= 31) {
      return true;
    }

    if (a === 192 && b === 168) {
      return true;
    }

    if (a === 100 && b >= 64 && b <= 127) {
      return true;
    }

    if (a === 198 && (b === 18 || b === 19)) {
      return true;
    }

    if (a >= 224) {
      return true;
    }

    return false;
  }

  return !host.includes(".");
}

function normalizeHttpUrl(rawValue) {
  const raw = String(rawValue ?? "").trim();

  if (!raw || raw.length > MAX_LINK_PREVIEW_URL_LENGTH) {
    return null;
  }

  const prefixed = raw.startsWith("www.") ? `https://${raw}` : raw;

  try {
    const parsed = new URL(prefixed);

    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return null;
    }

    if (isPrivateOrDisallowedHostname(parsed.hostname)) {
      return null;
    }

    return parsed.toString();
  } catch {
    return null;
  }
}

function firstPreviewUrlFromText(textValue) {
  const text = String(textValue ?? "");
  const urlPattern = /\b((https?:\/\/|www\.)[^\s]+)/gi;
  let match;

  while ((match = urlPattern.exec(text)) !== null) {
    const token = trimUrlToken(match[0]);
    const normalized = normalizeHttpUrl(token);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function normalizePreviewCacheLabel(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/+$/, "");
}

function previewProviderKeyFromHostname(hostname) {
  const host = String(hostname ?? "").trim().toLowerCase();

  if (!host) {
    return "website";
  }

  if (
    host === "youtu.be" ||
    host === "youtube.com" ||
    host.endsWith(".youtube.com")
  ) {
    return "youtube";
  }

  if (host === "twitter.com" || host === "x.com" || host.endsWith(".x.com")) {
    return "twitter";
  }

  if (host === "github.com" || host.endsWith(".github.com")) {
    return "github";
  }

  if (
    host === "tiktok.com" ||
    host.endsWith(".tiktok.com") ||
    host === "vm.tiktok.com" ||
    host === "vt.tiktok.com"
  ) {
    return "tiktok";
  }

  return "website";
}

function isYouTubeUrl(rawUrl) {
  try {
    const parsed = new URL(String(rawUrl ?? ""));
    const provider = previewProviderKeyFromHostname(parsed.hostname);
    return provider === "youtube";
  } catch {
    return false;
  }
}

function isTikTokUrl(rawUrl) {
  try {
    const parsed = new URL(String(rawUrl ?? ""));
    const provider = previewProviderKeyFromHostname(parsed.hostname);
    return provider === "tiktok";
  } catch {
    return false;
  }
}

function sanitizeLinkPreview(rawPreview) {
  if (!rawPreview || typeof rawPreview !== "object") {
    return null;
  }

  const normalizedUrl = normalizeHttpUrl(rawPreview.url);

  if (!normalizedUrl) {
    return null;
  }

  const parsedUrl = new URL(normalizedUrl);
  const title =
    cleanLinkPreviewText(rawPreview.title, MAX_LINK_PREVIEW_TITLE_LENGTH) || parsedUrl.hostname;
  const description = cleanLinkPreviewText(
    rawPreview.description,
    MAX_LINK_PREVIEW_DESCRIPTION_LENGTH
  );
  const siteName = cleanLinkPreviewText(rawPreview.siteName, MAX_LINK_PREVIEW_SITE_NAME_LENGTH);
  const imageUrl = normalizeHttpUrl(rawPreview.imageUrl);
  const authorName = cleanLinkPreviewText(rawPreview.authorName, MAX_LINK_PREVIEW_AUTHOR_LENGTH);
  const providerKey = previewProviderKeyFromHostname(parsedUrl.hostname);

  return {
    url: normalizedUrl,
    title,
    description: description || null,
    siteName: siteName || parsedUrl.hostname,
    imageUrl: imageUrl || null,
    authorName: authorName || null,
    providerKey
  };
}

function isWeakVideoPreview(rawPreview) {
  const preview = sanitizeLinkPreview(rawPreview);

  if (!preview) {
    return true;
  }

  const provider = String(preview.providerKey ?? "").trim().toLowerCase();

  if (provider !== "youtube" && provider !== "tiktok") {
    return false;
  }

  let hostLabel = "";

  try {
    hostLabel = new URL(preview.url).hostname;
  } catch {}

  const normalizedTitle = normalizePreviewCacheLabel(preview.title);
  const normalizedHost = normalizePreviewCacheLabel(hostLabel);
  const normalizedSite = normalizePreviewCacheLabel(preview.siteName);
  const missingAuthor = !String(preview.authorName ?? "").trim();
  const lowQualityTitle =
    !normalizedTitle ||
    normalizedTitle === normalizedHost ||
    normalizedTitle === normalizedSite ||
    normalizedTitle === "youtube" ||
    normalizedTitle === "tiktok";

  return missingAuthor || lowQualityTitle;
}

function parseMetaAttributes(tag) {
  const attributes = {};
  const attributePattern = /([a-zA-Z:-]+)\s*=\s*("([^"]*)"|'([^']*)'|([^\s"'>]+))/g;
  let match;

  while ((match = attributePattern.exec(tag)) !== null) {
    const key = String(match[1] ?? "").trim().toLowerCase();
    const value = String(match[3] ?? match[4] ?? match[5] ?? "").trim();

    if (key) {
      attributes[key] = value;
    }
  }

  return attributes;
}

function extractPreviewMetadata(html) {
  const source = String(html ?? "");
  const metadata = {};
  const metaTagPattern = /<meta\s+[^>]*>/gi;
  let tagMatch;

  while ((tagMatch = metaTagPattern.exec(source)) !== null) {
    const tag = String(tagMatch[0] ?? "");
    const attributes = parseMetaAttributes(tag);
    const key = String(attributes.property ?? attributes.name ?? attributes.itemprop ?? "")
      .trim()
      .toLowerCase();
    const content = String(attributes.content ?? "").trim();

    if (!key || !content || metadata[key]) {
      continue;
    }

    metadata[key] = content;
  }

  const titleMatch = source.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const titleFromTag = titleMatch ? cleanLinkPreviewText(titleMatch[1], MAX_LINK_PREVIEW_TITLE_LENGTH) : "";

  return {
    title: metadata["og:title"] || metadata["twitter:title"] || titleFromTag,
    description:
      metadata["og:description"] || metadata["twitter:description"] || metadata.description || "",
    siteName: metadata["og:site_name"] || "",
    imageUrl: metadata["og:image"] || metadata["twitter:image"] || "",
    author: metadata.author || metadata["twitter:creator"] || metadata["og:video:tag"] || ""
  };
}

async function fetchYouTubeOEmbedPreview(url, signal) {
  const normalizedUrl = normalizeHttpUrl(url);

  if (!normalizedUrl || !isYouTubeUrl(normalizedUrl)) {
    return null;
  }

  try {
    const endpoint = new URL("https://www.youtube.com/oembed");
    endpoint.searchParams.set("url", normalizedUrl);
    endpoint.searchParams.set("format", "json");

    const response = await fetch(endpoint, {
      method: "GET",
      redirect: "follow",
      signal,
      headers: {
        "user-agent":
          "Mozilla/5.0 (compatible; ShadowChatLinkPreview/1.0; +https://shadow-chat.site)"
      }
    });

    if (!response.ok) {
      return null;
    }

    const payload = await response.json();

    return sanitizeLinkPreview({
      url: normalizedUrl,
      title: String(payload?.title ?? "").trim(),
      description: "",
      siteName: String(payload?.provider_name ?? "YouTube").trim() || "YouTube",
      imageUrl: String(payload?.thumbnail_url ?? "").trim(),
      authorName: String(payload?.author_name ?? "").trim(),
      providerKey: "youtube"
    });
  } catch {
    return null;
  }
}

async function fetchTikTokOEmbedPreview(url, signal) {
  const normalizedUrl = normalizeHttpUrl(url);

  if (!normalizedUrl || !isTikTokUrl(normalizedUrl)) {
    return null;
  }

  try {
    const endpoint = new URL("https://www.tiktok.com/oembed");
    endpoint.searchParams.set("url", normalizedUrl);

    const response = await fetch(endpoint, {
      method: "GET",
      redirect: "follow",
      signal,
      headers: {
        "user-agent":
          "Mozilla/5.0 (compatible; ShadowChatLinkPreview/1.0; +https://shadow-chat.site)"
      }
    });

    if (!response.ok) {
      return null;
    }

    const payload = await response.json();
    const html = String(payload?.html ?? "");
    const citeMatch = html.match(/cite=["']([^"']+)["']/i);
    const htmlUrlMatch = html.match(/https?:\/\/(?:www\.)?tiktok\.com\/[^"'<> ]+\/video\/\d+/i);
    const embedProductId = String(payload?.embed_product_id ?? "").trim();
    const rawAuthorName = String(payload?.author_name ?? "").trim();
    let normalizedAuthorName =
      rawAuthorName && !rawAuthorName.startsWith("@") ? `@${rawAuthorName}` : rawAuthorName;
    let canonicalUrl = normalizedUrl;

    if (citeMatch && citeMatch[1]) {
      const normalizedCite = normalizeHttpUrl(citeMatch[1]);
      if (normalizedCite) {
        canonicalUrl = normalizedCite;
      }
    } else if (htmlUrlMatch && htmlUrlMatch[0]) {
      const normalizedFromHtml = normalizeHttpUrl(htmlUrlMatch[0]);
      if (normalizedFromHtml) {
        canonicalUrl = normalizedFromHtml;
      }
    } else if (/^\d{6,}$/.test(embedProductId)) {
      canonicalUrl = `https://www.tiktok.com/@_/video/${embedProductId}`;
    }

    if (!normalizedAuthorName) {
      const usernameMatch = canonicalUrl.match(/(?:www\.)?tiktok\.com\/(@[^/?#]+)/i);

      if (usernameMatch && usernameMatch[1]) {
        normalizedAuthorName = String(usernameMatch[1]).trim();
      }
    }

    return sanitizeLinkPreview({
      url: canonicalUrl,
      title: String(payload?.title ?? "").trim(),
      description: "",
      siteName: String(payload?.provider_name ?? "TikTok").trim() || "TikTok",
      imageUrl: String(payload?.thumbnail_url ?? "").trim(),
      authorName: normalizedAuthorName,
      providerKey: "tiktok"
    });
  } catch {
    return null;
  }
}

async function readResponseTextWithLimit(response, maxBytes) {
  const maxLength = Math.max(1024, Number(maxBytes) || LINK_PREVIEW_MAX_HTML_BYTES);

  if (!response.body || typeof response.body.getReader !== "function") {
    const text = await response.text();
    return text.slice(0, maxLength);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  let bytesRead = 0;
  let done = false;

  while (!done) {
    const chunk = await reader.read();
    done = chunk.done === true;

    if (done) {
      break;
    }

    const value = chunk.value instanceof Uint8Array ? chunk.value : new Uint8Array();
    bytesRead += value.byteLength;

    if (bytesRead > maxLength) {
      const allowed = Math.max(0, value.byteLength - (bytesRead - maxLength));

      if (allowed > 0) {
        text += decoder.decode(value.slice(0, allowed), { stream: true });
      }

      try {
        await reader.cancel();
      } catch {}
      break;
    }

    text += decoder.decode(value, { stream: true });
  }

  text += decoder.decode();
  return text;
}

async function fetchPreviewResponseWithRedirects(initialUrl, signal) {
  let url = normalizeHttpUrl(initialUrl);

  if (!url) {
    return null;
  }

  const headers = {
    "user-agent":
      "Mozilla/5.0 (compatible; ShadowChatLinkPreview/1.0; +https://shadow-chat.site)"
  };

  for (let redirectCount = 0; redirectCount <= LINK_PREVIEW_MAX_REDIRECTS; redirectCount += 1) {
    const response = await fetch(url, {
      method: "GET",
      redirect: "manual",
      signal,
      headers
    });

    const status = Number(response.status);

    if (status >= 300 && status < 400) {
      const location = String(response.headers.get("location") ?? "").trim();

      if (!location) {
        return null;
      }

      const redirected = normalizeHttpUrl(new URL(location, url).toString());

      if (!redirected) {
        return null;
      }

      url = redirected;
      continue;
    }

    if (status < 200 || status >= 300) {
      return null;
    }

    const resolvedUrl = normalizeHttpUrl(response.url || url);

    if (!resolvedUrl) {
      return null;
    }

    return {
      response,
      resolvedUrl
    };
  }

  return null;
}

function getCachedLinkPreview(url) {
  const key = String(url ?? "").trim();

  if (!key) {
    return { hit: false, preview: null };
  }

  const entry = linkPreviewCache.get(key);

  if (!entry) {
    return { hit: false, preview: null };
  }

  if (Number(entry.expiresAt ?? 0) <= Date.now()) {
    linkPreviewCache.delete(key);
    return { hit: false, preview: null };
  }

  return {
    hit: true,
    preview: entry.preview ? { ...entry.preview } : null
  };
}

function setCachedLinkPreview(url, preview) {
  const key = String(url ?? "").trim();

  if (!key) {
    return;
  }

  linkPreviewCache.set(key, {
    preview: preview ? { ...preview } : null,
    expiresAt: Date.now() + LINK_PREVIEW_CACHE_TTL_MS
  });

  if (linkPreviewCache.size <= 800) {
    return;
  }

  const oldestKey = linkPreviewCache.keys().next().value;

  if (oldestKey) {
    linkPreviewCache.delete(oldestKey);
  }
}

async function fetchLinkPreview(url) {
  const normalizedUrl = normalizeHttpUrl(url);

  if (!normalizedUrl) {
    return null;
  }

  const timeoutController = new AbortController();
  const timeoutId = setTimeout(() => {
    timeoutController.abort();
  }, LINK_PREVIEW_FETCH_TIMEOUT_MS);

  try {
    if (isYouTubeUrl(normalizedUrl)) {
      const youtubePreview = await fetchYouTubeOEmbedPreview(
        normalizedUrl,
        timeoutController.signal
      );

      if (youtubePreview) {
        return youtubePreview;
      }
    }

    if (isTikTokUrl(normalizedUrl)) {
      const tiktokPreview = await fetchTikTokOEmbedPreview(
        normalizedUrl,
        timeoutController.signal
      );

      if (tiktokPreview) {
        return tiktokPreview;
      }
    }

    const responseBundle = await fetchPreviewResponseWithRedirects(
      normalizedUrl,
      timeoutController.signal
    );

    if (!responseBundle) {
      return null;
    }

    const { response, resolvedUrl } = responseBundle;
    const contentType = String(response.headers.get("content-type") ?? "").toLowerCase();

    if (!contentType.includes("text/html") && !contentType.includes("application/xhtml+xml")) {
      return null;
    }

    const html = await readResponseTextWithLimit(response, LINK_PREVIEW_MAX_HTML_BYTES);
    const extracted = extractPreviewMetadata(html);
    const resolved = new URL(resolvedUrl);
    const siteName =
      cleanLinkPreviewText(extracted.siteName, MAX_LINK_PREVIEW_SITE_NAME_LENGTH) || resolved.hostname;
    const title =
      cleanLinkPreviewText(extracted.title, MAX_LINK_PREVIEW_TITLE_LENGTH) || resolved.hostname;
    const description = cleanLinkPreviewText(
      extracted.description,
      MAX_LINK_PREVIEW_DESCRIPTION_LENGTH
    );
    let imageUrl = "";

    if (extracted.imageUrl) {
      try {
        imageUrl = new URL(extracted.imageUrl, resolvedUrl).toString();
      } catch {
        imageUrl = "";
      }
    }

    return sanitizeLinkPreview({
      url: resolvedUrl,
      title,
      description,
      siteName,
      imageUrl,
      authorName: extracted.author
    });
  } catch {
    return null;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function resolveLinkPreview(url) {
  const normalizedUrl = normalizeHttpUrl(url);

  if (!normalizedUrl) {
    return null;
  }

  const cached = getCachedLinkPreview(normalizedUrl);

  if (cached.hit) {
    return cached.preview;
  }

  const existingRequest = linkPreviewRequests.get(normalizedUrl);

  if (existingRequest) {
    return existingRequest;
  }

  const request = fetchLinkPreview(normalizedUrl)
    .then((preview) => {
      setCachedLinkPreview(normalizedUrl, preview);
      return preview;
    })
    .catch(() => {
      setCachedLinkPreview(normalizedUrl, null);
      return null;
    })
    .finally(() => {
      linkPreviewRequests.delete(normalizedUrl);
    });

  linkPreviewRequests.set(normalizedUrl, request);
  return request;
}

function linkPreviewForText(textValue) {
  const previewUrl = firstPreviewUrlFromText(textValue);

  if (!previewUrl) {
    return {
      previewUrl: null,
      preview: null,
      cacheHit: false
    };
  }

  const cacheResult = getCachedLinkPreview(previewUrl);
  const cachedPreview = cacheResult.preview;
  const shouldRefetch = cacheResult.hit && isWeakVideoPreview(cachedPreview);
  const preview = shouldRefetch ? null : cachedPreview;

  return {
    previewUrl,
    preview,
    cacheHit: cacheResult.hit && !shouldRefetch
  };
}

function weakVideoPreviewCandidates(chat, limit = 3) {
  if (!chat || !Array.isArray(chat.messages) || chat.messages.length === 0) {
    return [];
  }

  const maxCount = Math.max(1, Math.min(8, Number(limit) || 3));
  const candidates = [];

  for (
    let messageIndex = chat.messages.length - 1;
    messageIndex >= 0 && candidates.length < maxCount;
    messageIndex -= 1
  ) {
    const message = chat.messages[messageIndex];

    if (!message || message.deleted) {
      continue;
    }

    const previewUrl = firstPreviewUrlFromText(message.text);

    if (!previewUrl) {
      continue;
    }

    if (!isYouTubeUrl(previewUrl) && !isTikTokUrl(previewUrl)) {
      continue;
    }

    if (!isWeakVideoPreview(message.linkPreview)) {
      continue;
    }

    candidates.push({
      messageId: message.id,
      previewUrl
    });
  }

  return candidates;
}

function hydrateWeakVideoPreviewsForChat(chat, limit = 3) {
  const candidates = weakVideoPreviewCandidates(chat, limit);

  for (const candidate of candidates) {
    hydrateMessageLinkPreview(chat.id, candidate.messageId, candidate.previewUrl).catch(() => {});
  }
}

async function hydrateMessageLinkPreview(chatId, messageId, expectedUrl) {
  const normalizedExpectedUrl = normalizeHttpUrl(expectedUrl);

  if (!chatId || !messageId || !normalizedExpectedUrl) {
    return;
  }

  const preview = await resolveLinkPreview(normalizedExpectedUrl);

  if (!preview) {
    return;
  }

  const chat = chats.get(chatId);

  if (!chat) {
    return;
  }

  const message = chat.messages.find((entry) => entry.id === messageId);

  if (!message || message.deleted) {
    return;
  }

  const currentUrl = firstPreviewUrlFromText(message.text);

  if (!currentUrl || currentUrl !== normalizedExpectedUrl) {
    return;
  }

  const currentPreview = sanitizeLinkPreview(message.linkPreview);

  if (
    currentPreview &&
    currentPreview.url === preview.url &&
    currentPreview.title === preview.title &&
    currentPreview.description === preview.description &&
    currentPreview.siteName === preview.siteName &&
    currentPreview.imageUrl === preview.imageUrl &&
    currentPreview.authorName === preview.authorName &&
    currentPreview.providerKey === preview.providerKey
  ) {
    return;
  }

  message.linkPreview = preview;
  scheduleStatePersistence();
  emitChatState(chat);
}

function safeAck(callback, payload) {
  if (typeof callback === "function") {
    callback(payload);
  }
}

function isValidIsoDate(value) {
  const iso = String(value ?? "");

  if (!iso) {
    return false;
  }

  const timestamp = Date.parse(iso);
  return Number.isFinite(timestamp);
}

function cleanSessionDeviceName(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 64);
}

function cleanSessionUserAgent(value) {
  return String(value ?? "").trim().slice(0, 300);
}

function cleanSessionIpAddress(value) {
  return String(value ?? "").trim().slice(0, 100);
}

function isLoopbackIp(value) {
  const ip = String(value ?? "").trim().toLowerCase();

  if (!ip) {
    return false;
  }

  return (
    ip === "::1" ||
    ip === "127.0.0.1" ||
    ip === "localhost" ||
    ip === "::ffff:127.0.0.1"
  );
}

function normalizeIpCandidate(value) {
  let candidate = String(value ?? "").trim();

  if (!candidate) {
    return "";
  }

  if (
    candidate.startsWith("\"") &&
    candidate.endsWith("\"") &&
    candidate.length >= 2
  ) {
    candidate = candidate.slice(1, -1).trim();
  }

  if (
    candidate.startsWith("'") &&
    candidate.endsWith("'") &&
    candidate.length >= 2
  ) {
    candidate = candidate.slice(1, -1).trim();
  }

  if (
    candidate.startsWith("[") &&
    candidate.includes("]") &&
    candidate.indexOf("]") > 1
  ) {
    candidate = candidate.slice(1, candidate.indexOf("]")).trim();
  } else {
    const hasIpv4 = candidate.includes(".");
    const hasSinglePortSuffix = /^[^:]+:\d+$/.test(candidate);

    if (hasIpv4 && hasSinglePortSuffix) {
      candidate = candidate.slice(0, candidate.lastIndexOf(":")).trim();
    }
  }

  const lower = candidate.toLowerCase();

  if (lower === "unknown" || lower === "null") {
    return "";
  }

  if (lower.startsWith("::ffff:")) {
    candidate = candidate.slice(7).trim();
  }

  return cleanSessionIpAddress(candidate);
}

function appendHeaderValues(target, value) {
  if (typeof value === "string") {
    target.push(value);
    return;
  }

  if (Array.isArray(value)) {
    for (const entry of value) {
      if (typeof entry === "string") {
        target.push(entry);
      }
    }
  }
}

function extractForwardedForCandidates(rawValue) {
  const values = [];
  appendHeaderValues(values, rawValue);

  const candidates = [];

  for (const value of values) {
    for (const token of value.split(",")) {
      const normalized = normalizeIpCandidate(token);

      if (normalized) {
        candidates.push(normalized);
      }
    }
  }

  return candidates;
}

function extractForwardedHeaderCandidates(rawValue) {
  const values = [];
  appendHeaderValues(values, rawValue);

  const candidates = [];

  for (const value of values) {
    for (const group of value.split(",")) {
      for (const directive of group.split(";")) {
        const trimmed = directive.trim();

        if (!trimmed.toLowerCase().startsWith("for=")) {
          continue;
        }

        const rawCandidate = trimmed.slice(4).trim();
        const normalized = normalizeIpCandidate(rawCandidate);

        if (normalized) {
          candidates.push(normalized);
        }
      }
    }
  }

  return candidates;
}

function selectBestClientIp(candidates) {
  let fallback = "";

  for (const candidate of candidates) {
    const normalized = normalizeIpCandidate(candidate);

    if (!normalized) {
      continue;
    }

    if (!fallback) {
      fallback = normalized;
    }

    if (!isLoopbackIp(normalized)) {
      return normalized;
    }
  }

  return fallback;
}

function normalizeSessionTimestamp(value, fallbackIso) {
  return isValidIsoDate(value) ? String(value) : fallbackIso;
}

function browserNameFromUserAgent(userAgent) {
  const ua = userAgent.toLowerCase();

  if (ua.includes("edg/")) {
    return "Edge";
  }

  if (ua.includes("opr/") || ua.includes("opera")) {
    return "Opera";
  }

  if (ua.includes("firefox/")) {
    return "Firefox";
  }

  if (ua.includes("chrome/") || ua.includes("crios/")) {
    return "Chrome";
  }

  if (ua.includes("safari/")) {
    return "Safari";
  }

  return "Browser";
}

function osNameFromUserAgent(userAgent) {
  const ua = userAgent.toLowerCase();

  if (ua.includes("windows")) {
    return "Windows";
  }

  if (ua.includes("android")) {
    return "Android";
  }

  if (ua.includes("iphone") || ua.includes("ipad") || ua.includes("ipod")) {
    return "iOS";
  }

  if (ua.includes("mac os x") || ua.includes("macintosh")) {
    return "macOS";
  }

  if (ua.includes("linux")) {
    return "Linux";
  }

  return "Unknown OS";
}

function inferSessionDeviceLabel(session) {
  const explicit = cleanSessionDeviceName(session?.deviceName);

  if (explicit) {
    return explicit;
  }

  const userAgent = cleanSessionUserAgent(session?.userAgent);

  if (!userAgent) {
    return "Unknown device";
  }

  return `${osNameFromUserAgent(userAgent)} - ${browserNameFromUserAgent(userAgent)}`;
}

function socketClientIp(socket) {
  const headers = socket?.handshake?.headers ?? {};
  const candidates = [];

  candidates.push(...extractForwardedForCandidates(headers["x-forwarded-for"]));
  candidates.push(...extractForwardedHeaderCandidates(headers.forwarded));

  appendHeaderValues(candidates, headers["cf-connecting-ip"]);
  appendHeaderValues(candidates, headers["x-real-ip"]);
  appendHeaderValues(candidates, headers["x-client-ip"]);
  appendHeaderValues(candidates, headers["true-client-ip"]);
  appendHeaderValues(candidates, headers["fastly-client-ip"]);
  appendHeaderValues(candidates, headers["fly-client-ip"]);

  appendHeaderValues(candidates, socket?.handshake?.address);
  appendHeaderValues(candidates, socket?.conn?.remoteAddress);
  appendHeaderValues(candidates, socket?.request?.socket?.remoteAddress);

  return selectBestClientIp(candidates);
}

function buildSessionDetails(userKey, sessionId, socket, requestedDeviceName = "", previous = null) {
  const nowIso = new Date().toISOString();
  const previousCreatedAt = previous?.createdAt;

  return {
    sessionId,
    userKey,
    createdAt: normalizeSessionTimestamp(previousCreatedAt, nowIso),
    lastSeenAt: nowIso,
    deviceName: cleanSessionDeviceName(requestedDeviceName || previous?.deviceName),
    userAgent: cleanSessionUserAgent(socket?.handshake?.headers?.["user-agent"] || previous?.userAgent),
    ipAddress: socketClientIp(socket) || cleanSessionIpAddress(previous?.ipAddress)
  };
}

function sessionTimeValue(isoDate) {
  const timestamp = Date.parse(String(isoDate ?? ""));
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function normalizeUserList(rawUsers) {
  if (!Array.isArray(rawUsers)) {
    return [];
  }

  const unique = new Set();

  for (const rawUserKey of rawUsers) {
    const normalized = normalizeUserName(rawUserKey);

    if (normalized && users.has(normalized)) {
      unique.add(normalized);
    }
  }

  return [...unique];
}

function sanitizeStoredMessage(rawMessage, participants) {
  const senderKey = String(rawMessage?.senderKey ?? "");
  const senderIsSystem = senderKey === "system";
  const senderAllowed = senderIsSystem || participants.includes(senderKey);

  if (!senderAllowed) {
    return null;
  }

  const attachments = [];
  const rawAttachments = Array.isArray(rawMessage?.attachments) ? rawMessage.attachments : [];

  for (const rawAttachment of rawAttachments) {
    const dataUrl = String(rawAttachment?.dataUrl ?? "");
    const size = Number(rawAttachment?.size ?? 0);
    const durationSecondsRaw = Number(rawAttachment?.durationSeconds ?? NaN);
    const durationSeconds =
      Number.isFinite(durationSecondsRaw) && durationSecondsRaw > 0
        ? Math.min(24 * 60 * 60, Math.round(durationSecondsRaw * 100) / 100)
        : null;

    if (!dataUrl.startsWith("data:")) {
      continue;
    }

    if (dataUrl.length > MAX_DATA_URL_LENGTH || size > MAX_ATTACHMENT_SIZE) {
      continue;
    }

    attachments.push({
      id: String(rawAttachment?.id ?? crypto.randomUUID()),
      name: cleanFileName(rawAttachment?.name),
      type: String(rawAttachment?.type ?? "application/octet-stream").slice(0, 100),
      size,
      dataUrl,
      durationSeconds
    });
  }

  const deleted = Boolean(rawMessage?.deleted);
  const sentAt = isValidIsoDate(rawMessage?.sentAt)
    ? String(rawMessage.sentAt)
    : new Date().toISOString();
  const editedAt = isValidIsoDate(rawMessage?.editedAt) ? String(rawMessage.editedAt) : null;
  const deletedAt =
    deleted && isValidIsoDate(rawMessage?.deletedAt)
      ? String(rawMessage.deletedAt)
      : deleted
        ? new Date().toISOString()
        : null;

  const readBy = {};
  const rawReadBy =
    rawMessage?.readBy && typeof rawMessage.readBy === "object" ? rawMessage.readBy : {};

  for (const participant of participants) {
    const readAt = rawReadBy[participant];

    if (isValidIsoDate(readAt)) {
      readBy[participant] = String(readAt);
    }
  }

  if (!senderIsSystem && !readBy[senderKey]) {
    readBy[senderKey] = sentAt;
  }

  const deliveredBy = {};
  const rawDeliveredBy =
    rawMessage?.deliveredBy && typeof rawMessage.deliveredBy === "object"
      ? rawMessage.deliveredBy
      : {};

  for (const participant of participants) {
    const deliveredAt = rawDeliveredBy[participant];

    if (isValidIsoDate(deliveredAt)) {
      deliveredBy[participant] = String(deliveredAt);
    }
  }

  for (const [participant, readAt] of Object.entries(readBy)) {
    if (isValidIsoDate(readAt) && !isValidIsoDate(deliveredBy[participant])) {
      deliveredBy[participant] = String(readAt);
    }
  }

  if (!senderIsSystem && !deliveredBy[senderKey]) {
    deliveredBy[senderKey] = sentAt;
  }

  const reactionUsersByEmoji = new Map();
  const rawReactions = Array.isArray(rawMessage?.reactions) ? rawMessage.reactions : [];

  for (const rawReaction of rawReactions) {
    const emoji = String(rawReaction?.emoji ?? "").trim().slice(0, 32);

    if (!emoji) {
      continue;
    }

    const usersForReaction = normalizeUserList(rawReaction?.users).filter((userKey) =>
      participants.includes(userKey)
    );

    if (usersForReaction.length === 0) {
      continue;
    }

    if (!reactionUsersByEmoji.has(emoji)) {
      reactionUsersByEmoji.set(emoji, new Set());
    }

    const emojiUsers = reactionUsersByEmoji.get(emoji);
    for (const userKey of usersForReaction) {
      emojiUsers.add(userKey);
    }
  }

  const reactions = [...reactionUsersByEmoji.entries()]
    .map(([emoji, userSet]) => ({
      emoji,
      users: [...userSet]
    }))
    .filter((entry) => entry.users.length > 0);

  const rawReplyTo = rawMessage?.replyTo;
  const fallbackReplyMessageId = String(rawReplyTo?.messageId ?? "").trim();
  const storedReplyMessageId = String(rawMessage?.replyToMessageId ?? "").trim();
  const replyToMessageId = storedReplyMessageId || fallbackReplyMessageId || null;

  let replyTo = null;

  if (rawReplyTo && typeof rawReplyTo === "object") {
    const replySenderName = cleanDisplayName(rawReplyTo.senderName) || "Unknown";
    const replyText = String(rawReplyTo.text ?? "").trim().slice(0, 220);
    const replySenderKey = String(rawReplyTo.senderKey ?? "").trim() || null;

    replyTo = {
      messageId: replyToMessageId,
      senderKey: replySenderKey,
      senderName: replySenderName,
      text: replyText || "Message",
      deleted: Boolean(rawReplyTo.deleted)
    };
  }

  const linkPreview = sanitizeLinkPreview(rawMessage?.linkPreview);

  return {
    id: String(rawMessage?.id ?? crypto.randomUUID()),
    senderKey: senderIsSystem ? "system" : senderKey,
    senderName: senderIsSystem ? "System" : displayNameFor(senderKey),
    text: deleted ? "" : String(rawMessage?.text ?? ""),
    kind: rawMessage?.kind ? String(rawMessage.kind).slice(0, 40) : null,
    sentAt,
    editedAt,
    deleted,
    deletedAt,
    attachments: deleted ? [] : attachments,
    linkPreview: deleted ? null : linkPreview,
    replyToMessageId,
    replyTo,
    reactions: deleted ? [] : reactions,
    deliveredBy,
    readBy
  };
}

function serializeStateForStorage() {
  return JSON.stringify({
    version: 2,
    users: [...users.values()].map((user) => ({
      key: user.key,
      name: user.name,
      passwordHash: String(user.passwordHash ?? ""),
      passwordSalt: String(user.passwordSalt ?? ""),
      avatarDataUrl: user.avatarDataUrl ?? null,
      friends: [...user.friends],
      incomingRequests: [...user.incomingRequests],
      outgoingRequests: [...user.outgoingRequests],
      friendAliases: [...user.friendAliases.entries()],
      hiddenChats: [...(user.hiddenChats ?? new Set())]
    })),
    sessions: [...sessions.entries()].map(([sessionId, userKey]) => ({
      sessionId,
      userKey,
      createdAt: sessionDetails.get(sessionId)?.createdAt ?? null,
      lastSeenAt: sessionDetails.get(sessionId)?.lastSeenAt ?? null,
      deviceName: sessionDetails.get(sessionId)?.deviceName ?? "",
      userAgent: sessionDetails.get(sessionId)?.userAgent ?? "",
      ipAddress: sessionDetails.get(sessionId)?.ipAddress ?? ""
    })),
    chats: [...chats.values()].map((chat) => ({
      id: chat.id,
      type: chat.type,
      name: chat.name,
      participants: [...chat.participants],
      closeVotes: [...chat.closeVotes],
      viewerPrefs:
        chat.viewerPrefs instanceof Map
          ? [...chat.viewerPrefs.entries()].map(([userKey, prefs]) => [
              userKey,
              {
                nickname: String(prefs?.nickname ?? "").trim() || null,
                theme: normalizeChatTheme(prefs?.theme),
                wallpaper: normalizeChatWallpaper(prefs?.wallpaper)
              }
            ])
          : [],
      updatedAt: Number(chat.updatedAt || Date.now()),
      messages: chat.messages.map((message) => ({
        id: message.id,
        senderKey: message.senderKey,
        senderName: message.senderName,
        text: message.text,
        kind: message.kind ?? null,
        sentAt: message.sentAt,
        editedAt: message.editedAt ?? null,
        deleted: Boolean(message.deleted),
        deletedAt: message.deletedAt ?? null,
        attachments: Array.isArray(message.attachments) ? message.attachments : [],
        linkPreview: sanitizeLinkPreview(message.linkPreview),
        replyToMessageId: String(message.replyToMessageId ?? "").trim() || null,
        replyTo:
          message.replyTo && typeof message.replyTo === "object"
            ? {
                messageId: String(message.replyTo.messageId ?? "").trim() || null,
                senderKey: String(message.replyTo.senderKey ?? "").trim() || null,
                senderName: String(message.replyTo.senderName ?? "").trim().slice(0, 24) || "Unknown",
                text: String(message.replyTo.text ?? "").slice(0, 220),
                deleted: Boolean(message.replyTo.deleted)
              }
            : null,
        reactions: Array.isArray(message.reactions)
          ? message.reactions.map((reaction) => ({
              emoji: String(reaction?.emoji ?? "").trim().slice(0, 32),
              users: normalizeUserList(reaction?.users)
            }))
          : [],
        deliveredBy:
          message.deliveredBy && typeof message.deliveredBy === "object"
            ? { ...message.deliveredBy }
            : {},
        readBy:
          message.readBy && typeof message.readBy === "object" ? { ...message.readBy } : {}
      }))
    }))
  });
}

function persistStateToDatabase() {
  const serializedState = serializeStateForStorage();
  const storedState = encryptStatePayload(serializedState);

  if (storedState === null) {
    return;
  }

  if (!upsertStateStatement) {
    try {
      fs.writeFileSync(STATE_SNAPSHOT_PATH, storedState, "utf8");
    } catch (error) {
      console.error("Failed to persist state to JSON snapshot:", error);
    }
    return;
  }

  try {
    upsertStateStatement.run(storedState);
  } catch (error) {
    console.error("Failed to persist state to SQLite:", error);
  }
}

function scheduleStatePersistence() {
  if (persistenceScheduled) {
    return;
  }

  persistenceScheduled = true;

  setTimeout(() => {
    persistenceScheduled = false;
    persistStateToDatabase();
  }, 0);
}

async function restoreStateFromDatabase() {
  let serializedState = "";

  if (!selectStateStatement) {
    try {
      if (!fs.existsSync(STATE_SNAPSHOT_PATH)) {
        return;
      }

      serializedState = fs.readFileSync(STATE_SNAPSHOT_PATH, "utf8");
    } catch (error) {
      console.error("Failed to read persisted state from JSON snapshot:", error);
      return;
    }
  } else {
    let rawState = null;

    try {
      rawState = selectStateStatement.get();
    } catch (error) {
      console.error("Failed to read persisted state from SQLite:", error);
      return;
    }

    serializedState = String(rawState?.value ?? "");
  }

  if (!serializedState) {
    return;
  }

  const decryptedState = decryptStatePayload(serializedState);
  if (decryptedState === null) {
    return;
  }

  let parsedState;

  try {
    parsedState = JSON.parse(decryptedState);
  } catch (error) {
    console.error("Persisted state is invalid JSON and was ignored:", error);
    return;
  }

  const rawUsers = Array.isArray(parsedState?.users) ? parsedState.users : [];
  let shouldPersistMigratedPasswords = false;

  for (const rawUser of rawUsers) {
    const userKey = normalizeUserName(rawUser?.key);
    const name = cleanDisplayName(rawUser?.name);
    const passwordHash = String(rawUser?.passwordHash ?? "").trim().toLowerCase();
    const passwordSalt = String(rawUser?.passwordSalt ?? "").trim().toLowerCase();
    const hasPasswordHash = /^[a-f0-9]{128}$/.test(passwordHash);
    const hasPasswordSalt = /^[a-f0-9]{32}$/.test(passwordSalt);
    const hasValidPasswordHash = hasPasswordHash && hasPasswordSalt;
    const legacyPassword = cleanPassword(rawUser?.password);

    if (!userKey || !name || (!hasValidPasswordHash && legacyPassword.length < 4)) {
      continue;
    }

    const nextUser = {
      key: userKey,
      name,
      passwordHash: hasValidPasswordHash ? passwordHash : "",
      passwordSalt: hasValidPasswordHash ? passwordSalt : "",
      avatarDataUrl: String(rawUser?.avatarDataUrl ?? "").startsWith("data:image/")
        ? String(rawUser.avatarDataUrl)
        : null,
      friendAliases: new Map(),
      friends: new Set(),
      incomingRequests: new Set(),
      outgoingRequests: new Set(),
      hiddenChats: new Set()
    };

    if (!isHashedPassword(nextUser) && legacyPassword.length >= 4) {
      setUserPassword(nextUser, legacyPassword);
      shouldPersistMigratedPasswords = true;
    }

    users.set(userKey, nextUser);
  }

  for (const rawUser of rawUsers) {
    const userKey = normalizeUserName(rawUser?.key);

    if (!userKey || !users.has(userKey)) {
      continue;
    }

    const user = users.get(userKey);

    user.friends = new Set(normalizeUserList(rawUser?.friends));
    user.incomingRequests = new Set(normalizeUserList(rawUser?.incomingRequests));
    user.outgoingRequests = new Set(normalizeUserList(rawUser?.outgoingRequests));
    user.hiddenChats = new Set(
      Array.isArray(rawUser?.hiddenChats)
        ? rawUser.hiddenChats.map((entry) => String(entry ?? "").trim()).filter(Boolean)
        : []
    );

    const rawAliases = Array.isArray(rawUser?.friendAliases) ? rawUser.friendAliases : [];

    for (const entry of rawAliases) {
      if (!Array.isArray(entry) || entry.length < 2) {
        continue;
      }

      const aliasTarget = normalizeUserName(entry[0]);
      const aliasValue = cleanAlias(entry[1]);

      if (!aliasTarget || !aliasValue || !users.has(aliasTarget)) {
        continue;
      }

      user.friendAliases.set(aliasTarget, aliasValue);
    }
  }

  if (shouldPersistMigratedPasswords) {
    scheduleStatePersistence();
  }

  const rawSessions = Array.isArray(parsedState?.sessions) ? parsedState.sessions : [];

  for (const rawSession of rawSessions) {
    const sessionId = String(rawSession?.sessionId ?? "").trim();
    const userKey = normalizeUserName(rawSession?.userKey);

    if (!sessionId || !userKey || !users.has(userKey)) {
      continue;
    }

    const createdAt = normalizeSessionTimestamp(rawSession?.createdAt, new Date().toISOString());
    const lastSeenAt = normalizeSessionTimestamp(rawSession?.lastSeenAt, createdAt);

    sessions.set(sessionId, userKey);
    sessionDetails.set(sessionId, {
      sessionId,
      userKey,
      createdAt,
      lastSeenAt,
      deviceName: cleanSessionDeviceName(rawSession?.deviceName),
      userAgent: cleanSessionUserAgent(rawSession?.userAgent),
      ipAddress: cleanSessionIpAddress(rawSession?.ipAddress)
    });

    if (!sessionsByUser.has(userKey)) {
      sessionsByUser.set(userKey, new Set());
    }

    sessionsByUser.get(userKey).add(sessionId);
  }

  const rawChats = Array.isArray(parsedState?.chats) ? parsedState.chats : [];

  for (const rawChat of rawChats) {
    const chatId = String(rawChat?.id ?? "");
    const type = rawChat?.type === "group" ? "group" : rawChat?.type === "temp" ? "temp" : "dm";
    const name = rawChat?.name === null ? null : String(rawChat?.name ?? "");
    const participants = normalizeUserList(rawChat?.participants);

    if (!chatId || participants.length < 2) {
      continue;
    }

    if (type !== "group" && participants.length !== 2) {
      continue;
    }

    const messageList = [];
    const rawMessages = Array.isArray(rawChat?.messages) ? rawChat.messages : [];

    for (const rawMessage of rawMessages) {
      const message = sanitizeStoredMessage(rawMessage, participants);

      if (message) {
        messageList.push(message);
      }
    }

    const viewerPrefs = new Map();
    const rawViewerPrefs = Array.isArray(rawChat?.viewerPrefs) ? rawChat.viewerPrefs : [];

    for (const rawEntry of rawViewerPrefs) {
      if (!Array.isArray(rawEntry) || rawEntry.length < 2) {
        continue;
      }

      const prefUserKey = normalizeUserName(rawEntry[0]);

      if (!prefUserKey || !participants.includes(prefUserKey)) {
        continue;
      }

      const normalizedPrefs = normalizeViewerChatPreference(rawEntry[1]);

      if (normalizedPrefs) {
        viewerPrefs.set(prefUserKey, normalizedPrefs);
      }
    }

    chats.set(chatId, {
      id: chatId,
      type,
      name: type === "group" ? cleanGroupName(name) || "Group chat" : null,
      participants: [...participants].sort(),
      messages: messageList,
      closeVotes: new Set(
        normalizeUserList(rawChat?.closeVotes).filter((userKey) =>
          participants.includes(userKey)
        )
      ),
      viewerPrefs,
      updatedAt: Number(rawChat?.updatedAt || Date.now())
    });
  }
}

function createUser(userKey, displayName, password) {
  const { passwordHash, passwordSalt } = hashPassword(password);

  users.set(userKey, {
    key: userKey,
    name: displayName,
    passwordHash,
    passwordSalt,
    avatarDataUrl: null,
    friendAliases: new Map(),
    friends: new Set(),
    incomingRequests: new Set(),
    outgoingRequests: new Set(),
    hiddenChats: new Set()
  });

  scheduleStatePersistence();
}

function displayNameFor(userKey) {
  return users.get(userKey)?.name ?? userKey;
}

function isOnline(userKey) {
  return onlineUsers.has(userKey);
}

function getUserSockets(userKey) {
  return onlineUsers.get(userKey) ?? new Set();
}

function emitToUser(userKey, eventName, payload) {
  for (const socketId of getUserSockets(userKey)) {
    io.to(socketId).emit(eventName, payload);
  }
}

function authenticateSocket(socket, userKey) {
  const previousUserKey = socketToUser.get(socket.id);

  if (previousUserKey && previousUserKey !== userKey) {
    const previousSockets = onlineUsers.get(previousUserKey);

    if (previousSockets) {
      previousSockets.delete(socket.id);

      if (previousSockets.size === 0) {
        onlineUsers.delete(previousUserKey);
      }
    }
  }

  if (!onlineUsers.has(userKey)) {
    onlineUsers.set(userKey, new Set());
  }

  onlineUsers.get(userKey).add(socket.id);
  socketToUser.set(socket.id, userKey);
}

function detachSocket(socketId) {
  detachSocketFromSession(socketId);

  const userKey = socketToUser.get(socketId);

  if (!userKey) {
    return null;
  }

  socketToUser.delete(socketId);

  const sockets = onlineUsers.get(userKey);

  if (sockets) {
    sockets.delete(socketId);

    if (sockets.size === 0) {
      onlineUsers.delete(userKey);
    }
  }

  return userKey;
}

function serializeAccount(userKey) {
  return {
    key: userKey,
    name: displayNameFor(userKey),
    displayName: displayNameFor(userKey),
    alias: null,
    online: isOnline(userKey),
    avatarDataUrl: users.get(userKey)?.avatarDataUrl ?? null
  };
}

function displayNameForViewer(viewerKey, targetKey) {
  if (!viewerKey || !targetKey || viewerKey === targetKey) {
    return displayNameFor(targetKey);
  }

  const viewer = users.get(viewerKey);
  const alias = viewer?.friendAliases?.get(targetKey);

  return alias || displayNameFor(targetKey);
}

function serializeFriendForUser(viewerKey, friendKey) {
  const account = serializeAccount(friendKey);
  const alias = users.get(viewerKey)?.friendAliases?.get(friendKey) ?? null;

  return {
    ...account,
    name: alias || account.displayName,
    alias
  };
}

function sortUserKeys(keys) {
  return [...keys].sort((left, right) => {
    return displayNameFor(left).localeCompare(displayNameFor(right));
  });
}

function relationshipStateFor(userKey) {
  const user = users.get(userKey);

  if (!user) {
    return {
      friends: [],
      incomingRequests: [],
      outgoingRequests: []
    };
  }

  return {
    friends: sortUserKeys(user.friends).map((friendKey) =>
      serializeFriendForUser(userKey, friendKey)
    ),
    incomingRequests: sortUserKeys(user.incomingRequests).map((requestKey) =>
      serializeAccount(requestKey)
    ),
    outgoingRequests: sortUserKeys(user.outgoingRequests).map((requestKey) =>
      serializeAccount(requestKey)
    )
  };
}

function emitRelationshipState(userKey) {
  if (!users.has(userKey)) {
    return;
  }

  emitToUser(userKey, "relationship_state", relationshipStateFor(userKey));
}

function emitRelationshipStateForNetwork(userKey) {
  const user = users.get(userKey);

  if (!user) {
    return;
  }

  const impactedUsers = new Set([
    userKey,
    ...user.friends,
    ...user.incomingRequests,
    ...user.outgoingRequests
  ]);

  for (const impactedKey of impactedUsers) {
    emitRelationshipState(impactedKey);
  }
}

function emitPresenceStateForFriends(userKey) {
  const user = users.get(userKey);

  if (!user) {
    return;
  }

  const impactedUsers = new Set([userKey, ...user.friends]);

  for (const impactedKey of impactedUsers) {
    emitRelationshipState(impactedKey);
  }
}

function areFriends(userA, userB) {
  const account = users.get(userA);
  return Boolean(account && account.friends.has(userB));
}

function buildDirectChatId(userA, userB, isTemp) {
  const [first, second] = [userA, userB].sort();
  return `${isTemp ? "temp" : "dm"}:${first}:${second}`;
}

function serializeParticipant(userKey) {
  return {
    key: userKey,
    name: displayNameFor(userKey),
    displayName: displayNameFor(userKey),
    alias: null,
    online: isOnline(userKey),
    avatarDataUrl: users.get(userKey)?.avatarDataUrl ?? null
  };
}

function serializeParticipantForViewer(viewerKey, participantKey) {
  const participant = serializeParticipant(participantKey);
  const alias =
    viewerKey && viewerKey !== participantKey
      ? users.get(viewerKey)?.friendAliases?.get(participantKey) ?? null
      : null;

  return {
    ...participant,
    name: alias || participant.displayName,
    alias
  };
}

function senderNameForViewer(message, viewerKey) {
  if (message.senderKey === "system") {
    return message.senderName;
  }

  if (!message.senderKey) {
    return message.senderName;
  }

  return displayNameForViewer(viewerKey, message.senderKey);
}

function serializeMessage(message, viewerKey) {
  const linkPreview = sanitizeLinkPreview(message.linkPreview);

  return {
    id: message.id,
    senderKey: message.senderKey,
    senderName: senderNameForViewer(message, viewerKey),
    text: message.text,
    kind: message.kind ?? null,
    sentAt: message.sentAt,
    editedAt: message.editedAt ?? null,
    deleted: Boolean(message.deleted),
    deletedAt: message.deletedAt ?? null,
    attachments: Array.isArray(message.attachments) ? message.attachments : [],
    linkPreview,
    replyToMessageId: String(message.replyToMessageId ?? "").trim() || null,
    replyTo:
      message.replyTo && typeof message.replyTo === "object"
        ? {
            messageId: String(message.replyTo.messageId ?? "").trim() || null,
            senderKey: String(message.replyTo.senderKey ?? "").trim() || null,
            senderName: message.replyTo.senderKey && message.replyTo.senderKey !== "system"
              ? displayNameForViewer(viewerKey, message.replyTo.senderKey)
              : String(message.replyTo.senderName ?? "Unknown"),
            text: String(message.replyTo.text ?? "").slice(0, 220),
            deleted: Boolean(message.replyTo.deleted)
          }
        : null,
    reactions: Array.isArray(message.reactions)
      ? message.reactions
          .map((reaction) => ({
            emoji: String(reaction?.emoji ?? "").trim().slice(0, 32),
            users: normalizeUserList(reaction?.users)
          }))
          .filter((reaction) => reaction.emoji && reaction.users.length > 0)
      : [],
    deliveredBy:
      message.deliveredBy && typeof message.deliveredBy === "object"
        ? { ...message.deliveredBy }
        : {},
    readBy: message.readBy && typeof message.readBy === "object" ? { ...message.readBy } : {}
  };
}

function resolveUserKey(rawValue) {
  const normalized = normalizeUserName(rawValue);

  if (!normalized) {
    return null;
  }

  if (users.has(normalized)) {
    return normalized;
  }

  for (const [userKey, user] of users.entries()) {
    if (normalizeUserName(user.name) === normalized) {
      return userKey;
    }
  }

  return null;
}

function viewerChatPreferencesForUser(chat, viewerKey) {
  const fallback = {
    nickname: null,
    theme: "default",
    wallpaper: null
  };

  if (!chat || !viewerKey || !chat.participants.includes(viewerKey)) {
    return fallback;
  }

  if (!(chat.viewerPrefs instanceof Map)) {
    return fallback;
  }

  const normalized = normalizeViewerChatPreference(chat.viewerPrefs.get(viewerKey));

  if (!normalized) {
    return fallback;
  }

  return normalized;
}

function serializeChat(chat, viewerKey) {
  const viewerPrefs = viewerChatPreferencesForUser(chat, viewerKey);

  return {
    id: chat.id,
    type: chat.type,
    isTemp: chat.type === "temp",
    name: chat.name,
    participants: chat.participants.map((participant) =>
      serializeParticipantForViewer(viewerKey, participant)
    ),
    messages: chat.messages.map((message) => serializeMessage(message, viewerKey)),
    closeVotes: [...chat.closeVotes],
    viewerPrefs,
    updatedAt: chat.updatedAt
  };
}

function chatTitleForUser(chat, userKey) {
  const viewerPrefs = viewerChatPreferencesForUser(chat, userKey);

  if (viewerPrefs.nickname) {
    return viewerPrefs.nickname;
  }

  if (chat.type === "group") {
    return chat.name;
  }

  const otherParticipant = chat.participants.find((participant) => participant !== userKey);
  return displayNameForViewer(userKey, otherParticipant);
}

function messagePreview(message) {
  if (!message) {
    return "";
  }

  if (message.deleted) {
    return "Message deleted";
  }

  if (message.kind === "temp_close_request") {
    return "Temp close requested";
  }

  if (message.kind === "temp_close_reset") {
    return "Temp close canceled";
  }

  const attachments = Array.isArray(message.attachments) ? message.attachments : [];
  const attachmentCount = attachments.length;

  if (message.text && attachmentCount > 0) {
    return `${message.text} (${attachmentCount} attachment${attachmentCount > 1 ? "s" : ""})`;
  }

  if (message.text) {
    return message.text;
  }

  if (attachmentCount > 0) {
    const hasImage = attachments.some((attachment) => {
      return String(attachment.type ?? "").startsWith("image/");
    });
    const hasAudio = attachments.some((attachment) => {
      return String(attachment.type ?? "").startsWith("audio/");
    });
    const hasVideo = attachments.some((attachment) => {
      return String(attachment.type ?? "").startsWith("video/");
    });

    if (hasAudio && !hasImage && !hasVideo) {
      return "[Voice note]";
    }

    if (hasVideo) {
      return "[Video]";
    }

    return hasImage ? "[Image]" : "[File]";
  }

  return "";
}

function chatSummariesForUser(userKey) {
  const hiddenChats = users.get(userKey)?.hiddenChats ?? new Set();

  return [...chats.values()]
    .filter((chat) => chat.participants.includes(userKey) && !hiddenChats.has(chat.id))
    .sort((left, right) => right.updatedAt - left.updatedAt)
    .map((chat) => {
      const lastMessage = chat.messages.at(-1);

      return {
        id: chat.id,
        type: chat.type,
        isTemp: chat.type === "temp",
        title: chatTitleForUser(chat, userKey),
        updatedAt: chat.updatedAt,
        closeVotes: [...chat.closeVotes],
        lastMessage: messagePreview(lastMessage),
        lastMessageAt: lastMessage ? lastMessage.sentAt : null
      };
    });
}

function emitChatSummaries(userKey) {
  emitToUser(userKey, "chat_summaries", chatSummariesForUser(userKey));
}

function emitChatState(chat) {
  for (const participant of chat.participants) {
    emitToUser(participant, "chat_updated", serializeChat(chat, participant));
    emitChatSummaries(participant);
  }
}

function emitGroupCreatedEvents(chat, creatorKey) {
  for (const participant of chat.participants) {
    emitChatSummaries(participant);
  }

  for (const participant of chat.participants) {
    if (participant === creatorKey) {
      continue;
    }

    emitToUser(participant, "group_created", {
      from: displayNameFor(creatorKey),
      groupName: chat.name,
      chatId: chat.id
    });
  }
}

function attachSocketToSession(socketId, sessionId) {
  if (!socketId || !sessionId) {
    return;
  }

  const previousSessionId = socketToSession.get(socketId);

  if (previousSessionId && previousSessionId !== sessionId) {
    detachSocketFromSession(socketId);
  }

  socketToSession.set(socketId, sessionId);

  if (!sessionToSockets.has(sessionId)) {
    sessionToSockets.set(sessionId, new Set());
  }

  sessionToSockets.get(sessionId).add(socketId);
}

function detachSocketFromSession(socketId) {
  const sessionId = socketToSession.get(socketId);

  if (!sessionId) {
    return null;
  }

  socketToSession.delete(socketId);

  const socketIds = sessionToSockets.get(sessionId);

  if (socketIds) {
    socketIds.delete(socketId);

    if (socketIds.size === 0) {
      sessionToSockets.delete(sessionId);
    }
  }

  return sessionId;
}

function sessionsForUser(userKey, currentSessionId = null) {
  const userSessions = sessionsByUser.get(userKey) ?? new Set();
  const result = [];

  for (const sessionId of userSessions) {
    const details = sessionDetails.get(sessionId) ?? {
      sessionId,
      userKey,
      createdAt: null,
      lastSeenAt: null,
      deviceName: "",
      userAgent: "",
      ipAddress: ""
    };

    result.push({
      sessionId,
      label: inferSessionDeviceLabel(details),
      deviceName: details.deviceName || null,
      userAgent: details.userAgent || null,
      ipAddress: details.ipAddress || null,
      createdAt: details.createdAt,
      lastSeenAt: details.lastSeenAt,
      isCurrent: currentSessionId === sessionId
    });
  }

  return result.sort((left, right) => {
    return sessionTimeValue(right.lastSeenAt || right.createdAt) -
      sessionTimeValue(left.lastSeenAt || left.createdAt);
  });
}

function emitSessionStateForUser(userKey) {
  for (const socketId of getUserSockets(userKey)) {
    const currentSessionId = socketToSession.get(socketId) ?? null;
    io.to(socketId).emit("sessions_updated", {
      sessions: sessionsForUser(userKey, currentSessionId),
      currentSessionId
    });
  }
}

function createSessionForUser(userKey, socket, requestedDeviceName = "") {
  const sessionId = crypto.randomUUID();

  sessions.set(sessionId, userKey);
  sessionDetails.set(sessionId, buildSessionDetails(userKey, sessionId, socket, requestedDeviceName));

  if (!sessionsByUser.has(userKey)) {
    sessionsByUser.set(userKey, new Set());
  }

  sessionsByUser.get(userKey).add(sessionId);

  if (socket) {
    attachSocketToSession(socket.id, sessionId);
  }

  scheduleStatePersistence();
  return sessionId;
}

function touchSessionForSocket(socket, sessionId, requestedDeviceName = "") {
  const userKey = sessions.get(sessionId);

  if (!userKey) {
    return null;
  }

  const nextDetails = buildSessionDetails(
    userKey,
    sessionId,
    socket,
    requestedDeviceName,
    sessionDetails.get(sessionId)
  );

  sessionDetails.set(sessionId, nextDetails);

  if (socket) {
    attachSocketToSession(socket.id, sessionId);
  }

  scheduleStatePersistence();
  return sessionId;
}

function invalidateSession(sessionId, options = {}) {
  const disconnectSockets = Boolean(options.disconnectSockets);
  const reason = String(options.reason ?? "revoked").slice(0, 40);
  const userKey = sessions.get(sessionId);

  if (!userKey) {
    return null;
  }

  if (disconnectSockets) {
    const socketIds = [...(sessionToSockets.get(sessionId) ?? new Set())];

    for (const socketId of socketIds) {
      const targetSocket = io.sockets.sockets.get(socketId);

      if (!targetSocket) {
        detachSocket(socketId);
        continue;
      }

      targetSocket.emit("session_revoked", {
        sessionId,
        reason
      });
      targetSocket.disconnect(true);
    }
  }

  sessions.delete(sessionId);
  sessionDetails.delete(sessionId);

  const userSessions = sessionsByUser.get(userKey);

  if (userSessions) {
    userSessions.delete(sessionId);

    if (userSessions.size === 0) {
      sessionsByUser.delete(userKey);
    }
  }

  const sessionSockets = sessionToSockets.get(sessionId);

  if (sessionSockets) {
    for (const socketId of sessionSockets) {
      if (socketToSession.get(socketId) === sessionId) {
        socketToSession.delete(socketId);
      }
    }

    sessionToSockets.delete(sessionId);
  }

  scheduleStatePersistence();
  return userKey;
}

function invalidateSessionsForUser(userKey, options = {}) {
  const userSessions = [...(sessionsByUser.get(userKey) ?? new Set())];

  if (userSessions.length === 0) {
    return 0;
  }

  let invalidatedCount = 0;

  for (const sessionId of userSessions) {
    if (invalidateSession(sessionId, options)) {
      invalidatedCount += 1;
    }
  }

  return invalidatedCount;
}

function createOrGetDirectChat(userA, userB, isTemp) {
  const chatId = buildDirectChatId(userA, userB, isTemp);

  if (chats.has(chatId)) {
    const existingChat = chats.get(chatId);

    if (!(existingChat.viewerPrefs instanceof Map)) {
      existingChat.viewerPrefs = new Map();
    }

    return { chat: existingChat, created: false };
  }

  const chat = {
    id: chatId,
    type: isTemp ? "temp" : "dm",
    name: null,
    participants: [userA, userB].sort(),
    messages: [],
    closeVotes: new Set(),
    viewerPrefs: new Map(),
    updatedAt: Date.now()
  };

  chats.set(chatId, chat);
  scheduleStatePersistence();
  return { chat, created: true };
}

function createGroupChat(ownerKey, groupName, members) {
  const uniqueMembers = new Set(members);
  uniqueMembers.delete(ownerKey);
  const nextGroupName = cleanGroupName(groupName) || `${displayNameFor(ownerKey)}'s group`;

  const chat = {
    id: `group:${crypto.randomUUID()}`,
    type: "group",
    name: nextGroupName,
    participants: [ownerKey, ...sortUserKeys(uniqueMembers)],
    messages: [],
    closeVotes: new Set(),
    viewerPrefs: new Map(),
    updatedAt: Date.now()
  };

  chats.set(chat.id, chat);
  scheduleStatePersistence();
  return chat;
}

function setChatHiddenForUser(userKey, chatId, hidden) {
  const user = users.get(userKey);

  if (!user || !chatId) {
    return false;
  }

  if (!(user.hiddenChats instanceof Set)) {
    user.hiddenChats = new Set();
  }

  if (hidden) {
    if (user.hiddenChats.has(chatId)) {
      return false;
    }

    user.hiddenChats.add(chatId);
    return true;
  }

  if (!user.hiddenChats.has(chatId)) {
    return false;
  }

  user.hiddenChats.delete(chatId);
  return true;
}

function clearChatHiddenForParticipants(chatId, participants) {
  let changed = false;

  for (const userKey of participants) {
    if (setChatHiddenForUser(userKey, chatId, false)) {
      changed = true;
    }
  }

  return changed;
}

function removeChatFromHiddenLists(chatId) {
  let changed = false;

  for (const user of users.values()) {
    if (user.hiddenChats?.delete(chatId)) {
      changed = true;
    }
  }

  return changed;
}

function appendSystemMessage(chat, text, kind = "system") {
  chat.messages.push({
    id: crypto.randomUUID(),
    senderKey: "system",
    senderName: "System",
    text,
    kind,
    sentAt: new Date().toISOString(),
    attachments: [],
    linkPreview: null
  });

  scheduleStatePersistence();
}

function normalizeReactionEmoji(value) {
  return String(value ?? "").trim().slice(0, 32);
}

function ensureReadByMap(message) {
  if (!message.readBy || typeof message.readBy !== "object") {
    message.readBy = {};
  }

  return message.readBy;
}

function ensureDeliveredByMap(message) {
  if (!message.deliveredBy || typeof message.deliveredBy !== "object") {
    message.deliveredBy = {};
  }

  return message.deliveredBy;
}

function markChatMessagesDelivered(chat, userKey, deliveredAt = new Date().toISOString()) {
  let changed = false;

  for (const message of chat.messages) {
    if (message.senderKey === "system" || message.senderKey === userKey) {
      continue;
    }

    const deliveredBy = ensureDeliveredByMap(message);

    if (!isValidIsoDate(deliveredBy[userKey])) {
      deliveredBy[userKey] = deliveredAt;
      changed = true;
    }
  }

  return changed;
}

function markChatMessagesRead(chat, userKey, readAt = new Date().toISOString()) {
  let changed = false;

  for (const message of chat.messages) {
    if (message.senderKey === "system" || message.senderKey === userKey) {
      continue;
    }

    const deliveredBy = ensureDeliveredByMap(message);
    if (!isValidIsoDate(deliveredBy[userKey])) {
      deliveredBy[userKey] = readAt;
      changed = true;
    }

    const readBy = ensureReadByMap(message);

    if (!isValidIsoDate(readBy[userKey])) {
      readBy[userKey] = readAt;
      changed = true;
    }
  }

  return changed;
}

function markUndeliveredMessagesAsDeliveredForUser(userKey, deliveredAt = new Date().toISOString()) {
  const changedChats = [];

  for (const chat of chats.values()) {
    if (!chat.participants.includes(userKey)) {
      continue;
    }

    if (markChatMessagesDelivered(chat, userKey, deliveredAt)) {
      changedChats.push(chat);
    }
  }

  return changedChats;
}

function toggleReactionForMessage(message, userKey, emoji) {
  if (!Array.isArray(message.reactions)) {
    message.reactions = [];
  }

  const reaction = message.reactions.find((entry) => entry.emoji === emoji);

  if (!reaction) {
    message.reactions.push({ emoji, users: [userKey] });
    return true;
  }

  const uniqueUsers = new Set(normalizeUserList(reaction.users));

  if (uniqueUsers.has(userKey)) {
    uniqueUsers.delete(userKey);
  } else {
    uniqueUsers.add(userKey);
  }

  reaction.users = [...uniqueUsers];

  if (reaction.users.length === 0) {
    message.reactions = message.reactions.filter((entry) => entry !== reaction);
  }

  return true;
}

function getCurrentUser(socket, callback) {
  const userKey = socketToUser.get(socket.id);

  if (!userKey) {
    safeAck(callback, { ok: false, error: "Log in first." });
    return null;
  }

  return userKey;
}

function validateAndSanitizeAttachments(rawAttachments) {
  if (!Array.isArray(rawAttachments) || rawAttachments.length === 0) {
    return { attachments: [] };
  }

  if (rawAttachments.length > MAX_ATTACHMENT_COUNT) {
    return {
      error: `You can send up to ${MAX_ATTACHMENT_COUNT} attachments at once.`
    };
  }

  const attachments = [];

  for (const rawAttachment of rawAttachments) {
    const dataUrl = String(rawAttachment?.dataUrl ?? "");
    const size = Number(rawAttachment?.size ?? 0);
    const durationSecondsRaw = Number(rawAttachment?.durationSeconds ?? NaN);
    const durationSeconds =
      Number.isFinite(durationSecondsRaw) && durationSecondsRaw > 0
        ? Math.min(24 * 60 * 60, Math.round(durationSecondsRaw * 100) / 100)
        : null;

    if (!dataUrl.startsWith("data:")) {
      return { error: "Invalid attachment payload." };
    }

    if (dataUrl.length > MAX_DATA_URL_LENGTH || size > MAX_ATTACHMENT_SIZE) {
      return { error: "Attachment is too large (max 5 MB each)." };
    }

    attachments.push({
      id: crypto.randomUUID(),
      name: cleanFileName(rawAttachment?.name),
      type: String(rawAttachment?.type ?? "application/octet-stream").slice(0, 100),
      size,
      dataUrl,
      durationSeconds
    });
  }

  return { attachments };
}

function getCallChat(chatId, userKey) {
  const chat = chats.get(chatId);

  if (!chat || !chat.participants.includes(userKey)) {
    return { error: "Chat not found." };
  }

  if (chat.type === "group" || chat.participants.length !== 2) {
    return { error: "Calls are available only in direct chats." };
  }

  return { chat };
}

function relayToOtherParticipants(chat, senderKey, eventName, payload) {
  for (const participant of chat.participants) {
    if (participant === senderKey) {
      continue;
    }

    emitToUser(participant, eventName, payload);
  }
}

function formatCallDuration(milliseconds) {
  const totalSeconds = Math.max(0, Math.floor(Number(milliseconds || 0) / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function formatCallStartTime(timestamp) {
  const date = new Date(Number(timestamp) || Date.now());

  if (Number.isNaN(date.getTime())) {
    return "unknown time";
  }

  return date.toLocaleTimeString([], {
    hour: "numeric",
    minute: "2-digit"
  });
}

function appendCallLog(chat, text, kind) {
  appendSystemMessage(chat, text, kind);
  chat.updatedAt = Date.now();
  emitChatState(chat);
}

function startCallSession(chat, initiatorKey, mode) {
  activeCalls.set(chat.id, {
    chatId: chat.id,
    mode,
    initiatorKey,
    startedAt: Date.now(),
    answeredAt: null,
    messages: []
  });
}

function markCallAnswered(chatId) {
  const session = activeCalls.get(chatId);

  if (!session) {
    return { session: null, answeredNow: false };
  }

  let answeredNow = false;

  if (!session.answeredAt) {
    session.answeredAt = Date.now();
    answeredNow = true;
  }

  return { session, answeredNow };
}

function consumeCallSession(chatId) {
  const session = activeCalls.get(chatId) ?? null;
  activeCalls.delete(chatId);
  return session;
}

function timedOutCallLogText(callSession) {
  const starterName = callSession?.initiatorKey
    ? displayNameFor(callSession.initiatorKey)
    : "Someone";
  const mode = callSession?.mode === "video" ? "video" : "voice";
  const startedAtLabel = formatCallStartTime(callSession?.startedAt);

  return `No answer after 5 rings for ${mode} call from ${starterName} at ${startedAtLabel}.`;
}

function declinedCallLogText(callSession) {
  const starterName = callSession?.initiatorKey
    ? displayNameFor(callSession.initiatorKey)
    : "Someone";
  const mode = callSession?.mode === "video" ? "video" : "voice";
  const startedAtLabel = formatCallStartTime(callSession?.startedAt);

  return `Declined ${mode} call from ${starterName} at ${startedAtLabel}.`;
}

function completedCallLogText(callSession) {
  const starterName = callSession?.initiatorKey
    ? displayNameFor(callSession.initiatorKey)
    : "Someone";
  const mode = callSession?.mode === "video" ? "video" : "voice";
  const startedAtLabel = formatCallStartTime(callSession?.startedAt);
  const answeredAt = Number(callSession?.answeredAt || callSession?.startedAt || Date.now());
  const duration = formatCallDuration(Date.now() - answeredAt);

  return `${starterName} started a ${mode} call at ${startedAtLabel}. Duration ${duration}.`;
}

function completeAuth(socket, userKey, callback, existingSessionId = null, requestedDeviceName = "") {
  const previousUserKey = socketToUser.get(socket.id);

  authenticateSocket(socket, userKey);
  const deliveredChats = markUndeliveredMessagesAsDeliveredForUser(userKey);

  if (deliveredChats.length > 0) {
    scheduleStatePersistence();
  }

  const sessionId =
    existingSessionId && sessions.get(existingSessionId) === userKey
      ? existingSessionId
      : createSessionForUser(userKey, socket, requestedDeviceName);

  if (sessionId === existingSessionId) {
    touchSessionForSocket(socket, sessionId, requestedDeviceName);
  }

  safeAck(callback, {
    ok: true,
    user: serializeAccount(userKey),
    sessionId
  });

  if (previousUserKey && previousUserKey !== userKey) {
    emitPresenceStateForFriends(previousUserKey);
  }

  emitRelationshipStateForNetwork(userKey);
  for (const chat of deliveredChats) {
    emitChatState(chat);
  }
  emitChatSummaries(userKey);
  emitSessionStateForUser(userKey);
}

function validateAvatarDataUrl(avatarDataUrl) {
  if (avatarDataUrl === null || avatarDataUrl === undefined || avatarDataUrl === "") {
    return { avatarDataUrl: null };
  }

  const value = String(avatarDataUrl);

  if (!value.startsWith("data:image/")) {
    return { error: "Avatar must be an image file." };
  }

  if (value.length > MAX_AVATAR_DATA_URL_LENGTH) {
    return { error: "Avatar is too large (max 2 MB)." };
  }

  return { avatarDataUrl: value };
}

function emitChatsContainingUser(userKey) {
  for (const chat of chats.values()) {
    if (chat.participants.includes(userKey)) {
      emitChatState(chat);
    }
  }
}

await restoreStateFromDatabase();

io.on("connection", (socket) => {
  socket.on("resume_session", (payload, callback) => {
    const sessionId = String(payload?.sessionId ?? "").trim();
    const userKey = sessions.get(sessionId);

    if (!sessionId || !userKey || !users.has(userKey)) {
      if (sessionId) {
        invalidateSession(sessionId);
      }

      safeAck(callback, { ok: false, error: "Session expired." });
      return;
    }

    completeAuth(socket, userKey, callback, sessionId, payload?.deviceName);
  });

  socket.on("signup", (payload, callback) => {
    const displayName = cleanDisplayName(payload?.username);
    const userKey = normalizeUserName(displayName);
    const password = cleanPassword(payload?.password);

    if (!displayName || displayName.length < 2) {
      safeAck(callback, {
        ok: false,
        error: "Username must be at least 2 characters."
      });
      return;
    }

    if (password.length < 4) {
      safeAck(callback, {
        ok: false,
        error: "Password must be at least 4 characters."
      });
      return;
    }

    if (users.has(userKey)) {
      safeAck(callback, {
        ok: false,
        error: "That username already exists."
      });
      return;
    }

    createUser(userKey, displayName, password);
    completeAuth(socket, userKey, callback, null, payload?.deviceName);
  });

  socket.on("login", (payload, callback) => {
    const username = cleanDisplayName(payload?.username);
    const userKey = resolveUserKey(username);
    const password = cleanPassword(payload?.password);

    if (!userKey || !users.has(userKey)) {
      safeAck(callback, {
        ok: false,
        error: "Account not found."
      });
      return;
    }

    const user = users.get(userKey);

    if (!verifyUserPassword(user, password)) {
      safeAck(callback, {
        ok: false,
        error: "Incorrect password."
      });
      return;
    }

    if (maybeMigrateLegacyPassword(user)) {
      scheduleStatePersistence();
    }

    completeAuth(socket, userKey, callback, null, payload?.deviceName);
  });

  socket.on("update_profile", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const user = users.get(currentUser);

    if (!user) {
      safeAck(callback, { ok: false, error: "Account not found." });
      return;
    }

    const nextDisplayName = cleanDisplayName(payload?.displayName ?? user.name);
    const avatarValidation = validateAvatarDataUrl(payload?.avatarDataUrl);

    if (!nextDisplayName || nextDisplayName.length < 2) {
      safeAck(callback, { ok: false, error: "Name must be at least 2 characters." });
      return;
    }

    if (avatarValidation.error) {
      safeAck(callback, { ok: false, error: avatarValidation.error });
      return;
    }

    const normalizedNextName = normalizeUserName(nextDisplayName);

    for (const [userKey, account] of users.entries()) {
      if (userKey === currentUser) {
        continue;
      }

      if (userKey === normalizedNextName) {
        safeAck(callback, { ok: false, error: "Username is already in use." });
        return;
      }

      if (normalizeUserName(account.name) === normalizedNextName) {
        safeAck(callback, { ok: false, error: "Username is already in use." });
        return;
      }
    }

    user.name = nextDisplayName;
    user.avatarDataUrl = avatarValidation.avatarDataUrl;
    scheduleStatePersistence();

    safeAck(callback, { ok: true, user: serializeAccount(currentUser) });
    emitToUser(currentUser, "account_updated", { user: serializeAccount(currentUser) });

    emitRelationshipStateForNetwork(currentUser);
    emitChatsContainingUser(currentUser);
  });

  socket.on("change_password", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const user = users.get(currentUser);

    if (!user) {
      safeAck(callback, { ok: false, error: "Account not found." });
      return;
    }

    const currentPassword = cleanPassword(payload?.currentPassword);
    const nextPassword = cleanPassword(payload?.newPassword);

    if (!verifyUserPassword(user, currentPassword)) {
      safeAck(callback, { ok: false, error: "Current password is incorrect." });
      return;
    }

    if (nextPassword.length < 4) {
      safeAck(callback, { ok: false, error: "New password must be at least 4 characters." });
      return;
    }

    setUserPassword(user, nextPassword);
    scheduleStatePersistence();
    safeAck(callback, { ok: true });
  });

  socket.on("delete_account", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const user = users.get(currentUser);

    if (!user) {
      safeAck(callback, { ok: false, error: "Account not found." });
      return;
    }

    const password = cleanPassword(payload?.password);

    if (!verifyUserPassword(user, password)) {
      safeAck(callback, { ok: false, error: "Password is incorrect." });
      return;
    }

    const impactedUsers = new Set([
      ...user.friends,
      ...user.incomingRequests,
      ...user.outgoingRequests
    ]);

    for (const account of users.values()) {
      account.friends.delete(currentUser);
      account.incomingRequests.delete(currentUser);
      account.outgoingRequests.delete(currentUser);
      account.friendAliases.delete(currentUser);
    }

    const chatIdsToDelete = [];

    for (const [chatId, chat] of chats.entries()) {
      if (!chat.participants.includes(currentUser)) {
        continue;
      }

      if (chat.type === "group") {
        chat.participants = chat.participants.filter((participant) => participant !== currentUser);

        if (chat.participants.length < 2) {
          chatIdsToDelete.push(chatId);
          continue;
        }

        appendSystemMessage(chat, `${user.name} left the group.`, "member_left");
        chat.updatedAt = Date.now();
        emitChatState(chat);
        continue;
      }

      chatIdsToDelete.push(chatId);
    }

    for (const chatId of chatIdsToDelete) {
      const chat = chats.get(chatId);

      if (!chat) {
        continue;
      }

      chats.delete(chatId);
      activeCalls.delete(chatId);
      removeChatFromHiddenLists(chatId);

      for (const participant of chat.participants) {
        if (participant === currentUser) {
          continue;
        }

        emitChatSummaries(participant);
        emitToUser(participant, "chat_removed", {
          chatId,
          reason: "account_deleted"
        });
      }
    }

    invalidateSessionsForUser(currentUser);
    users.delete(currentUser);
    const activeSockets = [...(onlineUsers.get(currentUser) ?? new Set())];
    onlineUsers.delete(currentUser);

    for (const socketId of activeSockets) {
      socketToUser.delete(socketId);
    }

    scheduleStatePersistence();

    safeAck(callback, { ok: true });

    for (const impactedKey of impactedUsers) {
      emitRelationshipState(impactedKey);
      emitChatSummaries(impactedKey);
    }

    for (const socketId of activeSockets) {
      io.to(socketId).emit("account_deleted");
      const accountSocket = io.sockets.sockets.get(socketId);

      if (accountSocket) {
        accountSocket.disconnect(true);
      }
    }
  });

  socket.on("send_friend_request", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const withUserKey = resolveUserKey(payload?.userKey ?? payload?.username);

    if (!withUserKey || !users.has(withUserKey)) {
      safeAck(callback, { ok: false, error: "User not found." });
      return;
    }

    if (withUserKey === currentUser) {
      safeAck(callback, { ok: false, error: "You cannot add yourself." });
      return;
    }

    if (areFriends(currentUser, withUserKey)) {
      safeAck(callback, { ok: false, error: "Already friends." });
      return;
    }

    const me = users.get(currentUser);
    const target = users.get(withUserKey);

    if (me.outgoingRequests.has(withUserKey)) {
      safeAck(callback, { ok: false, error: "Friend request already sent." });
      return;
    }

    if (me.incomingRequests.has(withUserKey)) {
      safeAck(callback, {
        ok: false,
        error: "This user already requested you."
      });
      return;
    }

    me.outgoingRequests.add(withUserKey);
    target.incomingRequests.add(currentUser);
    scheduleStatePersistence();

    safeAck(callback, { ok: true });

    emitRelationshipState(currentUser);
    emitRelationshipState(withUserKey);

    emitToUser(withUserKey, "notification", {
      message: `${displayNameFor(currentUser)} sent you a friend request.`
    });
  });

  socket.on("cancel_friend_request", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const targetKey = resolveUserKey(payload?.toUserKey ?? payload?.toUser);

    if (!targetKey || !users.has(targetKey)) {
      safeAck(callback, { ok: false, error: "User not found." });
      return;
    }

    const me = users.get(currentUser);
    const target = users.get(targetKey);

    if (!me.outgoingRequests.has(targetKey)) {
      safeAck(callback, { ok: false, error: "No outgoing request for that user." });
      return;
    }

    me.outgoingRequests.delete(targetKey);
    target.incomingRequests.delete(currentUser);
    scheduleStatePersistence();

    safeAck(callback, { ok: true });

    emitRelationshipState(currentUser);
    emitRelationshipState(targetKey);
  });

  socket.on("accept_friend_request", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const fromKey = resolveUserKey(payload?.fromUserKey ?? payload?.fromUser);

    if (!fromKey || !users.has(fromKey)) {
      safeAck(callback, { ok: false, error: "User not found." });
      return;
    }

    const me = users.get(currentUser);
    const fromUser = users.get(fromKey);

    if (!me.incomingRequests.has(fromKey)) {
      safeAck(callback, { ok: false, error: "No incoming request from that user." });
      return;
    }

    me.incomingRequests.delete(fromKey);
    fromUser.outgoingRequests.delete(currentUser);

    me.friends.add(fromKey);
    fromUser.friends.add(currentUser);
    me.friendAliases.delete(fromKey);
    fromUser.friendAliases.delete(currentUser);
    scheduleStatePersistence();

    safeAck(callback, { ok: true });

    emitRelationshipState(currentUser);
    emitRelationshipState(fromKey);

    emitToUser(fromKey, "notification", {
      message: `${displayNameFor(currentUser)} accepted your friend request.`
    });
  });

  socket.on("decline_friend_request", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const fromKey = resolveUserKey(payload?.fromUserKey ?? payload?.fromUser);

    if (!fromKey || !users.has(fromKey)) {
      safeAck(callback, { ok: false, error: "User not found." });
      return;
    }

    const me = users.get(currentUser);
    const fromUser = users.get(fromKey);

    if (!me.incomingRequests.has(fromKey)) {
      safeAck(callback, { ok: false, error: "No incoming request from that user." });
      return;
    }

    me.incomingRequests.delete(fromKey);
    fromUser.outgoingRequests.delete(currentUser);
    scheduleStatePersistence();

    safeAck(callback, { ok: true });

    emitRelationshipState(currentUser);
    emitRelationshipState(fromKey);
  });

  socket.on("set_friend_alias", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const friendKey = resolveUserKey(payload?.friendUserKey ?? payload?.friendUser);

    if (!friendKey || !users.has(friendKey)) {
      safeAck(callback, { ok: false, error: "User not found." });
      return;
    }

    if (!areFriends(currentUser, friendKey)) {
      safeAck(callback, { ok: false, error: "You can only rename users in your friends list." });
      return;
    }

    const alias = cleanAlias(payload?.alias);
    const me = users.get(currentUser);

    if (!alias) {
      me.friendAliases.delete(friendKey);
    } else {
      me.friendAliases.set(friendKey, alias);
    }
    scheduleStatePersistence();

    safeAck(callback, { ok: true });
    emitRelationshipState(currentUser);
    emitChatSummaries(currentUser);
    emitChatsContainingUser(currentUser);
  });

  socket.on("remove_friend", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const friendKey = resolveUserKey(payload?.friendUserKey ?? payload?.friendUser);

    if (!friendKey || !users.has(friendKey)) {
      safeAck(callback, { ok: false, error: "User not found." });
      return;
    }

    if (!areFriends(currentUser, friendKey)) {
      safeAck(callback, { ok: false, error: "That user is not in your friends list." });
      return;
    }

    const me = users.get(currentUser);
    const friend = users.get(friendKey);

    me.friends.delete(friendKey);
    friend.friends.delete(currentUser);

    me.friendAliases.delete(friendKey);
    friend.friendAliases.delete(currentUser);
    scheduleStatePersistence();

    safeAck(callback, { ok: true });

    emitRelationshipState(currentUser);
    emitRelationshipState(friendKey);

    emitToUser(friendKey, "notification", {
      message: `${displayNameFor(currentUser)} removed you from friends.`
    });
  });

  socket.on("create_group_from_direct", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const directUserKey = resolveUserKey(payload?.directUserKey ?? payload?.directUser);
    const rawExtraUsers = Array.isArray(payload?.extraUserKeys)
      ? payload.extraUserKeys
      : [payload?.extraUserKey ?? payload?.extraUser];
    const extraUserKeys = new Set();

    for (const rawExtraUser of rawExtraUsers) {
      const extraUserKey = resolveUserKey(rawExtraUser);

      if (extraUserKey) {
        extraUserKeys.add(extraUserKey);
      }
    }

    extraUserKeys.delete(currentUser);
    extraUserKeys.delete(directUserKey);

    if (!directUserKey || extraUserKeys.size === 0) {
      safeAck(callback, { ok: false, error: "Pick valid friends for the new group." });
      return;
    }

    if (directUserKey === currentUser) {
      safeAck(callback, { ok: false, error: "You can only add other users to a group." });
      return;
    }

    if (extraUserKeys.size > 8) {
      safeAck(callback, { ok: false, error: "You can add up to 8 extra friends." });
      return;
    }

    const me = users.get(currentUser);

    if (!me) {
      safeAck(callback, { ok: false, error: "Account not found." });
      return;
    }

    for (const memberKey of [directUserKey, ...extraUserKeys]) {
      if (!users.has(memberKey)) {
        safeAck(callback, { ok: false, error: "One of those users no longer exists." });
        return;
      }

      if (!me.friends.has(memberKey)) {
        safeAck(callback, {
          ok: false,
          error: "You can only add users from your friends list."
        });
        return;
      }
    }

    const chat = createGroupChat(currentUser, "", [directUserKey, ...extraUserKeys]);

    safeAck(callback, { ok: true, chat: serializeChat(chat, currentUser) });
    emitGroupCreatedEvents(chat, currentUser);
  });

  socket.on("create_group", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const groupName = cleanGroupName(payload?.name);

    if (groupName.length < 2) {
      safeAck(callback, {
        ok: false,
        error: "Group name must have at least 2 characters."
      });
      return;
    }

    const rawMembers = Array.isArray(payload?.members) ? payload.members : [];
    const memberKeys = new Set();

    for (const rawMember of rawMembers) {
      const memberKey = normalizeUserName(rawMember);

      if (memberKey && memberKey !== currentUser) {
        memberKeys.add(memberKey);
      }
    }

    if (memberKeys.size === 0) {
      safeAck(callback, { ok: false, error: "Pick at least one friend for the group." });
      return;
    }

    const me = users.get(currentUser);

    for (const memberKey of memberKeys) {
      if (!users.has(memberKey)) {
        safeAck(callback, {
          ok: false,
          error: `User "${memberKey}" does not exist.`
        });
        return;
      }

      if (!me.friends.has(memberKey)) {
        safeAck(callback, {
          ok: false,
          error: "You can only add users from your friends list."
        });
        return;
      }
    }

    const chat = createGroupChat(currentUser, groupName, [...memberKeys]);

    safeAck(callback, { ok: true, chat: serializeChat(chat, currentUser) });
    emitGroupCreatedEvents(chat, currentUser);
  });

  socket.on("open_chat", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const withUserKey = resolveUserKey(payload?.withUserKey ?? payload?.withUser);

    if (!withUserKey || !users.has(withUserKey)) {
      safeAck(callback, { ok: false, error: "User not found." });
      return;
    }

    if (withUserKey === currentUser) {
      safeAck(callback, { ok: false, error: "You cannot chat with yourself." });
      return;
    }

    if (!areFriends(currentUser, withUserKey)) {
      safeAck(callback, {
        ok: false,
        error: "You can only start chats with friends."
      });
      return;
    }

    const { chat, created } = createOrGetDirectChat(
      currentUser,
      withUserKey,
      Boolean(payload?.isTemp)
    );

    if (setChatHiddenForUser(currentUser, chat.id, false)) {
      scheduleStatePersistence();
    }

    safeAck(callback, { ok: true, chat: serializeChat(chat, currentUser) });

    emitChatSummaries(currentUser);
    emitChatSummaries(withUserKey);

    if (created) {
      emitToUser(withUserKey, "chat_invite", {
        from: displayNameFor(currentUser),
        chatId: chat.id,
        isTemp: chat.type === "temp"
      });
    }
  });

  socket.on("load_chat", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    const readChanged = markChatMessagesRead(chat, currentUser);

    if (readChanged) {
      scheduleStatePersistence();
      emitChatState(chat);
    }

    safeAck(callback, { ok: true, chat: serializeChat(chat, currentUser) });
    hydrateWeakVideoPreviewsForChat(chat, 4);
  });

  socket.on("hide_chat", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    if (setChatHiddenForUser(currentUser, chatId, true)) {
      scheduleStatePersistence();
    }

    emitChatSummaries(currentUser);
    safeAck(callback, { ok: true });
  });

  socket.on("typing_state", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const isTyping = Boolean(payload?.isTyping);
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    relayToOtherParticipants(chat, currentUser, "typing_state", {
      chatId,
      fromKey: currentUser,
      fromName: displayNameFor(currentUser),
      isTyping,
      at: new Date().toISOString()
    });

    safeAck(callback, { ok: true });
  });

  socket.on("mark_chat_read", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    const changed = markChatMessagesRead(chat, currentUser);

    if (changed) {
      scheduleStatePersistence();
      emitChatState(chat);
    }

    safeAck(callback, { ok: true });
  });

  socket.on("set_chat_preferences", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "").trim();
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    if (!(chat.viewerPrefs instanceof Map)) {
      chat.viewerPrefs = new Map();
    }

    const currentPrefs = viewerChatPreferencesForUser(chat, currentUser);
    const nextPrefs = {
      nickname: currentPrefs.nickname,
      theme: currentPrefs.theme,
      wallpaper: currentPrefs.wallpaper
    };

    const hasNickname = Object.prototype.hasOwnProperty.call(payload ?? {}, "nickname");
    const hasTheme = Object.prototype.hasOwnProperty.call(payload ?? {}, "theme");
    const hasWallpaper = Object.prototype.hasOwnProperty.call(payload ?? {}, "wallpaper");

    if (hasNickname) {
      const nickname = cleanChatNickname(payload?.nickname);
      nextPrefs.nickname = nickname || null;
    }

    if (hasTheme) {
      nextPrefs.theme = normalizeChatTheme(payload?.theme);
    }

    if (hasWallpaper) {
      const rawWallpaper = String(payload?.wallpaper ?? "").trim();
      const wallpaper = normalizeChatWallpaper(rawWallpaper);

      if (rawWallpaper && !wallpaper) {
        safeAck(callback, {
          ok: false,
          error: "Wallpaper must be a valid http(s) or data:image URL."
        });
        return;
      }

      nextPrefs.wallpaper = wallpaper;
    }

    const normalizedNext = normalizeViewerChatPreference(nextPrefs);
    const normalizedCurrent = normalizeViewerChatPreference(currentPrefs);
    const nextSerialized = JSON.stringify(normalizedNext ?? null);
    const currentSerialized = JSON.stringify(normalizedCurrent ?? null);

    if (nextSerialized === currentSerialized) {
      safeAck(callback, { ok: true, prefs: normalizedNext ?? null });
      return;
    }

    if (normalizedNext) {
      chat.viewerPrefs.set(currentUser, normalizedNext);
    } else {
      chat.viewerPrefs.delete(currentUser);
    }

    scheduleStatePersistence();
    emitChatState(chat);
    safeAck(callback, { ok: true, prefs: normalizedNext ?? null });
  });

  socket.on("toggle_reaction", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const messageId = String(payload?.messageId ?? "");
    const emoji = normalizeReactionEmoji(payload?.emoji);
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    if (!emoji) {
      safeAck(callback, { ok: false, error: "Invalid reaction." });
      return;
    }

    const message = chat.messages.find((entry) => entry.id === messageId);

    if (!message || message.senderKey === "system") {
      safeAck(callback, { ok: false, error: "Message not found." });
      return;
    }

    if (message.deleted) {
      safeAck(callback, { ok: false, error: "Cannot react to deleted messages." });
      return;
    }

    toggleReactionForMessage(message, currentUser, emoji);
    scheduleStatePersistence();
    emitChatState(chat);
    safeAck(callback, { ok: true });
  });

  socket.on("rename_group", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser) || chat.type !== "group") {
      safeAck(callback, { ok: false, error: "Group chat not found." });
      return;
    }

    const nextName = cleanGroupName(payload?.name);

    if (nextName.length < 2) {
      safeAck(callback, { ok: false, error: "Group name must have at least 2 characters." });
      return;
    }

    if (chat.name === nextName) {
      safeAck(callback, { ok: true });
      return;
    }

    chat.name = nextName;
    appendSystemMessage(chat, `${displayNameFor(currentUser)} renamed the group.`, "group_renamed");
    chat.updatedAt = Date.now();
    scheduleStatePersistence();

    emitChatState(chat);
    safeAck(callback, { ok: true });
  });

  socket.on("add_group_members", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser) || chat.type !== "group") {
      safeAck(callback, { ok: false, error: "Group chat not found." });
      return;
    }

    const rawMembers = Array.isArray(payload?.memberUserKeys)
      ? payload.memberUserKeys
      : Array.isArray(payload?.memberUsers)
        ? payload.memberUsers
        : [payload?.memberUserKey ?? payload?.memberUser];
    const memberKeys = new Set();

    for (const rawMember of rawMembers) {
      const memberKey = resolveUserKey(rawMember);

      if (memberKey) {
        memberKeys.add(memberKey);
      }
    }

    for (const participant of chat.participants) {
      memberKeys.delete(participant);
    }
    memberKeys.delete(currentUser);

    if (memberKeys.size === 0) {
      safeAck(callback, { ok: false, error: "Pick at least one friend to add." });
      return;
    }

    if (memberKeys.size > MAX_GROUP_MEMBER_ADDS_PER_ACTION) {
      safeAck(callback, {
        ok: false,
        error: `You can add up to ${MAX_GROUP_MEMBER_ADDS_PER_ACTION} friends at once.`
      });
      return;
    }

    const me = users.get(currentUser);

    if (!me) {
      safeAck(callback, { ok: false, error: "Account not found." });
      return;
    }

    const addedKeys = [];

    for (const memberKey of memberKeys) {
      if (!users.has(memberKey)) {
        safeAck(callback, { ok: false, error: "One of those users no longer exists." });
        return;
      }

      if (!me.friends.has(memberKey)) {
        safeAck(callback, {
          ok: false,
          error: "You can only add users from your friends list."
        });
        return;
      }

      addedKeys.push(memberKey);
    }

    if (addedKeys.length === 0) {
      safeAck(callback, { ok: true, chat: serializeChat(chat, currentUser) });
      return;
    }

    chat.participants = sortUserKeys([...chat.participants, ...addedKeys]);
    appendSystemMessage(
      chat,
      `${displayNameFor(currentUser)} added ${addedKeys.map((memberKey) => displayNameFor(memberKey)).join(", ")}.`,
      "member_added"
    );
    chat.updatedAt = Date.now();
    clearChatHiddenForParticipants(chat.id, chat.participants);
    scheduleStatePersistence();
    emitChatState(chat);

    for (const memberKey of addedKeys) {
      emitToUser(memberKey, "group_created", {
        from: displayNameFor(currentUser),
        groupName: chat.name,
        chatId: chat.id
      });
    }

    safeAck(callback, { ok: true, chat: serializeChat(chat, currentUser) });
  });

  socket.on("delete_group", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser) || chat.type !== "group") {
      safeAck(callback, { ok: false, error: "Group chat not found." });
      return;
    }

    const participants = [...chat.participants];
    chats.delete(chatId);
    activeCalls.delete(chatId);
    removeChatFromHiddenLists(chatId);
    scheduleStatePersistence();
    safeAck(callback, { ok: true });

    for (const participant of participants) {
      emitChatSummaries(participant);
      emitToUser(participant, "chat_removed", {
        chatId,
        reason: "group_deleted"
      });
    }
  });

  socket.on("send_message", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    const text = String(payload?.text ?? "").trim();

    if (text.length > MAX_MESSAGE_LENGTH) {
      safeAck(callback, { ok: false, error: "Message is too long." });
      return;
    }

    const attachmentResult = validateAndSanitizeAttachments(payload?.attachments);

    if (attachmentResult.error) {
      safeAck(callback, { ok: false, error: attachmentResult.error });
      return;
    }

    const attachments = attachmentResult.attachments;

    if (!text && attachments.length === 0) {
      safeAck(callback, { ok: false, error: "Message cannot be empty." });
      return;
    }

    const replyToMessageId = String(payload?.replyToMessageId ?? "").trim();
    let replyTarget = null;

    if (replyToMessageId) {
      replyTarget = chat.messages.find((entry) => entry.id === replyToMessageId) ?? null;

      if (!replyTarget) {
        safeAck(callback, { ok: false, error: "Reply target was not found." });
        return;
      }
    }

    if (chat.type === "temp" && chat.closeVotes.size > 0) {
      chat.closeVotes.clear();
      appendSystemMessage(
        chat,
        "Temp close request canceled because a new message was sent.",
        "temp_close_reset"
      );
    }

    if (clearChatHiddenForParticipants(chat.id, chat.participants)) {
      scheduleStatePersistence();
    }

    const sentAt = new Date().toISOString();
    const deliveredBy = {
      [currentUser]: sentAt
    };

    for (const participant of chat.participants) {
      if (participant === currentUser) {
        continue;
      }

      if (isOnline(participant)) {
        deliveredBy[participant] = sentAt;
      }
    }

    const replySnapshot = replyTarget
      ? {
          messageId: replyTarget.id,
          senderKey: replyTarget.senderKey,
          senderName: replyTarget.senderName,
          text: messagePreview(replyTarget).slice(0, 220),
          deleted: Boolean(replyTarget.deleted)
        }
      : null;

    const messageId = crypto.randomUUID();
    const previewCandidate = linkPreviewForText(text);

    chat.messages.push({
      id: messageId,
      senderKey: currentUser,
      senderName: displayNameFor(currentUser),
      text,
      sentAt,
      attachments,
      linkPreview: previewCandidate.preview,
      replyToMessageId: replyTarget?.id ?? null,
      replyTo: replySnapshot,
      reactions: [],
      deliveredBy,
      readBy: {
        [currentUser]: sentAt
      }
    });

    chat.updatedAt = Date.now();
    scheduleStatePersistence();

    emitChatState(chat);
    safeAck(callback, { ok: true });

    const sentMessage = chat.messages.at(-1);

    if (previewCandidate.previewUrl && !previewCandidate.cacheHit) {
      hydrateMessageLinkPreview(chat.id, messageId, previewCandidate.previewUrl).catch(() => {});
    }

    const preview = messagePreview(sentMessage);

    for (const participant of chat.participants) {
      if (participant === currentUser) {
        continue;
      }

      emitToUser(participant, "incoming_message", {
        chatId: chat.id,
        from: displayNameFor(currentUser),
        title: chatTitleForUser(chat, participant),
        preview,
        sentAt: sentMessage.sentAt
      });
    }
  });

  socket.on("edit_message", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const messageId = String(payload?.messageId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    const message = chat.messages.find((entry) => entry.id === messageId);

    if (!message) {
      safeAck(callback, { ok: false, error: "Message not found." });
      return;
    }

    if (message.senderKey !== currentUser || message.senderKey === "system") {
      safeAck(callback, { ok: false, error: "You can edit only your own messages." });
      return;
    }

    if (message.deleted) {
      safeAck(callback, { ok: false, error: "Deleted messages cannot be edited." });
      return;
    }

    const nextText = String(payload?.text ?? "").trim();

    if (nextText.length > MAX_MESSAGE_LENGTH) {
      safeAck(callback, { ok: false, error: "Message is too long." });
      return;
    }

    const attachmentCount = Array.isArray(message.attachments)
      ? message.attachments.length
      : 0;

    if (!nextText && attachmentCount === 0) {
      safeAck(callback, { ok: false, error: "Message cannot be empty." });
      return;
    }

    const previewCandidate = linkPreviewForText(nextText);
    message.text = nextText;
    message.linkPreview = previewCandidate.preview;
    message.editedAt = new Date().toISOString();
    chat.updatedAt = Date.now();
    scheduleStatePersistence();

    emitChatState(chat);

    if (previewCandidate.previewUrl && !previewCandidate.cacheHit) {
      hydrateMessageLinkPreview(chat.id, message.id, previewCandidate.previewUrl).catch(() => {});
    }

    safeAck(callback, { ok: true });
  });

  socket.on("delete_message", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const messageId = String(payload?.messageId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    const message = chat.messages.find((entry) => entry.id === messageId);

    if (!message) {
      safeAck(callback, { ok: false, error: "Message not found." });
      return;
    }

    if (message.senderKey !== currentUser || message.senderKey === "system") {
      safeAck(callback, { ok: false, error: "You can delete only your own messages." });
      return;
    }

    if (message.deleted) {
      safeAck(callback, { ok: true });
      return;
    }

    message.text = "";
    message.attachments = [];
    message.linkPreview = null;
    message.reactions = [];
    message.editedAt = null;
    message.deleted = true;
    message.deletedAt = new Date().toISOString();
    chat.updatedAt = Date.now();
    scheduleStatePersistence();

    emitChatState(chat);
    safeAck(callback, { ok: true });
  });

  socket.on("list_sessions", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const currentSessionId = socketToSession.get(socket.id) ?? null;

    safeAck(callback, {
      ok: true,
      currentSessionId,
      sessions: sessionsForUser(currentUser, currentSessionId)
    });
  });

  socket.on("revoke_session", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const currentSessionId = socketToSession.get(socket.id) ?? null;
    const targetSessionId = String(payload?.sessionId ?? "").trim();

    if (!targetSessionId) {
      safeAck(callback, { ok: false, error: "Session not found." });
      return;
    }

    if (sessions.get(targetSessionId) !== currentUser) {
      safeAck(callback, { ok: false, error: "Session not found." });
      return;
    }

    if (currentSessionId && targetSessionId === currentSessionId) {
      safeAck(callback, { ok: false, error: "Use Log Out for this device." });
      return;
    }

    invalidateSession(targetSessionId, {
      disconnectSockets: true,
      reason: "revoked"
    });

    emitSessionStateForUser(currentUser);

    safeAck(callback, {
      ok: true,
      revokedCount: 1,
      currentSessionId,
      sessions: sessionsForUser(currentUser, currentSessionId)
    });
  });

  socket.on("revoke_other_sessions", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const currentSessionId = socketToSession.get(socket.id) ?? null;

    if (!currentSessionId) {
      safeAck(callback, { ok: false, error: "Current session not found." });
      return;
    }

    const userSessions = [...(sessionsByUser.get(currentUser) ?? new Set())];
    let revokedCount = 0;

    for (const sessionId of userSessions) {
      if (sessionId === currentSessionId) {
        continue;
      }

      if (
        invalidateSession(sessionId, {
          disconnectSockets: true,
          reason: "revoked"
        })
      ) {
        revokedCount += 1;
      }
    }

    emitSessionStateForUser(currentUser);

    safeAck(callback, {
      ok: true,
      revokedCount,
      currentSessionId,
      sessions: sessionsForUser(currentUser, currentSessionId)
    });
  });

  socket.on("logout", (payload, callback) => {
    const payloadSessionId = String(payload?.sessionId ?? "").trim();
    const currentSessionId = socketToSession.get(socket.id) ?? null;
    const sessionId = payloadSessionId || currentSessionId;

    if (sessionId) {
      invalidateSession(sessionId);
    }

    const userKey = detachSocket(socket.id);

    if (userKey) {
      emitPresenceStateForFriends(userKey);
      emitSessionStateForUser(userKey);
    }

    safeAck(callback, { ok: true });
  });

  socket.on("vote_close_temp_chat", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const chat = chats.get(chatId);

    if (!chat || !chat.participants.includes(currentUser)) {
      safeAck(callback, { ok: false, error: "Chat not found." });
      return;
    }

    if (chat.type !== "temp") {
      safeAck(callback, {
        ok: false,
        error: "Only temp chats can be closed this way."
      });
      return;
    }

    if (chat.closeVotes.has(currentUser)) {
      safeAck(callback, { ok: true });
      emitChatState(chat);
      return;
    }

    const isFirstVote = chat.closeVotes.size === 0;
    chat.closeVotes.add(currentUser);

    if (isFirstVote) {
      appendSystemMessage(
        chat,
        `${displayNameFor(currentUser)} requested to close this temp chat.`,
        "temp_close_request"
      );
    }

    chat.updatedAt = Date.now();
    scheduleStatePersistence();
    safeAck(callback, { ok: true });

    if (chat.closeVotes.size === chat.participants.length) {
      chats.delete(chat.id);
      activeCalls.delete(chat.id);
      removeChatFromHiddenLists(chat.id);
      scheduleStatePersistence();

      for (const participant of chat.participants) {
        emitToUser(participant, "temp_chat_deleted", {
          chatId: chat.id,
          closedAt: new Date().toISOString()
        });

        emitChatSummaries(participant);
      }

      return;
    }

    emitChatState(chat);
  });

  socket.on("call_offer", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const mode = payload?.mode === "video" ? "video" : "voice";
    const offer = payload?.offer;
    const micEnabled = payload?.micEnabled !== false;
    const cameraEnabled = payload?.cameraEnabled !== false;

    if (!offer || typeof offer !== "object") {
      safeAck(callback, { ok: false, error: "Invalid call offer." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    startCallSession(callChatResult.chat, currentUser, mode);

    relayToOtherParticipants(callChatResult.chat, currentUser, "incoming_call", {
      chatId,
      mode,
      offer,
      fromKey: currentUser,
      fromName: displayNameFor(currentUser),
      micEnabled,
      cameraEnabled
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_answer", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const answer = payload?.answer;
    const micEnabled = payload?.micEnabled !== false;
    const cameraEnabled = payload?.cameraEnabled !== false;

    if (!answer || typeof answer !== "object") {
      safeAck(callback, { ok: false, error: "Invalid call answer." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    markCallAnswered(chatId);

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_answer", {
      chatId,
      answer,
      fromKey: currentUser,
      micEnabled,
      cameraEnabled
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_ice_candidate", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const candidate = payload?.candidate;

    if (!candidate || typeof candidate !== "object") {
      safeAck(callback, { ok: false, error: "Invalid ICE candidate." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_ice_candidate", {
      chatId,
      candidate,
      fromKey: currentUser
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_reconnect_offer", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const offer = payload?.offer;

    if (!offer || typeof offer !== "object") {
      safeAck(callback, { ok: false, error: "Invalid reconnect offer." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = activeCalls.get(chatId);

    if (!callSession || !callSession.answeredAt) {
      safeAck(callback, { ok: false, error: "No active call to reconnect." });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_reconnect_offer", {
      chatId,
      offer,
      fromKey: currentUser,
      fromName: displayNameFor(currentUser)
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_reconnect_answer", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const answer = payload?.answer;

    if (!answer || typeof answer !== "object") {
      safeAck(callback, { ok: false, error: "Invalid reconnect answer." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = activeCalls.get(chatId);

    if (!callSession || !callSession.answeredAt) {
      safeAck(callback, { ok: false, error: "No active call to reconnect." });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_reconnect_answer", {
      chatId,
      answer,
      fromKey: currentUser
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_renegotiate_offer", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const offer = payload?.offer;

    if (!offer || typeof offer !== "object") {
      safeAck(callback, { ok: false, error: "Invalid media update offer." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = activeCalls.get(chatId);

    if (!callSession || !callSession.answeredAt) {
      safeAck(callback, { ok: false, error: "No active call." });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_renegotiate_offer", {
      chatId,
      offer,
      fromKey: currentUser,
      fromName: displayNameFor(currentUser)
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_renegotiate_answer", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const answer = payload?.answer;

    if (!answer || typeof answer !== "object") {
      safeAck(callback, { ok: false, error: "Invalid media update answer." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = activeCalls.get(chatId);

    if (!callSession || !callSession.answeredAt) {
      safeAck(callback, { ok: false, error: "No active call." });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_renegotiate_answer", {
      chatId,
      answer,
      fromKey: currentUser
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_media_state", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const micEnabled = payload?.micEnabled !== false;
    const cameraEnabled = payload?.cameraEnabled !== false;
    const screenSharing = payload?.screenSharing === true;
    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_media_state", {
      chatId,
      fromKey: currentUser,
      fromName: displayNameFor(currentUser),
      micEnabled,
      cameraEnabled,
      screenSharing
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_request_camera_corner", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const corner = String(payload?.corner ?? "").trim();
    const validCorners = new Set(["top-left", "top-right", "bottom-left", "bottom-right"]);

    if (!validCorners.has(corner)) {
      safeAck(callback, { ok: false, error: "Invalid camera position." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = activeCalls.get(chatId);

    if (!callSession || !callSession.answeredAt) {
      safeAck(callback, { ok: false, error: "No active call." });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_set_camera_corner", {
      chatId,
      corner,
      fromKey: currentUser,
      fromName: displayNameFor(currentUser)
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_request_camera_position", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const rawX = Number(payload?.x);
    const rawY = Number(payload?.y);

    if (!Number.isFinite(rawX) || !Number.isFinite(rawY)) {
      safeAck(callback, { ok: false, error: "Invalid camera position." });
      return;
    }

    const x = Math.max(0, Math.min(1, rawX));
    const y = Math.max(0, Math.min(1, rawY));
    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = activeCalls.get(chatId);

    if (!callSession || !callSession.answeredAt) {
      safeAck(callback, { ok: false, error: "No active call." });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_set_camera_position", {
      chatId,
      x,
      y,
      fromKey: currentUser,
      fromName: displayNameFor(currentUser)
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_annotation", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const annotationType = payload?.type === "clear" ? "clear" : "segment";
    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = activeCalls.get(chatId);

    if (!callSession || !callSession.answeredAt) {
      safeAck(callback, { ok: false, error: "No active call." });
      return;
    }

    if (annotationType === "clear") {
      relayToOtherParticipants(callChatResult.chat, currentUser, "call_annotation", {
        chatId,
        type: "clear",
        fromKey: currentUser
      });

      safeAck(callback, { ok: true });
      return;
    }

    const asUnit = (value) => {
      const numeric = Number(value);

      if (!Number.isFinite(numeric)) {
        return null;
      }

      return Math.min(1, Math.max(0, numeric));
    };
    const fromX = asUnit(payload?.fromX);
    const fromY = asUnit(payload?.fromY);
    const toX = asUnit(payload?.toX);
    const toY = asUnit(payload?.toY);

    if (fromX === null || fromY === null || toX === null || toY === null) {
      safeAck(callback, { ok: false, error: "Invalid annotation coordinates." });
      return;
    }

    const rawColor = String(payload?.color ?? "#a8a8a8").trim().slice(0, 24);
    const color = /^#[0-9a-fA-F]{3,8}$/.test(rawColor) ? rawColor : "#a8a8a8";
    const width = Math.min(14, Math.max(1, Number(payload?.width) || 3));

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_annotation", {
      chatId,
      type: "segment",
      fromKey: currentUser,
      fromX,
      fromY,
      toX,
      toY,
      color,
      width
    });

    safeAck(callback, { ok: true });
  });

  socket.on("call_chat_message", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const text = String(payload?.text ?? "").trim();

    if (!text) {
      safeAck(callback, { ok: false, error: "Message cannot be empty." });
      return;
    }

    if (text.length > MAX_CALL_CHAT_LENGTH) {
      safeAck(callback, { ok: false, error: "Call chat message is too long." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = activeCalls.get(chatId);

    if (!callSession || !callSession.answeredAt) {
      safeAck(callback, { ok: false, error: "No active call chat is available." });
      return;
    }

    const message = {
      id: crypto.randomUUID(),
      senderKey: currentUser,
      senderName: displayNameFor(currentUser),
      text,
      sentAt: new Date().toISOString()
    };

    if (!Array.isArray(callSession.messages)) {
      callSession.messages = [];
    }

    callSession.messages.push(message);

    if (callSession.messages.length > MAX_CALL_CHAT_MESSAGES) {
      callSession.messages = callSession.messages.slice(-MAX_CALL_CHAT_MESSAGES);
    }

    for (const participant of callChatResult.chat.participants) {
      emitToUser(participant, "call_chat_message", {
        chatId,
        message
      });
    }

    safeAck(callback, { ok: true, message });
  });

  socket.on("end_call", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const reason = String(payload?.reason ?? "ended").slice(0, 40);
    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = consumeCallSession(chatId);
    if (callSession) {
      if (reason === "no_answer" || reason === "missed") {
        appendCallLog(
          callChatResult.chat,
          timedOutCallLogText(callSession),
          "call_missed"
        );
      } else if (callSession.answeredAt) {
        appendCallLog(
          callChatResult.chat,
          completedCallLogText(callSession),
          "call_summary"
        );
      }
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_ended", {
      chatId,
      fromKey: currentUser,
      fromName: displayNameFor(currentUser),
      reason
    });

    safeAck(callback, { ok: true });
  });

  socket.on("reject_call", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const reason = String(payload?.reason ?? "declined").slice(0, 40);
    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    const callSession = consumeCallSession(chatId);
    if (callSession) {
      const isTimedOut = reason === "missed" || reason === "no_answer";
      appendCallLog(
        callChatResult.chat,
        isTimedOut ? timedOutCallLogText(callSession) : declinedCallLogText(callSession),
        isTimedOut ? "call_missed" : "call_declined"
      );
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_rejected", {
      chatId,
      fromKey: currentUser,
      reason
    });

    safeAck(callback, { ok: true });
  });

  socket.on("disconnect", () => {
    const userKey = detachSocket(socket.id);

    if (!userKey) {
      return;
    }

    emitPresenceStateForFriends(userKey);
  });
});

process.on("beforeExit", () => {
  persistStateToDatabase();
});

process.on("SIGINT", () => {
  persistStateToDatabase();
  process.exit(0);
});

process.on("SIGTERM", () => {
  persistStateToDatabase();
  process.exit(0);
});

server.listen(PORT, () => {
  console.log(
    `Server listening on http://localhost:${PORT} (persistence: ${persistenceBackend}, stateEncryption: ${stateEncryptionKey ? "on" : "off"})`
  );
});
