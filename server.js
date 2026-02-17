import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
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
const MAX_CALL_CHAT_LENGTH = 500;
const MAX_CALL_CHAT_MESSAGES = 160;
const MAX_GROUP_MEMBER_ADDS_PER_ACTION = 8;
const DATA_DIR = process.env.SHADOW_CHAT_DATA_DIR
  ? path.resolve(process.env.SHADOW_CHAT_DATA_DIR)
  : path.join(__dirname, "data");
const DB_PATH = process.env.SHADOW_CHAT_DB_PATH
  ? path.resolve(process.env.SHADOW_CHAT_DB_PATH)
  : path.join(DATA_DIR, "shadow-chat.sqlite");
const STATE_SNAPSHOT_PATH = process.env.SHADOW_CHAT_STATE_PATH
  ? path.resolve(process.env.SHADOW_CHAT_STATE_PATH)
  : path.join(DATA_DIR, "shadow-chat-state.json");
const MYSQL_URL = String(process.env.MYSQL_URL ?? "").trim();
const MYSQL_HOST = String(process.env.MYSQL_HOST ?? "").trim();
const MYSQL_PORT = Number(process.env.MYSQL_PORT ?? 3306);
const MYSQL_USER = String(process.env.MYSQL_USER ?? "").trim();
const MYSQL_PASSWORD = String(process.env.MYSQL_PASSWORD ?? "");
const MYSQL_DATABASE = String(process.env.MYSQL_DATABASE ?? "").trim();
const MYSQL_SSL_MODE = String(process.env.MYSQL_SSL_MODE ?? "").trim().toLowerCase();
const MYSQL_CONFIGURED =
  Boolean(MYSQL_URL) || Boolean(MYSQL_HOST && MYSQL_USER && MYSQL_DATABASE);

app.use(express.static(path.join(__dirname, "public")));

const users = new Map();
const onlineUsers = new Map();
const socketToUser = new Map();
const chats = new Map();
const activeCalls = new Map();
const sessions = new Map();
const sessionsByUser = new Map();
let persistenceScheduled = false;

let persistenceBackend = "json";
let mysqlPool = null;
let selectStateStatement = null;
let upsertStateStatement = null;

function buildMysqlSslOption(rawMode) {
  const mode = String(rawMode ?? "").trim().toLowerCase();

  if (!mode || mode === "off" || mode === "false" || mode === "disabled" || mode === "none") {
    return null;
  }

  if (mode === "insecure" || mode === "allow_invalid" || mode === "allow-invalid") {
    return { rejectUnauthorized: false };
  }

  if (mode === "verify" || mode === "verify_ca" || mode === "verify_identity") {
    return { rejectUnauthorized: true };
  }

  // "preferred"/"required"/"on" all map to TLS enabled.
  return {};
}

function buildMysqlConnectionConfig() {
  const baseConfig = {
    waitForConnections: true,
    connectionLimit: 10
  };

  if (MYSQL_URL) {
    const parsedUrl = new URL(MYSQL_URL);
    const dbName = parsedUrl.pathname.replace(/^\/+/, "");
    const parsedPort = Number(parsedUrl.port);

    return {
      ...baseConfig,
      host: parsedUrl.hostname,
      port: Number.isFinite(parsedPort) && parsedPort > 0 ? parsedPort : 3306,
      user: decodeURIComponent(parsedUrl.username || ""),
      password: decodeURIComponent(parsedUrl.password || ""),
      database: decodeURIComponent(dbName || "")
    };
  }

  return {
    ...baseConfig,
    host: MYSQL_HOST,
    port: Number.isFinite(MYSQL_PORT) ? MYSQL_PORT : 3306,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DATABASE
  };
}

if (MYSQL_CONFIGURED) {
  try {
    const mysql = await import("mysql2/promise");
    const connectionConfig = buildMysqlConnectionConfig();
    const sslOption = buildMysqlSslOption(MYSQL_SSL_MODE);

    if (sslOption) {
      connectionConfig.ssl = sslOption;
    }

    mysqlPool = mysql.createPool(connectionConfig);

    await mysqlPool.execute(`
      CREATE TABLE IF NOT EXISTS app_state (
        \`key\` VARCHAR(64) PRIMARY KEY,
        value LONGTEXT NOT NULL
      )
    `);
    persistenceBackend = "mysql";
  } catch (error) {
    mysqlPool = null;
    const reason = error instanceof Error ? error.message : String(error);
    console.warn(`MySQL persistence unavailable (${reason}). Falling back to local storage.`);
  }
}

if (!mysqlPool) {
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

function cleanAlias(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 24);
}

function cleanFileName(value) {
  const fallback = "file";
  const cleaned = String(value ?? "")
    .trim()
    .replace(/[<>:"/\\|?*\u0000-\u001f]/g, "_")
    .slice(0, 80);

  return cleaned || fallback;
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
      dataUrl
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
    replyToMessageId,
    replyTo,
    reactions: deleted ? [] : reactions,
    deliveredBy,
    readBy
  };
}

function serializeStateForStorage() {
  return JSON.stringify({
    version: 1,
    users: [...users.values()].map((user) => ({
      key: user.key,
      name: user.name,
      password: user.password,
      avatarDataUrl: user.avatarDataUrl ?? null,
      friends: [...user.friends],
      incomingRequests: [...user.incomingRequests],
      outgoingRequests: [...user.outgoingRequests],
      friendAliases: [...user.friendAliases.entries()],
      hiddenChats: [...(user.hiddenChats ?? new Set())]
    })),
    sessions: [...sessions.entries()].map(([sessionId, userKey]) => ({
      sessionId,
      userKey
    })),
    chats: [...chats.values()].map((chat) => ({
      id: chat.id,
      type: chat.type,
      name: chat.name,
      participants: [...chat.participants],
      closeVotes: [...chat.closeVotes],
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

  if (mysqlPool) {
    mysqlPool
      .execute(
        `
          INSERT INTO app_state (\`key\`, value)
          VALUES ('state_json', ?)
          ON DUPLICATE KEY UPDATE value = VALUES(value)
        `,
        [serializedState]
      )
      .catch((error) => {
        console.error("Failed to persist state to MySQL:", error);
      });
    return;
  }

  if (!upsertStateStatement) {
    try {
      fs.writeFileSync(STATE_SNAPSHOT_PATH, serializedState, "utf8");
    } catch (error) {
      console.error("Failed to persist state to JSON snapshot:", error);
    }
    return;
  }

  try {
    upsertStateStatement.run(serializedState);
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

  if (mysqlPool) {
    let rows;

    try {
      [rows] = await mysqlPool.execute(
        `
          SELECT value
          FROM app_state
          WHERE \`key\` = 'state_json'
          LIMIT 1
        `
      );
    } catch (error) {
      console.error("Failed to read persisted state from MySQL:", error);
      return;
    }

    serializedState = String(rows?.[0]?.value ?? "");
  } else {
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
  }

  if (!serializedState) {
    return;
  }

  let parsedState;

  try {
    parsedState = JSON.parse(serializedState);
  } catch (error) {
    console.error("Persisted state is invalid JSON and was ignored:", error);
    return;
  }

  const rawUsers = Array.isArray(parsedState?.users) ? parsedState.users : [];

  for (const rawUser of rawUsers) {
    const userKey = normalizeUserName(rawUser?.key);
    const name = cleanDisplayName(rawUser?.name);
    const password = cleanPassword(rawUser?.password);

    if (!userKey || !name || password.length < 4) {
      continue;
    }

    users.set(userKey, {
      key: userKey,
      name,
      password,
      avatarDataUrl: String(rawUser?.avatarDataUrl ?? "").startsWith("data:image/")
        ? String(rawUser.avatarDataUrl)
        : null,
      friendAliases: new Map(),
      friends: new Set(),
      incomingRequests: new Set(),
      outgoingRequests: new Set(),
      hiddenChats: new Set()
    });
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

  const rawSessions = Array.isArray(parsedState?.sessions) ? parsedState.sessions : [];

  for (const rawSession of rawSessions) {
    const sessionId = String(rawSession?.sessionId ?? "").trim();
    const userKey = normalizeUserName(rawSession?.userKey);

    if (!sessionId || !userKey || !users.has(userKey)) {
      continue;
    }

    sessions.set(sessionId, userKey);

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
      updatedAt: Number(rawChat?.updatedAt || Date.now())
    });
  }
}

function createUser(userKey, displayName, password) {
  users.set(userKey, {
    key: userKey,
    name: displayName,
    password,
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
  if (!onlineUsers.has(userKey)) {
    onlineUsers.set(userKey, new Set());
  }

  onlineUsers.get(userKey).add(socket.id);
  socketToUser.set(socket.id, userKey);
}

function detachSocket(socketId) {
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

function serializeChat(chat, viewerKey) {
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
    updatedAt: chat.updatedAt
  };
}

function chatTitleForUser(chat, userKey) {
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

function createSessionForUser(userKey) {
  const sessionId = crypto.randomUUID();

  sessions.set(sessionId, userKey);

  if (!sessionsByUser.has(userKey)) {
    sessionsByUser.set(userKey, new Set());
  }

  sessionsByUser.get(userKey).add(sessionId);
  scheduleStatePersistence();
  return sessionId;
}

function invalidateSession(sessionId) {
  const userKey = sessions.get(sessionId);

  if (!userKey) {
    return;
  }

  sessions.delete(sessionId);

  const userSessions = sessionsByUser.get(userKey);

  if (userSessions) {
    userSessions.delete(sessionId);

    if (userSessions.size === 0) {
      sessionsByUser.delete(userKey);
    }
  }

  scheduleStatePersistence();
}

function invalidateSessionsForUser(userKey) {
  const userSessions = sessionsByUser.get(userKey);

  if (!userSessions) {
    return;
  }

  for (const sessionId of userSessions) {
    sessions.delete(sessionId);
  }

  sessionsByUser.delete(userKey);
  scheduleStatePersistence();
}

function createOrGetDirectChat(userA, userB, isTemp) {
  const chatId = buildDirectChatId(userA, userB, isTemp);

  if (chats.has(chatId)) {
    return { chat: chats.get(chatId), created: false };
  }

  const chat = {
    id: chatId,
    type: isTemp ? "temp" : "dm",
    name: null,
    participants: [userA, userB].sort(),
    messages: [],
    closeVotes: new Set(),
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
    attachments: []
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
      dataUrl
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

function completeAuth(socket, userKey, callback, existingSessionId = null) {
  authenticateSocket(socket, userKey);
  const deliveredChats = markUndeliveredMessagesAsDeliveredForUser(userKey);

  if (deliveredChats.length > 0) {
    scheduleStatePersistence();
  }

  const sessionId =
    existingSessionId && sessions.get(existingSessionId) === userKey
      ? existingSessionId
      : createSessionForUser(userKey);

  safeAck(callback, {
    ok: true,
    user: serializeAccount(userKey),
    sessionId
  });

  emitRelationshipStateForNetwork(userKey);
  for (const chat of deliveredChats) {
    emitChatState(chat);
  }
  emitChatSummaries(userKey);
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

    completeAuth(socket, userKey, callback, sessionId);
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
    completeAuth(socket, userKey, callback);
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

    if (user.password !== password) {
      safeAck(callback, {
        ok: false,
        error: "Incorrect password."
      });
      return;
    }

    completeAuth(socket, userKey, callback);
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

    if (user.password !== currentPassword) {
      safeAck(callback, { ok: false, error: "Current password is incorrect." });
      return;
    }

    if (nextPassword.length < 4) {
      safeAck(callback, { ok: false, error: "New password must be at least 4 characters." });
      return;
    }

    user.password = nextPassword;
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

    if (user.password !== password) {
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

    chat.messages.push({
      id: crypto.randomUUID(),
      senderKey: currentUser,
      senderName: displayNameFor(currentUser),
      text,
      sentAt,
      attachments,
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

    message.text = nextText;
    message.editedAt = new Date().toISOString();
    chat.updatedAt = Date.now();
    scheduleStatePersistence();

    emitChatState(chat);
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
    message.reactions = [];
    message.editedAt = null;
    message.deleted = true;
    message.deletedAt = new Date().toISOString();
    chat.updatedAt = Date.now();
    scheduleStatePersistence();

    emitChatState(chat);
    safeAck(callback, { ok: true });
  });

  socket.on("logout", (payload, callback) => {
    const sessionId = String(payload?.sessionId ?? "").trim();

    if (sessionId) {
      invalidateSession(sessionId);
    }

    const userKey = detachSocket(socket.id);

    if (userKey) {
      emitPresenceStateForFriends(userKey);
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

function shutdownAndExit(exitCode = 0) {
  persistStateToDatabase();

  if (!mysqlPool) {
    process.exit(exitCode);
    return;
  }

  mysqlPool
    .end()
    .catch((error) => {
      console.error("Failed to close MySQL pool:", error);
    })
    .finally(() => {
      process.exit(exitCode);
    });
}

process.on("SIGINT", () => {
  shutdownAndExit(0);
});

process.on("SIGTERM", () => {
  shutdownAndExit(0);
});

server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT} (persistence: ${persistenceBackend})`);
});
