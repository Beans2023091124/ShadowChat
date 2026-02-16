import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

import express from "express";
import { Server } from "socket.io";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = Number(process.env.PORT) || 3000;

const MAX_MESSAGE_LENGTH = 1200;
const MAX_ATTACHMENT_COUNT = 4;
const MAX_ATTACHMENT_SIZE = 5 * 1024 * 1024;
const MAX_DATA_URL_LENGTH = 7_000_000;
const MAX_AVATAR_DATA_URL_LENGTH = 2_500_000;
const DATA_DIR = process.env.SHADOW_CHAT_DATA_DIR
  ? path.resolve(process.env.SHADOW_CHAT_DATA_DIR)
  : path.join(__dirname, "data");
const DB_PATH = process.env.SHADOW_CHAT_DB_PATH
  ? path.resolve(process.env.SHADOW_CHAT_DB_PATH)
  : path.join(DATA_DIR, "shadow-chat.sqlite");

app.use(express.static(path.join(__dirname, "public")));

const users = new Map();
const onlineUsers = new Map();
const socketToUser = new Map();
const chats = new Map();
const sessions = new Map();
const sessionsByUser = new Map();
let persistenceScheduled = false;

fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

const db = new DatabaseSync(DB_PATH);
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  CREATE TABLE IF NOT EXISTS app_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
`);

const selectStateStatement = db.prepare(`
  SELECT value
  FROM app_state
  WHERE key = 'state_json'
`);

const upsertStateStatement = db.prepare(`
  INSERT INTO app_state (key, value)
  VALUES ('state_json', ?)
  ON CONFLICT(key) DO UPDATE SET value = excluded.value
`);

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

  return {
    id: String(rawMessage?.id ?? crypto.randomUUID()),
    senderKey: senderIsSystem ? "system" : senderKey,
    senderName: senderIsSystem ? "System" : displayNameFor(senderKey),
    text: deleted ? "" : String(rawMessage?.text ?? ""),
    kind: rawMessage?.kind ? String(rawMessage.kind).slice(0, 40) : null,
    sentAt: isValidIsoDate(rawMessage?.sentAt)
      ? String(rawMessage.sentAt)
      : new Date().toISOString(),
    editedAt: isValidIsoDate(rawMessage?.editedAt) ? String(rawMessage.editedAt) : null,
    deleted,
    deletedAt: deleted && isValidIsoDate(rawMessage?.deletedAt)
      ? String(rawMessage.deletedAt)
      : deleted
        ? new Date().toISOString()
        : null,
    attachments: deleted ? [] : attachments
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
      friendAliases: [...user.friendAliases.entries()]
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
        attachments: Array.isArray(message.attachments) ? message.attachments : []
      }))
    }))
  });
}

function persistStateToDatabase() {
  try {
    upsertStateStatement.run(serializeStateForStorage());
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

function restoreStateFromDatabase() {
  let rawState = null;

  try {
    rawState = selectStateStatement.get();
  } catch (error) {
    console.error("Failed to read persisted state from SQLite:", error);
    return;
  }

  if (!rawState?.value) {
    return;
  }

  let parsedState;

  try {
    parsedState = JSON.parse(String(rawState.value));
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
      outgoingRequests: new Set()
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
    outgoingRequests: new Set()
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
    attachments: Array.isArray(message.attachments) ? message.attachments : []
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
  return [...chats.values()]
    .filter((chat) => chat.participants.includes(userKey))
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

  const chat = {
    id: `group:${crypto.randomUUID()}`,
    type: "group",
    name: groupName,
    participants: [ownerKey, ...sortUserKeys(uniqueMembers)],
    messages: [],
    closeVotes: new Set(),
    updatedAt: Date.now()
  };

  chats.set(chat.id, chat);
  scheduleStatePersistence();
  return chat;
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

function completeAuth(socket, userKey, callback, existingSessionId = null) {
  authenticateSocket(socket, userKey);
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

restoreStateFromDatabase();

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
    const displayName = cleanDisplayName(payload?.username);
    const userKey = normalizeUserName(displayName);
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

      if (normalizeUserName(account.name) === normalizedNextName) {
        safeAck(callback, { ok: false, error: "Display name is already in use." });
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

    for (const participant of chat.participants) {
      emitChatSummaries(participant);
    }

    for (const participant of chat.participants) {
      if (participant === currentUser) {
        continue;
      }

      emitToUser(participant, "group_created", {
        from: displayNameFor(currentUser),
        groupName: chat.name,
        chatId: chat.id
      });
    }
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

    safeAck(callback, { ok: true, chat: serializeChat(chat, currentUser) });
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

    if (chat.type === "temp" && chat.closeVotes.size > 0) {
      chat.closeVotes.clear();
      appendSystemMessage(
        chat,
        "Temp close request canceled because a new message was sent.",
        "temp_close_reset"
      );
    }

    chat.messages.push({
      id: crypto.randomUUID(),
      senderKey: currentUser,
      senderName: displayNameFor(currentUser),
      text,
      sentAt: new Date().toISOString(),
      attachments
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

    if (!offer || typeof offer !== "object") {
      safeAck(callback, { ok: false, error: "Invalid call offer." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "incoming_call", {
      chatId,
      mode,
      offer,
      fromKey: currentUser,
      fromName: displayNameFor(currentUser)
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

    if (!answer || typeof answer !== "object") {
      safeAck(callback, { ok: false, error: "Invalid call answer." });
      return;
    }

    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_answer", {
      chatId,
      answer,
      fromKey: currentUser
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

  socket.on("end_call", (payload, callback) => {
    const currentUser = getCurrentUser(socket, callback);

    if (!currentUser) {
      return;
    }

    const chatId = String(payload?.chatId ?? "");
    const callChatResult = getCallChat(chatId, currentUser);

    if (callChatResult.error) {
      safeAck(callback, { ok: false, error: callChatResult.error });
      return;
    }

    relayToOtherParticipants(callChatResult.chat, currentUser, "call_ended", {
      chatId,
      fromKey: currentUser
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
  console.log(`Server listening on http://localhost:${PORT}`);
});
