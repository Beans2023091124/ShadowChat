const socket = io();
const SESSION_STORAGE_KEY = "shadow_chat_session_id";
const NOTIFICATION_PREF_KEY = "shadow_chat_notifications_enabled";
const LAST_OPEN_CHAT_STORAGE_PREFIX = "shadow_chat_last_open_chat_";
const CHAT_DRAFT_STORAGE_PREFIX = "shadow_chat_draft_";
const ENTER_TO_SEND_PREF_KEY = "shadow_chat_enter_to_send";

const state = {
  currentUser: null,
  sessionId: null,
  authMode: "login",
  relationships: {
    friends: [],
    incomingRequests: [],
    outgoingRequests: []
  },
  chatSummaries: [],
  messageSearchQuery: "",
  activeChat: null,
  profileUserKey: null,
  replyingTo: null,
  typingByChatId: {},
  pendingAttachments: [],
  composerSending: false,
  unreadByChatId: {},
  unreadAnchorByChatId: {},
  activeUnreadAnchorMessageId: null,
  groupCreateSelectedKeys: new Set(),
  groupCreateMode: "direct",
  groupCreateBaseUserKey: null,
  groupCreateTargetChatId: null,
  sidebarOpen: false,
  pendingIncomingCall: null,
  call: null,
  settingsAvatarDataUrl: null,
  settingsSessions: [],
  settingsSessionsLoading: false,
  isResumingSession: false,
  notificationsEnabled: localStorage.getItem(NOTIFICATION_PREF_KEY) !== "off",
  enterToSendEnabled: localStorage.getItem(ENTER_TO_SEND_PREF_KEY) !== "off",
  pendingLastOpenChatId: null,
  hasAttemptedLastChatRestore: false,
  stickerTab: "emoji",
  emojiCatalog: [],
  emojiCatalogLoaded: false,
  gifResults: [],
  voiceNoteRecorder: null,
  voiceNoteStream: null,
  voiceNoteChunks: [],
  voiceNoteMimeType: "",
  voiceNoteChatId: null,
  voiceNoteStartedAt: 0,
  voiceNoteFinalizing: false
};

const elements = {
  sidebar: document.getElementById("sidebar"),
  sidebarBackdrop: document.getElementById("sidebarBackdrop"),
  mobileSidebarButton: document.getElementById("mobileSidebarButton"),

  authOverlay: document.getElementById("authOverlay"),
  loginTabButton: document.getElementById("loginTabButton"),
  signupTabButton: document.getElementById("signupTabButton"),
  authForm: document.getElementById("authForm"),
  authUsernameInput: document.getElementById("authUsernameInput"),
  authPasswordInput: document.getElementById("authPasswordInput"),
  authPasswordToggleButton: document.getElementById("authPasswordToggleButton"),
  authConfirmRow: document.getElementById("authConfirmRow"),
  authConfirmInput: document.getElementById("authConfirmInput"),
  authConfirmToggleButton: document.getElementById("authConfirmToggleButton"),
  authSubmitButton: document.getElementById("authSubmitButton"),
  authSwitchText: document.getElementById("authSwitchText"),

  friendForm: document.getElementById("friendForm"),
  friendInput: document.getElementById("friendInput"),
  incomingRequests: document.getElementById("incomingRequests"),
  outgoingRequests: document.getElementById("outgoingRequests"),
  friendsList: document.getElementById("friendsList"),
  friendCount: document.getElementById("friendCount"),

  chatList: document.getElementById("chatList"),
  toggleGroupPanelButton: document.getElementById("toggleGroupPanelButton"),
  groupCreateModal: document.getElementById("groupCreateModal"),
  groupCreateTitle: document.getElementById("groupCreateTitle"),
  closeGroupCreateModalButton: document.getElementById("closeGroupCreateModalButton"),
  groupCreateHint: document.getElementById("groupCreateHint"),
  groupCreateSearchInput: document.getElementById("groupCreateSearchInput"),
  groupCreateEmpty: document.getElementById("groupCreateEmpty"),
  groupCreateFriendList: document.getElementById("groupCreateFriendList"),
  createGroupFromDirectButton: document.getElementById("createGroupFromDirectButton"),

  selfAvatar: document.getElementById("selfAvatar"),
  selfName: document.getElementById("selfName"),
  settingsButton: document.getElementById("settingsButton"),

  chatPanel: document.getElementById("chatPanel"),
  chatTitle: document.getElementById("chatTitle"),
  chatSubtitle: document.getElementById("chatSubtitle"),
  messageSearchInput: document.getElementById("messageSearchInput"),
  voiceCallButton: document.getElementById("voiceCallButton"),
  videoCallButton: document.getElementById("videoCallButton"),
  chatPrefsButton: document.getElementById("chatPrefsButton"),
  openTempChatButton: document.getElementById("openTempChatButton"),
  closeTempChatButton: document.getElementById("closeTempChatButton"),
  messages: document.getElementById("messages"),
  jumpToUnreadButton: document.getElementById("jumpToUnreadButton"),
  typingIndicator: document.getElementById("typingIndicator"),
  attachmentPreview: document.getElementById("attachmentPreview"),
  replyPreview: document.getElementById("replyPreview"),
  composerForm: document.getElementById("composerForm"),
  attachButton: document.getElementById("attachButton"),
  attachMenu: document.getElementById("attachMenu"),
  attachCameraButton: document.getElementById("attachCameraButton"),
  attachPhotoButton: document.getElementById("attachPhotoButton"),
  attachUploadButton: document.getElementById("attachUploadButton"),
  attachVoiceNoteButton: document.getElementById("attachVoiceNoteButton"),
  stickerButton: document.getElementById("stickerButton"),
  fileInput: document.getElementById("fileInput"),
  photoInput: document.getElementById("photoInput"),
  cameraInput: document.getElementById("cameraInput"),
  messageInput: document.getElementById("messageInput"),
  sendButton: document.getElementById("sendButton"),
  stickerMenu: document.getElementById("stickerMenu"),
  emojiTabButton: document.getElementById("emojiTabButton"),
  gifTabButton: document.getElementById("gifTabButton"),
  emojiTabPanel: document.getElementById("emojiTabPanel"),
  gifTabPanel: document.getElementById("gifTabPanel"),
  emojiSearchInput: document.getElementById("emojiSearchInput"),
  stickerEmojiGrid: document.getElementById("stickerEmojiGrid"),
  gifSearchForm: document.getElementById("gifSearchForm"),
  gifSearchInput: document.getElementById("gifSearchInput"),
  gifSearchButton: document.getElementById("gifSearchButton"),
  gifResults: document.getElementById("gifResults"),
  closeStickerMenuButton: document.getElementById("closeStickerMenuButton"),

  profileEmpty: document.getElementById("profileEmpty"),
  profileContent: document.getElementById("profileContent"),
  profileAvatar: document.getElementById("profileAvatar"),
  profileName: document.getElementById("profileName"),
  profileState: document.getElementById("profileState"),
  profileMeta: document.getElementById("profileMeta"),
  profileGroupContent: document.getElementById("profileGroupContent"),
  profileGroupTitle: document.getElementById("profileGroupTitle"),
  profileGroupMeta: document.getElementById("profileGroupMeta"),
  profileGroupList: document.getElementById("profileGroupList"),

  settingsModal: document.getElementById("settingsModal"),
  closeSettingsButton: document.getElementById("closeSettingsButton"),
  settingsAvatarPreview: document.getElementById("settingsAvatarPreview"),
  settingsAvatarInput: document.getElementById("settingsAvatarInput"),
  settingsDisplayNameInput: document.getElementById("settingsDisplayNameInput"),
  saveProfileButton: document.getElementById("saveProfileButton"),
  settingsCurrentPasswordInput: document.getElementById("settingsCurrentPasswordInput"),
  settingsNewPasswordInput: document.getElementById("settingsNewPasswordInput"),
  changePasswordButton: document.getElementById("changePasswordButton"),
  revokeOtherSessionsButton: document.getElementById("revokeOtherSessionsButton"),
  settingsSessionsList: document.getElementById("settingsSessionsList"),
  settingsSessionsEmpty: document.getElementById("settingsSessionsEmpty"),
  settingsDeletePasswordInput: document.getElementById("settingsDeletePasswordInput"),
  deleteAccountButton: document.getElementById("deleteAccountButton"),
  enableNotificationsButton: document.getElementById("enableNotificationsButton"),
  toggleEnterToSendButton: document.getElementById("toggleEnterToSendButton"),
  logoutButton: document.getElementById("logoutButton"),

  inlinePromptModal: document.getElementById("inlinePromptModal"),
  inlinePromptTitle: document.getElementById("inlinePromptTitle"),
  inlinePromptDescription: document.getElementById("inlinePromptDescription"),
  inlinePromptForm: document.getElementById("inlinePromptForm"),
  inlinePromptInput: document.getElementById("inlinePromptInput"),
  inlinePromptSubmitButton: document.getElementById("inlinePromptSubmitButton"),
  inlinePromptCancelButton: document.getElementById("inlinePromptCancelButton"),

  incomingCallPrompt: document.getElementById("incomingCallPrompt"),
  incomingCallText: document.getElementById("incomingCallText"),
  incomingCallRings: document.getElementById("incomingCallRings"),
  acceptCallButton: document.getElementById("acceptCallButton"),
  declineCallButton: document.getElementById("declineCallButton"),

  callOverlay: document.getElementById("callOverlay"),
  callTitle: document.getElementById("callTitle"),
  callStatus: document.getElementById("callStatus"),
  callDuration: document.getElementById("callDuration"),
  callQualityBadge: document.getElementById("callQualityBadge"),
  remoteMediaStatus: document.getElementById("remoteMediaStatus"),
  remoteMicIndicator: document.getElementById("remoteMicIndicator"),
  remoteCameraIndicator: document.getElementById("remoteCameraIndicator"),
  callModeBadge: document.getElementById("callModeBadge"),
  callMain: document.getElementById("callMain"),
  callStage: document.getElementById("callStage"),
  voiceCallPanel: document.getElementById("voiceCallPanel"),
  voiceCallRemoteUser: document.getElementById("voiceCallRemoteUser"),
  voiceCallSelfUser: document.getElementById("voiceCallSelfUser"),
  voiceCallAvatar: document.getElementById("voiceCallAvatar"),
  voiceCallName: document.getElementById("voiceCallName"),
  voiceCallSelfAvatar: document.getElementById("voiceCallSelfAvatar"),
  voiceCallSelfName: document.getElementById("voiceCallSelfName"),
  callVideos: document.getElementById("callVideos"),
  remoteVideo: document.getElementById("remoteVideo"),
  remoteCameraVideo: document.getElementById("remoteCameraVideo"),
  remoteVideoPlaceholder: document.getElementById("remoteVideoPlaceholder"),
  remoteVideoPlaceholderText: document.getElementById("remoteVideoPlaceholderText"),
  callAnnotationCanvas: document.getElementById("callAnnotationCanvas"),
  localVideo: document.getElementById("localVideo"),
  callChatPanel: document.getElementById("callChatPanel"),
  callChatMessages: document.getElementById("callChatMessages"),
  callChatForm: document.getElementById("callChatForm"),
  callChatInput: document.getElementById("callChatInput"),
  callChatSendButton: document.getElementById("callChatSendButton"),
  toggleMicButton: document.getElementById("toggleMicButton"),
  toggleCameraButton: document.getElementById("toggleCameraButton"),
  toggleScreenShareButton: document.getElementById("toggleScreenShareButton"),
  toggleDrawButton: document.getElementById("toggleDrawButton"),
  clearDrawButton: document.getElementById("clearDrawButton"),
  toggleCallChatButton: document.getElementById("toggleCallChatButton"),
  endCallButton: document.getElementById("endCallButton"),

  toast: document.getElementById("toast")
};

const contextMenuElement = document.createElement("div");
contextMenuElement.id = "contextMenu";
contextMenuElement.className = "context-menu hidden";
document.body.append(contextMenuElement);

const timeFormatter = new Intl.DateTimeFormat([], {
  hour: "2-digit",
  minute: "2-digit"
});

const dateTimeFormatter = new Intl.DateTimeFormat([], {
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit"
});

const MAX_RING_COUNT = 5;
const RING_INTERVAL_MS = 4000;
const TYPING_PULSE_MS = 1800;
const TYPING_REMOTE_TTL_MS = 4200;
const VOICE_NOTE_MAX_DURATION_MS = 120000;
const VOICE_NOTE_MAX_SIZE_BYTES = 5 * 1024 * 1024;
const CALL_QUALITY_POLL_MS = 2800;
const CALL_RECONNECT_GRACE_MS = 12000;
const VOICE_ACTIVITY_POLL_MS = 120;
const VOICE_ACTIVITY_THRESHOLD = 0.04;
const VOICE_ACTIVITY_HOLD_MS = 260;
const CALL_CHAT_MESSAGE_LIMIT = 80;
const STICKER_RESULT_RENDER_LIMIT = 5000;
const CALL_ANNOTATION_DEFAULT_COLOR = "#a8a8a8";
const CALL_ANNOTATION_DEFAULT_WIDTH = 3;
const MAX_GROUP_CREATE_EXTRA_MEMBERS = 8;
const EMOJI_DATASET_URL = "https://cdn.jsdelivr.net/npm/emojibase-data/en/compact.json";
const TENOR_API_ENDPOINT = "https://tenor.googleapis.com/v2/search";
const TENOR_API_KEY =
  window.SHADOW_CHAT_TENOR_KEY || "AIzaSyBOti4mM-6x9WDnZIjIeyEU21OpBXqWBgw";
const TENOR_CLIENT_KEY = "shadow_chat";
const TENOR_RESULT_LIMIT = 18;
const LOCAL_VIDEO_CORNERS = [
  "top-left",
  "top-right",
  "bottom-left",
  "bottom-right"
];
const QUICK_REACTIONS = [
  "\u{1F44D}",
  "\u2764\uFE0F",
  "\u{1F602}",
  "\u{1F62E}",
  "\u{1F622}",
  "\u{1F525}"
];
const CHAT_THEME_OPTIONS = ["default", "slate", "forest", "sunset", "night"];

let toastTimeout;
let outgoingRingTimer;
let incomingRingTimer;
let voiceNoteDurationTimer;
let callDurationTimer;
let callQualityTimer;
let callReconnectTimer;
let callVoiceActivityTimer;
let localTypingPulseTimer;
let localTypingChatId = null;
let localTypingActive = false;
let inlinePromptResolve;
let preferredLocalVideoCorner = "top-right";
let localVideoDragState;
let remoteVideoDragState;
let callAnnotationPointerState = null;
let emojiLoadPromise;
let gifSearchAbortController;
let composerDragDepth = 0;
let activeComposerDraftChatId = null;

function initials(value) {
  const compact = String(value ?? "").trim();

  if (!compact) {
    return "?";
  }

  const parts = compact.split(/\s+/).filter(Boolean);

  if (parts.length === 1) {
    return parts[0].slice(0, 2).toUpperCase();
  }

  return `${parts[0][0]}${parts[1][0]}`.toUpperCase();
}

function setAvatar(element, user) {
  if (!element) {
    return;
  }

  const name = String(user?.name ?? "").trim();
  const avatarDataUrl = String(user?.avatarDataUrl ?? "").trim();

  if (avatarDataUrl.startsWith("data:image/")) {
    element.textContent = "";
    element.style.backgroundImage = `url("${avatarDataUrl.replace(/"/g, "%22")}")`;
    element.classList.add("has-image");
    return;
  }

  element.style.backgroundImage = "";
  element.classList.remove("has-image");
  element.textContent = initials(name || "?");
}

function clearOutgoingRingTimer() {
  clearInterval(outgoingRingTimer);
  outgoingRingTimer = undefined;
}

function clearIncomingRingTimer() {
  clearInterval(incomingRingTimer);
  incomingRingTimer = undefined;
}

function clearVoiceNoteDurationTimer() {
  clearInterval(voiceNoteDurationTimer);
  voiceNoteDurationTimer = undefined;
}

function clearCallDurationTimer() {
  clearInterval(callDurationTimer);
  callDurationTimer = undefined;
}

function clearCallQualityTimer() {
  clearInterval(callQualityTimer);
  callQualityTimer = undefined;
}

function clearCallReconnectTimer() {
  clearTimeout(callReconnectTimer);
  callReconnectTimer = undefined;
}

function clearCallVoiceActivityTimer() {
  clearInterval(callVoiceActivityTimer);
  callVoiceActivityTimer = undefined;
}

function syncCallFocusState() {
  const callVisible = Boolean(state.call || state.pendingIncomingCall);
  document.body.classList.toggle("call-active", callVisible);
}

function showToast(message) {
  elements.toast.textContent = message;
  elements.toast.classList.remove("hidden");
  clearTimeout(toastTimeout);
  toastTimeout = window.setTimeout(() => {
    elements.toast.classList.add("hidden");
  }, 2600);
}

function setSessionId(sessionId) {
  const value = String(sessionId ?? "").trim();

  if (!value) {
    state.sessionId = null;
    localStorage.removeItem(SESSION_STORAGE_KEY);
    return;
  }

  state.sessionId = value;
  localStorage.setItem(SESSION_STORAGE_KEY, value);
}

function currentDeviceName() {
  const userAgentData = navigator.userAgentData;

  if (userAgentData && typeof userAgentData.platform === "string") {
    const platform = userAgentData.platform.trim();

    if (platform) {
      return `${platform} ${userAgentData.mobile ? "Mobile" : "Desktop"}`.slice(0, 64);
    }
  }

  const platform = String(navigator.platform ?? "").trim();

  if (platform) {
    return platform.slice(0, 64);
  }

  return "Browser";
}

function lastOpenChatStorageKey() {
  const userKey = String(state.currentUser?.key ?? "").trim();
  return userKey ? `${LAST_OPEN_CHAT_STORAGE_PREFIX}${userKey}` : "";
}

function readStoredLastOpenChatId() {
  const key = lastOpenChatStorageKey();

  if (!key) {
    return null;
  }

  const value = String(localStorage.getItem(key) ?? "").trim();
  return value || null;
}

function storeLastOpenChatId(chatId) {
  const key = lastOpenChatStorageKey();
  const normalizedChatId = String(chatId ?? "").trim();

  if (!key || !normalizedChatId) {
    return;
  }

  localStorage.setItem(key, normalizedChatId);
  state.pendingLastOpenChatId = normalizedChatId;
}

function clearStoredLastOpenChatIdIfMatches(chatId) {
  const key = lastOpenChatStorageKey();
  const normalizedChatId = String(chatId ?? "").trim();

  if (!key || !normalizedChatId) {
    return;
  }

  const current = String(localStorage.getItem(key) ?? "").trim();

  if (current === normalizedChatId) {
    localStorage.removeItem(key);
    if (state.pendingLastOpenChatId === normalizedChatId) {
      state.pendingLastOpenChatId = null;
    }
  }
}

function draftStorageKey(chatId) {
  const userKey = String(state.currentUser?.key ?? "").trim();
  const normalizedChatId = String(chatId ?? "").trim();

  if (!userKey || !normalizedChatId) {
    return "";
  }

  return `${CHAT_DRAFT_STORAGE_PREFIX}${userKey}_${normalizedChatId}`;
}

function readStoredDraft(chatId) {
  const key = draftStorageKey(chatId);

  if (!key) {
    return "";
  }

  return String(localStorage.getItem(key) ?? "").slice(0, 1200);
}

function storeDraft(chatId, text) {
  const key = draftStorageKey(chatId);

  if (!key) {
    return;
  }

  const value = String(text ?? "").slice(0, 1200);

  if (!value) {
    localStorage.removeItem(key);
    return;
  }

  localStorage.setItem(key, value);
}

function clearStoredDraft(chatId) {
  const key = draftStorageKey(chatId);

  if (!key) {
    return;
  }

  localStorage.removeItem(key);
}

function storeActiveChatDraft() {
  const activeChatId = String(state.activeChat?.id ?? "").trim();

  if (!activeChatId) {
    return;
  }

  storeDraft(activeChatId, elements.messageInput.value);
}

function restoreDraftForActiveChat() {
  const activeChatId = String(state.activeChat?.id ?? "").trim();

  if (!activeChatId) {
    elements.messageInput.value = "";
    resizeComposerInput();
    return;
  }

  const draft = readStoredDraft(activeChatId);

  if (elements.messageInput.value !== draft) {
    elements.messageInput.value = draft;
  }

  resizeComposerInput();
}

function normalizeChatThemeValue(value) {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  return CHAT_THEME_OPTIONS.includes(normalized) ? normalized : "default";
}

function sanitizeChatWallpaper(value) {
  const raw = String(value ?? "").trim();

  if (!raw) {
    return "";
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

  return "";
}

function activeChatViewerPrefs() {
  const fallback = {
    nickname: "",
    theme: "default",
    wallpaper: ""
  };

  if (!state.activeChat || !state.activeChat.viewerPrefs || typeof state.activeChat.viewerPrefs !== "object") {
    return fallback;
  }

  const nickname = String(state.activeChat.viewerPrefs.nickname ?? "").trim();
  const theme = normalizeChatThemeValue(state.activeChat.viewerPrefs.theme);
  const wallpaper = sanitizeChatWallpaper(state.activeChat.viewerPrefs.wallpaper);

  return {
    nickname,
    theme,
    wallpaper
  };
}

function applyActiveChatAppearance() {
  if (!elements.chatPanel || !elements.messages) {
    return;
  }

  for (const theme of CHAT_THEME_OPTIONS) {
    if (theme === "default") {
      continue;
    }

    elements.chatPanel.classList.remove(`theme-${theme}`);
  }

  elements.messages.classList.remove("has-wallpaper");
  elements.messages.style.backgroundImage = "";
  elements.messages.style.backgroundSize = "";
  elements.messages.style.backgroundPosition = "";
  elements.messages.style.backgroundRepeat = "";

  if (!state.activeChat) {
    return;
  }

  const prefs = activeChatViewerPrefs();

  if (prefs.theme !== "default") {
    elements.chatPanel.classList.add(`theme-${prefs.theme}`);
  }

  if (prefs.wallpaper) {
    const escapedWallpaper = prefs.wallpaper.replace(/"/g, "%22");
    elements.messages.classList.add("has-wallpaper");
    elements.messages.style.backgroundImage = `linear-gradient(rgba(14, 14, 16, 0.76), rgba(14, 14, 16, 0.76)), url("${escapedWallpaper}")`;
    elements.messages.style.backgroundSize = "cover";
    elements.messages.style.backgroundPosition = "center";
    elements.messages.style.backgroundRepeat = "no-repeat";
  }
}

function completeAuth(response) {
  state.isResumingSession = false;
  state.currentUser = response.user;
  setSessionId(response.sessionId ?? state.sessionId);
  state.pendingLastOpenChatId = readStoredLastOpenChatId();
  state.hasAttemptedLastChatRestore = false;
  elements.authOverlay.classList.add("hidden");

  elements.authUsernameInput.value = "";
  elements.authPasswordInput.value = "";
  elements.authConfirmInput.value = "";
  setAuthPasswordVisibility(false);
  setAuthConfirmPasswordVisibility(false);

  updateSelfStrip();
  renderRelationshipState();
  renderChatList();
  renderActiveChat();
}

function clearClientSessionState() {
  state.isResumingSession = false;
  state.currentUser = null;
  state.messageSearchQuery = "";
  state.activeChat = null;
  state.profileUserKey = null;
  state.replyingTo = null;
  state.typingByChatId = {};
  state.relationships = {
    friends: [],
    incomingRequests: [],
    outgoingRequests: []
  };
  state.chatSummaries = [];
  state.unreadByChatId = {};
  state.unreadAnchorByChatId = {};
  state.activeUnreadAnchorMessageId = null;
  state.groupCreateMode = "direct";
  state.groupCreateSelectedKeys = new Set();
  state.groupCreateBaseUserKey = null;
  state.groupCreateTargetChatId = null;
  state.settingsAvatarDataUrl = null;
  state.settingsSessions = [];
  state.settingsSessionsLoading = false;
  state.pendingLastOpenChatId = null;
  state.hasAttemptedLastChatRestore = false;
  state.stickerTab = "emoji";
  state.gifResults = [];
  state.composerSending = false;
  if (state.voiceNoteRecorder) {
    stopVoiceNoteRecording(false);
  }
  clearVoiceNoteDurationTimer();
  state.voiceNoteStream = null;
  state.voiceNoteChunks = [];
  state.voiceNoteMimeType = "";
  state.voiceNoteChatId = null;
  state.voiceNoteStartedAt = 0;
  state.voiceNoteFinalizing = false;
  stopLocalTyping();
  hideGroupQuickPopover();
  closeInlinePrompt(null);
  closeAttachMenu();
  closeStickerMenu();
  elements.messageSearchInput.value = "";
  elements.messageSearchInput.disabled = true;
  elements.emojiSearchInput.value = "";
  elements.gifSearchInput.value = "";
  elements.gifResults.innerHTML = "";
  elements.stickerEmojiGrid.innerHTML = "";
  renderReplyPreview();
  renderTypingIndicator();

  elements.authOverlay.classList.remove("hidden");
  updateSelfStrip();
  renderRelationshipState();
  renderChatList();
  renderActiveChat();
  renderSettingsSessions();
  updateVoiceNoteButton();
}

function tryResumeSession() {
  if (state.isResumingSession) {
    return;
  }

  const sessionId = localStorage.getItem(SESSION_STORAGE_KEY);

  if (!sessionId) {
    return;
  }

  state.isResumingSession = true;
  socket.emit("resume_session", { sessionId, deviceName: currentDeviceName() }, (response) => {
    state.isResumingSession = false;

    if (!response?.ok) {
      setSessionId(null);
      clearClientSessionState();
      return;
    }

    completeAuth(response);
  });
}

function hideContextMenu() {
  contextMenuElement.classList.add("hidden");
  contextMenuElement.innerHTML = "";
}

function showContextMenu(x, y, items) {
  if (!Array.isArray(items) || items.length === 0) {
    hideContextMenu();
    return;
  }

  contextMenuElement.innerHTML = "";

  for (const item of items) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `context-menu-item${item.danger ? " danger" : ""}`;
    button.textContent = item.label;
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      hideContextMenu();
      item.onSelect();
    });
    contextMenuElement.append(button);
  }

  contextMenuElement.classList.remove("hidden");

  requestAnimationFrame(() => {
    const width = contextMenuElement.offsetWidth;
    const height = contextMenuElement.offsetHeight;
    const left = Math.min(Math.max(8, x), window.innerWidth - width - 8);
    const top = Math.min(Math.max(8, y), window.innerHeight - height - 8);

    contextMenuElement.style.left = `${left}px`;
    contextMenuElement.style.top = `${top}px`;
  });
}

function closeAttachMenu() {
  elements.attachMenu.classList.add("hidden");
}

function toggleAttachMenu() {
  if (!state.currentUser || !state.activeChat || elements.attachButton.disabled) {
    return;
  }

  syncAttachMenuButtonState();
  const isHidden = elements.attachMenu.classList.contains("hidden");

  if (!isHidden) {
    closeAttachMenu();
    return;
  }

  closeStickerMenu();
  elements.attachMenu.classList.remove("hidden");
}

function createStickerEmptyState(message) {
  const empty = document.createElement("p");
  empty.className = "sticker-empty";
  empty.textContent = message;
  return empty;
}

function setStickerTab(tab) {
  const normalized = tab === "gif" ? "gif" : "emoji";
  state.stickerTab = normalized;
  const emojiTabActive = normalized === "emoji";

  elements.emojiTabButton.classList.toggle("active", emojiTabActive);
  elements.gifTabButton.classList.toggle("active", !emojiTabActive);
  elements.emojiTabButton.setAttribute("aria-selected", emojiTabActive ? "true" : "false");
  elements.gifTabButton.setAttribute("aria-selected", emojiTabActive ? "false" : "true");
  elements.emojiTabPanel.classList.toggle("hidden", !emojiTabActive);
  elements.gifTabPanel.classList.toggle("hidden", emojiTabActive);
}

async function ensureEmojiCatalogLoaded() {
  if (state.emojiCatalogLoaded) {
    return true;
  }

  if (emojiLoadPromise) {
    await emojiLoadPromise;
    return state.emojiCatalogLoaded;
  }

  emojiLoadPromise = fetch(EMOJI_DATASET_URL)
    .then(async (response) => {
      if (!response.ok) {
        throw new Error("Could not load emoji catalog.");
      }

      const payload = await response.json();

      if (!Array.isArray(payload)) {
        throw new Error("Emoji catalog response is invalid.");
      }

      const dedupe = new Set();
      const catalog = [];

      for (const entry of payload) {
        const emoji = String(entry?.unicode ?? "").trim();

        if (!emoji || dedupe.has(emoji)) {
          continue;
        }

        dedupe.add(emoji);

        const label = String(entry?.label ?? "").trim();
        const tags = Array.isArray(entry?.tags) ? entry.tags.join(" ") : "";
        const shortcodes = Array.isArray(entry?.shortcodes) ? entry.shortcodes.join(" ") : "";
        const searchIndex = `${label} ${tags} ${shortcodes}`.trim().toLowerCase();

        catalog.push({
          emoji,
          label: label || emoji,
          searchIndex
        });
      }

      state.emojiCatalog = catalog;
      state.emojiCatalogLoaded = catalog.length > 0;
    })
    .catch(() => {
      state.emojiCatalog = QUICK_REACTIONS.map((emoji) => ({
        emoji,
        label: emoji,
        searchIndex: emoji
      }));
      state.emojiCatalogLoaded = state.emojiCatalog.length > 0;
      showToast("Emoji catalog could not fully load. Showing quick reactions.");
    })
    .finally(() => {
      emojiLoadPromise = undefined;
    });

  await emojiLoadPromise;
  return state.emojiCatalogLoaded;
}

function renderEmojiPickerResults() {
  const container = elements.stickerEmojiGrid;
  container.innerHTML = "";

  if (!state.emojiCatalogLoaded) {
    container.append(createStickerEmptyState("Loading emojis..."));
    return;
  }

  const query = String(elements.emojiSearchInput.value ?? "").trim().toLowerCase();
  const source = query
    ? state.emojiCatalog.filter((entry) => entry.searchIndex.includes(query))
    : state.emojiCatalog;
  const visible = source.slice(0, STICKER_RESULT_RENDER_LIMIT);

  if (visible.length === 0) {
    container.append(createStickerEmptyState("No emojis found."));
    return;
  }

  const fragment = document.createDocumentFragment();

  for (const entry of visible) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "sticker-emoji";
    button.setAttribute("data-emoji", entry.emoji);
    button.setAttribute("title", entry.label);
    button.textContent = entry.emoji;
    fragment.append(button);
  }

  container.append(fragment);
}

function extractTenorGifData(item) {
  const media = item?.media_formats ?? {};
  const gifUrl =
    String(
      media.tinygif?.url ??
        media.nanogif?.url ??
        media.gif?.url ??
        media.mediumgif?.url ??
        ""
    ).trim() || null;
  const previewUrl =
    String(
      media.tinygifpreview?.url ??
        media.gifpreview?.url ??
        media.tinygif?.url ??
        media.nanogif?.url ??
        gifUrl ??
        ""
    ).trim() || null;

  if (!gifUrl || !previewUrl) {
    return null;
  }

  const description = String(item?.content_description ?? item?.title ?? "GIF").trim() || "GIF";
  return { gifUrl, previewUrl, description };
}

function renderGifResults() {
  const container = elements.gifResults;
  container.innerHTML = "";

  if (state.gifResults.length === 0) {
    const query = String(elements.gifSearchInput.value ?? "").trim();
    container.append(
      createStickerEmptyState(query ? "No GIFs found for that search." : "Search for GIFs on Tenor.")
    );
    return;
  }

  const fragment = document.createDocumentFragment();

  for (const result of state.gifResults) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "gif-result";
    button.setAttribute("data-gif-url", result.gifUrl);
    button.setAttribute("title", result.description);

    const image = document.createElement("img");
    image.className = "gif-result-image";
    image.src = result.previewUrl;
    image.alt = result.description;
    image.loading = "lazy";
    image.decoding = "async";

    button.append(image);
    fragment.append(button);
  }

  container.append(fragment);
}

function renderGifResultsLoading() {
  elements.gifResults.innerHTML = "";
  elements.gifResults.append(createStickerEmptyState("Searching GIFs..."));
}

async function searchTenorGifs(query) {
  const normalizedQuery = String(query ?? "").trim();

  if (!normalizedQuery) {
    state.gifResults = [];
    renderGifResults();
    return;
  }

  if (!TENOR_API_KEY) {
    showToast("Tenor API key is missing.");
    return;
  }

  if (gifSearchAbortController) {
    gifSearchAbortController.abort();
  }

  gifSearchAbortController = new AbortController();
  renderGifResultsLoading();

  const params = new URLSearchParams({
    key: TENOR_API_KEY,
    client_key: TENOR_CLIENT_KEY,
    q: normalizedQuery,
    limit: String(TENOR_RESULT_LIMIT),
    media_filter: "minimal",
    contentfilter: "medium"
  });

  try {
    const response = await fetch(`${TENOR_API_ENDPOINT}?${params.toString()}`, {
      signal: gifSearchAbortController.signal
    });

    if (!response.ok) {
      throw new Error("Tenor request failed.");
    }

    const payload = await response.json();
    const entries = Array.isArray(payload?.results) ? payload.results : [];
    const mapped = [];

    for (const entry of entries) {
      const formatted = extractTenorGifData(entry);

      if (formatted) {
        mapped.push(formatted);
      }
    }

    state.gifResults = mapped;
    renderGifResults();
  } catch (error) {
    if (error?.name === "AbortError") {
      return;
    }

    state.gifResults = [];
    elements.gifResults.innerHTML = "";
    elements.gifResults.append(createStickerEmptyState("Could not load GIFs right now."));
  } finally {
    gifSearchAbortController = undefined;
  }
}

function closeStickerMenu() {
  if (gifSearchAbortController) {
    gifSearchAbortController.abort();
    gifSearchAbortController = undefined;
  }

  elements.stickerMenu.classList.add("hidden");
}

async function openStickerMenu(preferredTab = state.stickerTab) {
  if (!state.currentUser || !state.activeChat || elements.stickerButton.disabled) {
    return;
  }

  closeAttachMenu();
  elements.stickerMenu.classList.remove("hidden");
  setStickerTab(preferredTab);

  if (state.stickerTab === "emoji") {
    await ensureEmojiCatalogLoaded();
    renderEmojiPickerResults();
    elements.emojiSearchInput.focus();
    return;
  }

  if (state.gifResults.length === 0) {
    const defaultSearch = String(elements.gifSearchInput.value ?? "").trim() || "reaction";
    elements.gifSearchInput.value = defaultSearch;
    searchTenorGifs(defaultSearch);
  }

  elements.gifSearchInput.focus();
}

function toggleStickerMenu() {
  if (elements.stickerMenu.classList.contains("hidden")) {
    openStickerMenu();
    return;
  }

  closeStickerMenu();
}

function insertTextAtCursor(input, textToInsert) {
  if (!input || input.disabled) {
    return;
  }

  const value = String(input.value ?? "");
  const insert = String(textToInsert ?? "");
  const start = Number(input.selectionStart ?? value.length);
  const end = Number(input.selectionEnd ?? value.length);
  const nextValue = `${value.slice(0, start)}${insert}${value.slice(end)}`;

  input.value = nextValue;

  const nextCaret = start + insert.length;
  input.setSelectionRange(nextCaret, nextCaret);
  input.focus();
  resizeComposerInput();
  storeActiveChatDraft();
  touchLocalTypingState();
}

function bindLongPress(target, onLongPress) {
  let timerId;
  let startX = 0;
  let startY = 0;
  let longPressed = false;

  target.addEventListener(
    "touchstart",
    (event) => {
      if (event.touches.length !== 1) {
        return;
      }

      longPressed = false;
      const touch = event.touches[0];
      startX = touch.clientX;
      startY = touch.clientY;

      clearTimeout(timerId);
      timerId = window.setTimeout(() => {
        longPressed = true;
        onLongPress(startX, startY);
      }, 450);
    },
    { passive: true }
  );

  target.addEventListener(
    "touchmove",
    (event) => {
      if (!timerId || event.touches.length !== 1) {
        return;
      }

      const touch = event.touches[0];
      const movedX = Math.abs(touch.clientX - startX);
      const movedY = Math.abs(touch.clientY - startY);

      if (movedX > 10 || movedY > 10) {
        clearTimeout(timerId);
        timerId = undefined;
      }
    },
    { passive: true }
  );

  const clear = () => {
    clearTimeout(timerId);
    timerId = undefined;
  };

  target.addEventListener("touchend", clear, { passive: true });
  target.addEventListener("touchcancel", clear, { passive: true });

  return () => {
    const result = longPressed;
    longPressed = false;
    return result;
  };
}

function closeInlinePrompt(result = null) {
  const resolver = inlinePromptResolve;
  inlinePromptResolve = undefined;
  elements.inlinePromptModal.classList.add("hidden");
  elements.inlinePromptInput.value = "";

  if (resolver) {
    resolver(result);
  }
}

function openInlinePrompt({
  title = "Edit",
  description = "",
  initialValue = "",
  placeholder = "",
  submitLabel = "Save",
  maxLength = 1200,
  multiline = true,
  showInput = true
} = {}) {
  if (inlinePromptResolve) {
    closeInlinePrompt(null);
  }

  elements.inlinePromptTitle.textContent = String(title);

  const descriptionText = String(description ?? "").trim();
  elements.inlinePromptDescription.textContent = descriptionText;
  elements.inlinePromptDescription.classList.toggle("hidden", !descriptionText);

  elements.inlinePromptInput.value = showInput ? String(initialValue ?? "") : "";
  elements.inlinePromptInput.placeholder = showInput ? String(placeholder ?? "") : "";
  elements.inlinePromptInput.maxLength = Math.max(1, Number(maxLength) || 1200);
  elements.inlinePromptInput.rows = multiline ? 4 : 1;
  elements.inlinePromptInput.classList.toggle("single-line", !multiline);
  elements.inlinePromptInput.disabled = !showInput;
  elements.inlinePromptInput.classList.toggle("hidden", !showInput);
  elements.inlinePromptSubmitButton.textContent = String(submitLabel ?? "Save");

  elements.inlinePromptModal.classList.remove("hidden");

  if (showInput) {
    window.requestAnimationFrame(() => {
      elements.inlinePromptInput.focus();
      elements.inlinePromptInput.select();
    });
  } else {
    window.requestAnimationFrame(() => {
      elements.inlinePromptSubmitButton.focus();
    });
  }

  return new Promise((resolve) => {
    inlinePromptResolve = resolve;
  });
}

async function confirmInlineAction({
  title = "Confirm",
  description = "",
  confirmLabel = "Confirm"
} = {}) {
  const result = await openInlinePrompt({
    title,
    description,
    submitLabel: confirmLabel,
    multiline: false,
    showInput: false
  });
  return result !== null;
}

async function editChatMessage(chatId, message) {
  const nextText = await openInlinePrompt({
    title: "Edit message",
    initialValue: String(message.text ?? ""),
    placeholder: "Update your message",
    submitLabel: "Save",
    maxLength: 1200,
    multiline: true
  });

  if (nextText === null) {
    return;
  }

  socket.emit(
    "edit_message",
    {
      chatId,
      messageId: message.id,
      text: nextText
    },
    (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Could not edit message.");
      }
    }
  );
}

async function deleteChatMessage(chatId, message) {
  const confirmed = await confirmInlineAction({
    title: "Delete message",
    description: "This message will be removed for everyone in this chat.",
    confirmLabel: "Delete"
  });

  if (!confirmed) {
    return;
  }

  socket.emit(
    "delete_message",
    {
      chatId,
      messageId: message.id
    },
    (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Could not delete message.");
      }
    }
  );
}

async function promptAddReaction(chatId, message) {
  const suggested = QUICK_REACTIONS[0];
  const emoji = await openInlinePrompt({
    title: "Add reaction",
    description: "Add any emoji reaction for this message.",
    initialValue: suggested,
    placeholder: "Emoji",
    submitLabel: "React",
    maxLength: 32,
    multiline: false
  });

  if (emoji === null) {
    return;
  }

  const nextEmoji = String(emoji).trim();

  if (!nextEmoji) {
    return;
  }

  toggleMessageReaction(chatId, message.id, nextEmoji);
}

function showMessageActions(chatId, message, x, y) {
  const items = [
    {
      label: "Reply",
      onSelect: () => setReplyTarget(message)
    }
  ];

  if (!message.deleted) {
    for (const emoji of QUICK_REACTIONS.slice(0, 3)) {
      items.push({
        label: `React ${emoji}`,
        onSelect: () => toggleMessageReaction(chatId, message.id, emoji)
      });
    }

    items.push({
      label: "Add custom reaction",
      onSelect: () => promptAddReaction(chatId, message)
    });
  }

  const isOwnMessage = message.senderKey === state.currentUser?.key;

  if (isOwnMessage && !message.deleted) {
    items.push(
      {
        label: "Edit message",
        onSelect: () => editChatMessage(chatId, message)
      },
      {
        label: "Delete message",
        danger: true,
        onSelect: () => deleteChatMessage(chatId, message)
      }
    );
  }

  showContextMenu(x, y, items);
}

function toggleMessageReaction(chatId, messageId, emoji) {
  socket.emit(
    "toggle_reaction",
    {
      chatId,
      messageId,
      emoji
    },
    (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Could not add reaction.");
      }
    }
  );
}

function showFriendActions(friend, x, y) {
  showContextMenu(x, y, [
    {
      label: "Rename friend",
      onSelect: () => {
        const currentAlias = friend.alias ?? "";
        const alias = window.prompt(
          "Friend nickname (leave blank to reset)",
          currentAlias
        );

        if (alias === null) {
          return;
        }

        socket.emit("set_friend_alias", { friendUserKey: friend.key, alias }, (response) => {
          if (!response?.ok) {
            showToast(response?.error ?? "Could not rename friend.");
            return;
          }

          showToast(alias.trim() ? "Friend renamed." : "Friend nickname reset.");
        });
      }
    },
    {
      label: "Remove friend",
      danger: true,
      onSelect: () => {
        const confirmed = window.confirm(`Remove ${friend.name} from friends?`);

        if (!confirmed) {
          return;
        }

        socket.emit("remove_friend", { friendUserKey: friend.key }, (response) => {
          if (!response?.ok) {
            showToast(response?.error ?? "Could not remove friend.");
            return;
          }

          showToast("Friend removed.");
        });
      }
    }
  ]);
}

function renameGroupChat(summary) {
  const nextName = window.prompt("Group name", String(summary.title ?? "").trim());

  if (nextName === null) {
    return;
  }

  socket.emit("rename_group", { chatId: summary.id, name: nextName }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not rename group.");
      return;
    }

    showToast("Group renamed.");
  });
}

function deleteGroupChat(summary) {
  const confirmed = window.confirm(`Delete ${summary.title}?`);

  if (!confirmed) {
    return;
  }

  socket.emit("delete_group", { chatId: summary.id }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not delete group.");
    }
  });
}

function openAddMembersToGroup(summary) {
  if (!summary || summary.type !== "group") {
    return;
  }

  const openModal = () => {
    if (!state.activeChat || state.activeChat.type !== "group" || state.activeChat.id !== summary.id) {
      showToast("Open the group first.");
      return;
    }

    state.groupCreateMode = "group-add";
    state.groupCreateTargetChatId = state.activeChat.id;
    state.groupCreateBaseUserKey = null;
    state.groupCreateSelectedKeys = new Set();
    elements.groupCreateSearchInput.value = "";

    const candidateCount = renderGroupCreateModal();

    if (candidateCount === 0) {
      showToast("All your friends are already in this group.");
      return;
    }

    elements.groupCreateModal.classList.remove("hidden");
    window.requestAnimationFrame(() => {
      elements.groupCreateSearchInput.focus();
    });
  };

  if (state.activeChat?.id === summary.id && state.activeChat.type === "group") {
    openModal();
    return;
  }

  loadChat(summary.id, (loaded) => {
    if (!loaded) {
      return;
    }
    openModal();
  });
}

function showGroupChatActions(summary, x, y) {
  showContextMenu(x, y, [
    {
      label: "Add members",
      onSelect: () => openAddMembersToGroup(summary)
    },
    {
      label: "Rename group",
      onSelect: () => renameGroupChat(summary)
    },
    {
      label: "Delete group",
      danger: true,
      onSelect: () => deleteGroupChat(summary)
    }
  ]);
}

function saveActiveChatPreferences(patch, successMessage = "Chat settings updated.") {
  if (!state.activeChat) {
    return;
  }

  socket.emit(
    "set_chat_preferences",
    {
      chatId: state.activeChat.id,
      ...patch
    },
    (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Could not update chat settings.");
        return;
      }

      if (response.prefs && state.activeChat) {
        state.activeChat.viewerPrefs = response.prefs;
      }

      applyActiveChatAppearance();
      renderActiveChat();
      showToast(successMessage);
    }
  );
}

async function promptSetActiveChatNickname() {
  if (!state.activeChat) {
    return;
  }

  const prefs = activeChatViewerPrefs();
  const nickname = await openInlinePrompt({
    title: "Chat nickname",
    description: "Set a private nickname for this chat on your account.",
    initialValue: prefs.nickname,
    placeholder: "Nickname",
    submitLabel: "Save",
    maxLength: 48,
    multiline: false
  });

  if (nickname === null) {
    return;
  }

  saveActiveChatPreferences(
    {
      nickname
    },
    String(nickname).trim() ? "Chat nickname saved." : "Chat nickname cleared."
  );
}

async function promptSetActiveChatWallpaper() {
  if (!state.activeChat) {
    return;
  }

  const prefs = activeChatViewerPrefs();
  const wallpaper = await openInlinePrompt({
    title: "Chat wallpaper",
    description: "Use an image URL or data:image URL. Leave empty to clear.",
    initialValue: prefs.wallpaper,
    placeholder: "https://example.com/wallpaper.jpg",
    submitLabel: "Save",
    maxLength: 2048,
    multiline: false
  });

  if (wallpaper === null) {
    return;
  }

  saveActiveChatPreferences(
    {
      wallpaper
    },
    String(wallpaper).trim() ? "Wallpaper saved." : "Wallpaper cleared."
  );
}

function openActiveChatStyleMenu(anchorElement) {
  if (!state.activeChat || !anchorElement) {
    return;
  }

  const prefs = activeChatViewerPrefs();
  const rect = anchorElement.getBoundingClientRect();
  const x = rect.left + rect.width / 2;
  const y = rect.bottom + 8;

  const items = [
    {
      label: "Set chat nickname",
      onSelect: () => {
        promptSetActiveChatNickname();
      }
    },
    {
      label: "Set wallpaper URL",
      onSelect: () => {
        promptSetActiveChatWallpaper();
      }
    }
  ];

  if (prefs.nickname) {
    items.push({
      label: "Clear nickname",
      onSelect: () => {
        saveActiveChatPreferences({ nickname: "" }, "Chat nickname cleared.");
      }
    });
  }

  if (prefs.wallpaper) {
    items.push({
      label: "Clear wallpaper",
      onSelect: () => {
        saveActiveChatPreferences({ wallpaper: "" }, "Wallpaper cleared.");
      }
    });
  }

  const themeLabel = {
    default: "Default",
    slate: "Slate",
    forest: "Forest",
    sunset: "Sunset",
    night: "Night"
  };

  for (const theme of CHAT_THEME_OPTIONS) {
    items.push({
      label: `${prefs.theme === theme ? "Theme * " : "Theme "} ${themeLabel[theme] ?? theme}`,
      onSelect: () => {
        saveActiveChatPreferences({ theme }, `Theme set to ${themeLabel[theme] ?? theme}.`);
      }
    });
  }

  showContextMenu(x, y, items);
}

function hideGroupQuickPopover() {
  elements.groupCreateModal.classList.add("hidden");
  state.groupCreateMode = "direct";
  state.groupCreateSelectedKeys = new Set();
  state.groupCreateBaseUserKey = null;
  state.groupCreateTargetChatId = null;
  elements.groupCreateTitle.textContent = "Select Friends";
  elements.groupCreateHint.textContent = "You can add up to 8 more friends.";
  elements.groupCreateEmpty.textContent = "No friends found that are not already in this DM.";
  elements.groupCreateSearchInput.value = "";
  elements.groupCreateFriendList.innerHTML = "";
  elements.groupCreateEmpty.classList.add("hidden");
  elements.createGroupFromDirectButton.disabled = true;
  elements.createGroupFromDirectButton.textContent = "Create Group DM";
}

function isGroupQuickPopoverOpen() {
  return !elements.groupCreateModal.classList.contains("hidden");
}

function groupQuickCandidates() {
  if (state.groupCreateMode === "group-add") {
    if (
      !state.activeChat ||
      state.activeChat.type !== "group" ||
      state.activeChat.id !== state.groupCreateTargetChatId
    ) {
      return [];
    }

    const participantKeys = new Set(state.activeChat.participants.map((participant) => participant.key));
    return state.relationships.friends.filter((friend) => !participantKeys.has(friend.key));
  }

  const directTarget =
    (state.groupCreateBaseUserKey && resolveUserByKey(state.groupCreateBaseUserKey)) ||
    currentDirectTarget();

  if (!directTarget) {
    return [];
  }

  return state.relationships.friends.filter((friend) => friend.key !== directTarget.key);
}

function renderGroupCreateModal() {
  const isGroupMode = state.groupCreateMode === "group-add";
  const candidates = groupQuickCandidates();
  const allowedKeys = new Set(candidates.map((friend) => friend.key));

  for (const selectedKey of [...state.groupCreateSelectedKeys]) {
    if (!allowedKeys.has(selectedKey)) {
      state.groupCreateSelectedKeys.delete(selectedKey);
    }
  }

  const query = elements.groupCreateSearchInput.value.trim().toLowerCase();
  const filtered = candidates.filter((friend) => {
    if (!query) {
      return true;
    }

    return String(friend.name ?? "")
      .toLowerCase()
      .includes(query);
  });

  const remainingSlots = Math.max(0, MAX_GROUP_CREATE_EXTRA_MEMBERS - state.groupCreateSelectedKeys.size);
  elements.groupCreateTitle.textContent = isGroupMode ? "Add Members" : "Select Friends";
  elements.groupCreateHint.textContent = `You can add ${remainingSlots} more friend${
    remainingSlots === 1 ? "" : "s"
  }.`;
  elements.groupCreateEmpty.textContent = isGroupMode
    ? "No friends found that are not already in this group."
    : "No friends found that are not already in this DM.";

  elements.groupCreateFriendList.innerHTML = "";

  if (filtered.length === 0) {
    elements.groupCreateEmpty.classList.remove("hidden");
  } else {
    elements.groupCreateEmpty.classList.add("hidden");
  }

  for (const friend of filtered) {
    const item = document.createElement("li");
    item.className = "group-create-item";

    const label = document.createElement("label");
    label.className = "group-create-label";

    const avatar = document.createElement("div");
    avatar.className = "avatar group-create-avatar";
    setAvatar(avatar, friend);

    const name = document.createElement("p");
    name.className = "group-create-name";
    name.textContent = friend.name;

    const checkbox = document.createElement("input");
    checkbox.className = "group-create-check";
    checkbox.type = "checkbox";
    checkbox.checked = state.groupCreateSelectedKeys.has(friend.key);
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) {
        if (state.groupCreateSelectedKeys.size >= MAX_GROUP_CREATE_EXTRA_MEMBERS) {
          checkbox.checked = false;
          showToast(`You can add up to ${MAX_GROUP_CREATE_EXTRA_MEMBERS} extra friends.`);
          return;
        }

        state.groupCreateSelectedKeys.add(friend.key);
      } else {
        state.groupCreateSelectedKeys.delete(friend.key);
      }

      renderGroupCreateModal();
    });

    label.append(avatar, name, checkbox);
    item.append(label);
    elements.groupCreateFriendList.append(item);
  }

  const selectedCount = state.groupCreateSelectedKeys.size;
  elements.createGroupFromDirectButton.disabled = selectedCount === 0;
  if (isGroupMode) {
    elements.createGroupFromDirectButton.textContent =
      selectedCount > 0 ? `Add Members (${selectedCount})` : "Add Members";
  } else {
    elements.createGroupFromDirectButton.textContent =
      selectedCount > 0 ? `Create Group DM (${selectedCount + 2})` : "Create Group DM";
  }

  return candidates.length;
}

function openGroupQuickPopover() {
  if (state.activeChat?.type === "group") {
    const summary =
      state.chatSummaries.find(
        (entry) => entry.id === state.activeChat?.id && entry.type === "group"
      ) ?? {
        id: state.activeChat.id,
        type: "group",
        title: state.activeChat.name || "Group chat"
      };

    openAddMembersToGroup(summary);
    return;
  }

  const directTarget = currentDirectTarget();

  if (!directTarget || !state.activeChat || state.activeChat.type === "group") {
    showToast("Open a direct chat first.");
    return;
  }

  state.groupCreateMode = "direct";
  state.groupCreateTargetChatId = null;
  state.groupCreateBaseUserKey = directTarget.key;
  state.groupCreateSelectedKeys = new Set();
  elements.groupCreateSearchInput.value = "";

  const candidateCount = renderGroupCreateModal();

  if (candidateCount === 0) {
    showToast("Add at least one more friend to create a group.");
    return;
  }

  elements.groupCreateModal.classList.remove("hidden");
  window.requestAnimationFrame(() => {
    elements.groupCreateSearchInput.focus();
  });
}

function toggleGroupQuickPopover() {
  if (isGroupQuickPopoverOpen()) {
    hideGroupQuickPopover();
    return;
  }

  openGroupQuickPopover();
}

function createGroupFromDirect() {
  if (state.groupCreateMode === "group-add") {
    if (
      !state.activeChat ||
      state.activeChat.type !== "group" ||
      state.activeChat.id !== state.groupCreateTargetChatId
    ) {
      showToast("Open a group chat first.");
      hideGroupQuickPopover();
      return;
    }

    const memberUserKeys = [...state.groupCreateSelectedKeys].filter(Boolean);

    if (memberUserKeys.length === 0) {
      showToast("Pick at least one friend to add.");
      return;
    }

    socket.emit(
      "add_group_members",
      {
        chatId: state.activeChat.id,
        memberUserKeys
      },
      (response) => {
        if (!response?.ok) {
          showToast(response?.error ?? "Could not add members.");
          return;
        }

        hideGroupQuickPopover();

        if (response.chat) {
          state.activeChat = response.chat;
          storeLastOpenChatId(response.chat.id);
          renderActiveChat();
        } else {
          renderProfilePanel();
        }

        renderChatList();
        showToast(memberUserKeys.length === 1 ? "Member added." : "Members added.");
      }
    );
    return;
  }

  const directTarget =
    (state.groupCreateBaseUserKey && resolveUserByKey(state.groupCreateBaseUserKey)) ||
    currentDirectTarget();

  if (!directTarget || !state.activeChat || state.activeChat.type === "group") {
    showToast("Open a direct chat first.");
    hideGroupQuickPopover();
    return;
  }

  const extraUserKeys = [...state.groupCreateSelectedKeys].filter(Boolean);

  if (extraUserKeys.length === 0) {
    showToast("Pick at least one friend to add.");
    return;
  }

  socket.emit(
    "create_group_from_direct",
    {
      directUserKey: directTarget.key,
      extraUserKeys
    },
    (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Could not create group.");
        return;
      }

      hideGroupQuickPopover();
      state.messageSearchQuery = "";
      elements.messageSearchInput.value = "";
      state.activeChat = response.chat;
      storeLastOpenChatId(response.chat.id);
      state.unreadByChatId[response.chat.id] = 0;

      renderActiveChat();
      renderChatList();
      showToast("Group created.");
    }
  );
}

function previewTextForMessage(message) {
  if (!message || message.deleted) {
    return "Message deleted";
  }

  const text = String(message.text ?? "").trim();

  if (text) {
    return text.slice(0, 160);
  }

  const attachments = Array.isArray(message.attachments) ? message.attachments : [];

  if (attachments.length > 0) {
    return attachments.some((attachment) => String(attachment?.type ?? "").startsWith("image/"))
      ? "[Image]"
      : "[Attachment]";
  }

  return "Message";
}

function setReplyTarget(message) {
  if (!message || message.senderKey === "system") {
    return;
  }

  state.replyingTo = {
    messageId: message.id,
    senderName: message.senderName,
    preview: previewTextForMessage(message)
  };

  renderReplyPreview();
  elements.messageInput.focus();
}

function clearReplyTarget() {
  state.replyingTo = null;
  renderReplyPreview();
}

function renderReplyPreview() {
  elements.replyPreview.innerHTML = "";

  if (!state.replyingTo) {
    elements.replyPreview.classList.add("hidden");
    return;
  }

  const card = document.createElement("div");
  card.className = "reply-preview-card";

  const label = document.createElement("p");
  label.className = "reply-preview-label";
  label.textContent = `Replying to ${state.replyingTo.senderName}`;

  const preview = document.createElement("p");
  preview.className = "reply-preview-text";
  preview.textContent = state.replyingTo.preview;

  const cancel = document.createElement("button");
  cancel.type = "button";
  cancel.className = "chip-remove";
  cancel.textContent = "x";
  cancel.addEventListener("click", () => {
    clearReplyTarget();
  });

  card.append(label, preview, cancel);
  elements.replyPreview.append(card);
  elements.replyPreview.classList.remove("hidden");
}

function resolveReplyReference(chat, message) {
  const replyMessageId = String(message.replyToMessageId ?? "").trim();
  const replySnapshot = message.replyTo && typeof message.replyTo === "object" ? message.replyTo : null;

  if (!replyMessageId && !replySnapshot) {
    return null;
  }

  const liveMessage = chat.messages.find((entry) => entry.id === replyMessageId) ?? null;

  if (liveMessage) {
    return {
      messageId: liveMessage.id,
      senderName: liveMessage.senderName,
      preview: previewTextForMessage(liveMessage)
    };
  }

  if (!replySnapshot) {
    return null;
  }

  return {
    messageId: String(replySnapshot.messageId ?? "").trim() || null,
    senderName: String(replySnapshot.senderName ?? "Unknown"),
    preview: String(replySnapshot.text ?? "Message")
  };
}

function scrollToMessage(messageId) {
  const targetId = String(messageId ?? "").trim();

  if (!targetId) {
    return;
  }

  const target = elements.messages.querySelector(`[data-message-id="${targetId}"]`);

  if (!target) {
    return;
  }

  target.scrollIntoView({ behavior: "smooth", block: "center" });
  target.classList.add("jump-highlight");
  window.setTimeout(() => {
    target.classList.remove("jump-highlight");
    updateJumpToUnreadButton();
  }, 900);
}

function createUnreadDivider() {
  const divider = document.createElement("div");
  divider.className = "unread-divider";
  divider.textContent = "New messages";
  divider.dataset.unreadDivider = "true";
  return divider;
}

function updateJumpToUnreadButton() {
  if (!elements.jumpToUnreadButton) {
    return;
  }

  const activeChatId = state.activeChat?.id;
  const unreadAnchorId = state.activeUnreadAnchorMessageId;
  const hasSearchQuery = state.messageSearchQuery.trim().length > 0;

  if (!activeChatId || !unreadAnchorId || hasSearchQuery) {
    elements.jumpToUnreadButton.classList.add("hidden");
    return;
  }

  const anchorElement = elements.messages.querySelector(`[data-message-id="${unreadAnchorId}"]`);

  if (!anchorElement) {
    elements.jumpToUnreadButton.classList.add("hidden");
    return;
  }

  const containerRect = elements.messages.getBoundingClientRect();
  const anchorRect = anchorElement.getBoundingClientRect();
  const visible = anchorRect.top >= containerRect.top && anchorRect.bottom <= containerRect.bottom;

  elements.jumpToUnreadButton.classList.toggle("hidden", visible);
}

function searchFiltersFromQuery(query) {
  const tokens = String(query ?? "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  const filters = {
    hasLinks: false,
    hasMedia: false,
    hasFiles: false,
    from: "",
    terms: []
  };

  for (const token of tokens) {
    const normalized = token.toLowerCase();

    if (normalized === "links" || normalized === "link" || normalized === "has:links") {
      filters.hasLinks = true;
      continue;
    }

    if (normalized === "media" || normalized === "has:media") {
      filters.hasMedia = true;
      continue;
    }

    if (normalized === "files" || normalized === "file" || normalized === "has:files") {
      filters.hasFiles = true;
      continue;
    }

    if (normalized.startsWith("from:") && normalized.length > 5) {
      filters.from = normalized.slice(5);
      continue;
    }

    filters.terms.push(normalized);
  }

  return filters;
}

function messageHasLinks(message) {
  const text = String(message?.text ?? "");

  if (/\b(?:https?:\/\/|www\.)[^\s]+/i.test(text)) {
    return true;
  }

  const attachments = Array.isArray(message?.attachments) ? message.attachments : [];
  return attachments.some((attachment) => {
    const name = String(attachment?.name ?? "");
    return /\b(?:https?:\/\/|www\.)[^\s]+/i.test(name);
  });
}

function messageHasMedia(message) {
  const attachments = Array.isArray(message?.attachments) ? message.attachments : [];
  return attachments.some((attachment) => {
    const type = String(attachment?.type ?? "").toLowerCase();
    return type.startsWith("image/") || type.startsWith("video/") || type.startsWith("audio/");
  });
}

function messageHasFiles(message) {
  const attachments = Array.isArray(message?.attachments) ? message.attachments : [];
  return attachments.some((attachment) => {
    const type = String(attachment?.type ?? "").toLowerCase();
    return !type.startsWith("image/") && !type.startsWith("video/") && !type.startsWith("audio/");
  });
}

function messageMatchesSearch(message, query) {
  const filters = searchFiltersFromQuery(query);

  if (filters.from) {
    const sender = String(message?.senderName ?? "").toLowerCase();
    const senderKey = String(message?.senderKey ?? "").toLowerCase();
    if (!sender.includes(filters.from) && !senderKey.includes(filters.from)) {
      return false;
    }
  }

  if (filters.hasLinks && !messageHasLinks(message)) {
    return false;
  }

  if (filters.hasMedia && !messageHasMedia(message)) {
    return false;
  }

  if (filters.hasFiles && !messageHasFiles(message)) {
    return false;
  }

  if (filters.terms.length === 0) {
    return true;
  }

  const parts = [
    String(message.senderName ?? ""),
    String(message.senderKey ?? ""),
    String(message.text ?? ""),
    String(message.kind ?? "")
  ];

  const attachments = Array.isArray(message.attachments) ? message.attachments : [];
  for (const attachment of attachments) {
    parts.push(String(attachment?.name ?? ""));
    parts.push(String(attachment?.type ?? ""));
  }

  if (message.replyTo && typeof message.replyTo === "object") {
    parts.push(String(message.replyTo.senderName ?? ""));
    parts.push(String(message.replyTo.text ?? ""));
  }

  if (message.linkPreview && typeof message.linkPreview === "object") {
    parts.push(String(message.linkPreview.title ?? ""));
    parts.push(String(message.linkPreview.description ?? ""));
    parts.push(String(message.linkPreview.siteName ?? ""));
    parts.push(String(message.linkPreview.authorName ?? ""));
    parts.push(String(message.linkPreview.providerKey ?? ""));
    parts.push(String(message.linkPreview.url ?? ""));
  }

  const haystack = parts.join(" ").toLowerCase();
  return filters.terms.every((term) => haystack.includes(term));
}

function typingUsersForActiveChat() {
  const chatId = state.activeChat?.id;

  if (!chatId) {
    return [];
  }

  const users = state.typingByChatId[chatId] ?? {};
  const now = Date.now();
  const names = [];

  for (const [userKey, entry] of Object.entries(users)) {
    if (userKey === state.currentUser?.key) {
      continue;
    }

    if (Number(entry?.expiresAt ?? 0) <= now) {
      continue;
    }

    names.push(String(entry?.name ?? "Someone"));
  }

  return names;
}

function renderTypingIndicator() {
  const names = typingUsersForActiveChat();

  if (names.length === 0) {
    elements.typingIndicator.classList.add("hidden");
    elements.typingIndicator.textContent = "";
    return;
  }

  const label =
    names.length === 1
      ? `${names[0]} is typing...`
      : names.length === 2
        ? `${names[0]} and ${names[1]} are typing...`
        : `${names[0]} and others are typing...`;

  elements.typingIndicator.textContent = label;
  elements.typingIndicator.classList.remove("hidden");
}

function applyTypingState(payload) {
  const chatId = String(payload?.chatId ?? "").trim();
  const fromKey = String(payload?.fromKey ?? "").trim();

  if (!chatId || !fromKey) {
    return;
  }

  if (!state.typingByChatId[chatId]) {
    state.typingByChatId[chatId] = {};
  }

  if (payload?.isTyping) {
    state.typingByChatId[chatId][fromKey] = {
      name: String(payload?.fromName ?? "Someone"),
      expiresAt: Date.now() + TYPING_REMOTE_TTL_MS
    };
  } else {
    delete state.typingByChatId[chatId][fromKey];
  }

  renderTypingIndicator();
}

function pruneTypingState() {
  const now = Date.now();
  let changed = false;

  for (const [chatId, users] of Object.entries(state.typingByChatId)) {
    for (const [userKey, entry] of Object.entries(users)) {
      if (Number(entry?.expiresAt ?? 0) <= now) {
        delete users[userKey];
        changed = true;
      }
    }

    if (Object.keys(users).length === 0) {
      delete state.typingByChatId[chatId];
    }
  }

  if (changed) {
    renderTypingIndicator();
  }
}

function emitTypingState(chatId, isTyping) {
  if (!chatId) {
    return;
  }

  socket.emit("typing_state", { chatId, isTyping }, () => {});
}

function startLocalTypingPulse() {
  clearInterval(localTypingPulseTimer);

  localTypingPulseTimer = window.setInterval(() => {
    if (!localTypingActive || !localTypingChatId) {
      clearInterval(localTypingPulseTimer);
      localTypingPulseTimer = undefined;
      return;
    }

    const activeChatId = state.activeChat?.id ?? null;
    const hasDraft = activeChatId === localTypingChatId && elements.messageInput.value.length > 0;

    if (!hasDraft) {
      stopLocalTyping();
      return;
    }

    emitTypingState(localTypingChatId, true);
  }, TYPING_PULSE_MS);
}

function stopLocalTyping() {
  clearInterval(localTypingPulseTimer);
  localTypingPulseTimer = undefined;

  if (localTypingActive && localTypingChatId) {
    emitTypingState(localTypingChatId, false);
  }

  localTypingActive = false;
  localTypingChatId = null;
}

function touchLocalTypingState() {
  const chatId = state.activeChat?.id ?? null;
  const hasDraft = Boolean(chatId && elements.messageInput.value.length > 0);

  if (!hasDraft) {
    stopLocalTyping();
    return;
  }

  if (localTypingChatId !== chatId) {
    stopLocalTyping();
    localTypingChatId = chatId;
  }

  if (!localTypingActive) {
    localTypingActive = true;
    emitTypingState(chatId, true);
    startLocalTypingPulse();
    return;
  }

  if (!localTypingPulseTimer) {
    startLocalTypingPulse();
  }
}

function markActiveChatRead() {
  const chatId = state.activeChat?.id;

  if (!chatId) {
    return;
  }

  if (document.visibilityState !== "visible") {
    return;
  }

  const pendingUnreadCount = Number(state.unreadByChatId[chatId] ?? 0);

  if (pendingUnreadCount > 0) {
    rememberUnreadAnchorForChat(state.activeChat, pendingUnreadCount);
    state.unreadByChatId[chatId] = 0;
    renderChatList();
  }

  const readAt = new Date().toISOString();
  let changed = false;

  for (const message of state.activeChat?.messages ?? []) {
    if (message.senderKey === "system" || message.senderKey === state.currentUser?.key) {
      continue;
    }

    if (!message.deliveredBy || typeof message.deliveredBy !== "object") {
      message.deliveredBy = {};
    }

    if (!message.readBy || typeof message.readBy !== "object") {
      message.readBy = {};
    }

    if (!message.deliveredBy[state.currentUser.key]) {
      message.deliveredBy[state.currentUser.key] = readAt;
      changed = true;
    }

    if (!message.readBy[state.currentUser.key]) {
      message.readBy[state.currentUser.key] = readAt;
      changed = true;
    }
  }

  if (changed) {
    renderMessages(state.activeChat);
  }

  socket.emit("mark_chat_read", { chatId }, () => {});
}

function unreadAnchorFromCount(chat, unreadCount) {
  if (!chat || !Array.isArray(chat.messages)) {
    return null;
  }

  const count = Number(unreadCount ?? 0);

  if (!Number.isFinite(count) || count <= 0) {
    return null;
  }

  const incomingMessages = chat.messages.filter((message) => {
    return message.senderKey !== "system" && message.senderKey !== state.currentUser?.key;
  });

  if (incomingMessages.length === 0) {
    return null;
  }

  const anchorIndex = Math.max(0, incomingMessages.length - Math.floor(count));
  return incomingMessages[anchorIndex]?.id ?? null;
}

function firstUnreadByReadState(chat) {
  if (!chat || !state.currentUser) {
    return null;
  }

  for (const message of chat.messages ?? []) {
    if (message.senderKey === "system" || message.senderKey === state.currentUser.key) {
      continue;
    }

    const readBy = message.readBy && typeof message.readBy === "object" ? message.readBy : {};

    if (!readBy[state.currentUser.key]) {
      return message.id;
    }
  }

  return null;
}

function rememberUnreadAnchorForChat(chat, unreadCountOverride = null) {
  if (!chat?.id) {
    return null;
  }

  const unresolvedAnchor = unreadAnchorFromCount(
    chat,
    unreadCountOverride ?? state.unreadByChatId[chat.id]
  );
  const fallbackAnchor = firstUnreadByReadState(chat);
  const anchor = unresolvedAnchor || fallbackAnchor;

  if (anchor) {
    state.unreadAnchorByChatId[chat.id] = anchor;
    return anchor;
  }

  return null;
}

function resolveUnreadAnchorForChat(chat) {
  if (!chat?.id) {
    return null;
  }

  const storedAnchor = state.unreadAnchorByChatId[chat.id];

  if (storedAnchor && chat.messages.some((message) => message.id === storedAnchor)) {
    return storedAnchor;
  }

  if (storedAnchor) {
    delete state.unreadAnchorByChatId[chat.id];
  }

  const nextAnchor = rememberUnreadAnchorForChat(chat, state.unreadByChatId[chat.id]);

  if (!nextAnchor) {
    delete state.unreadAnchorByChatId[chat.id];
  }

  return nextAnchor;
}

function latestIsoDate(isoValues) {
  let latest = null;
  let latestTimestamp = 0;

  for (const rawValue of isoValues) {
    const value = String(rawValue ?? "").trim();

    if (!value) {
      continue;
    }

    const timestamp = Date.parse(value);

    if (!Number.isFinite(timestamp)) {
      continue;
    }

    if (!latest || timestamp > latestTimestamp) {
      latest = value;
      latestTimestamp = timestamp;
    }
  }

  return latest;
}

function renderReadReceipt(chat, message) {
  if (!chat || !message || message.senderKey !== state.currentUser?.key) {
    return null;
  }

  const recipients = chat.participants.filter((participant) => participant.key !== state.currentUser.key);

  if (recipients.length === 0) {
    return null;
  }

  const readBy = message.readBy && typeof message.readBy === "object" ? message.readBy : {};
  const deliveredBy =
    message.deliveredBy && typeof message.deliveredBy === "object" ? message.deliveredBy : {};

  const deliveredTimes = [];
  const readTimes = [];

  for (const participant of recipients) {
    const readAt = String(readBy[participant.key] ?? "").trim();

    if (readAt) {
      readTimes.push(readAt);
      deliveredTimes.push(readAt);
      continue;
    }

    const deliveredAt = String(deliveredBy[participant.key] ?? "").trim();

    if (deliveredAt) {
      deliveredTimes.push(deliveredAt);
    }
  }

  const latestReadAt = latestIsoDate(readTimes);
  const latestDeliveredAt = latestIsoDate(deliveredTimes);
  const receipt = document.createElement("p");
  receipt.className = "message-read-receipt";

  if (latestReadAt) {
    if (recipients.length === 1) {
      receipt.textContent = `Read ${formatSummaryTime(latestReadAt)}`;
      return receipt;
    }

    receipt.textContent = `Read ${readTimes.length}/${recipients.length} - ${formatSummaryTime(
      latestReadAt
    )}`;
    return receipt;
  }

  if (latestDeliveredAt) {
    if (recipients.length === 1) {
      receipt.textContent = `Delivered ${formatSummaryTime(latestDeliveredAt)}`;
      return receipt;
    }

    receipt.textContent = `Delivered ${deliveredTimes.length}/${recipients.length} - ${formatSummaryTime(
      latestDeliveredAt
    )}`;
    return receipt;
  }

  receipt.textContent = `Sent ${formatSummaryTime(message.sentAt)}`;
  return receipt;
}

function renderReactions(chat, message) {
  if (message.deleted) {
    return null;
  }

  const reactions = Array.isArray(message.reactions)
    ? message.reactions.filter((reaction) => {
        return (
          String(reaction?.emoji ?? "").trim() &&
          Array.isArray(reaction?.users) &&
          reaction.users.length > 0
        );
      })
    : [];

  if (reactions.length === 0) {
    return null;
  }

  const row = document.createElement("div");
  row.className = "message-reactions";

  for (const reaction of reactions) {
    const emoji = String(reaction.emoji ?? "").trim();
    const users = reaction.users.filter(Boolean);
    const reactedByMe = users.includes(state.currentUser?.key);

    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = `reaction-chip${reactedByMe ? " active" : ""}`;
    chip.textContent = `${emoji} ${users.length}`;
    chip.addEventListener("click", () => {
      toggleMessageReaction(chat.id, message.id, emoji);
    });

    row.append(chip);
  }

  return row;
}

function formatMessageTime(isoDate) {
  if (!isoDate) {
    return "";
  }

  const date = new Date(isoDate);

  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return timeFormatter.format(date);
}

function formatSummaryTime(isoDate) {
  if (!isoDate) {
    return "";
  }

  const date = new Date(isoDate);

  if (Number.isNaN(date.getTime())) {
    return "";
  }

  const now = new Date();

  const isSameDay =
    date.getFullYear() === now.getFullYear() &&
    date.getMonth() === now.getMonth() &&
    date.getDate() === now.getDate();

  return isSameDay ? timeFormatter.format(date) : dateTimeFormatter.format(date);
}

function notifyDesktop(title, body) {
  if (!state.notificationsEnabled) {
    return;
  }

  if (!("Notification" in window)) {
    return;
  }

  if (Notification.permission !== "granted") {
    return;
  }

  if (document.visibilityState === "visible") {
    return;
  }

  new Notification(title, { body });
}

function persistNotificationPreference() {
  localStorage.setItem(NOTIFICATION_PREF_KEY, state.notificationsEnabled ? "on" : "off");
}

function setNotificationsEnabled(enabled) {
  state.notificationsEnabled = Boolean(enabled);
  persistNotificationPreference();
  updateNotificationToggleButton();
}

function updateNotificationToggleButton() {
  if (!elements.enableNotificationsButton) {
    return;
  }

  if (!("Notification" in window)) {
    elements.enableNotificationsButton.disabled = true;
    elements.enableNotificationsButton.textContent = "Desktop notifications unavailable";
    return;
  }

  if (Notification.permission === "denied" && state.notificationsEnabled) {
    state.notificationsEnabled = false;
    persistNotificationPreference();
  }

  elements.enableNotificationsButton.disabled = false;

  if (Notification.permission === "denied") {
    elements.enableNotificationsButton.textContent = "Desktop notifications blocked";
    return;
  }

  if (Notification.permission === "granted") {
    elements.enableNotificationsButton.textContent = state.notificationsEnabled
      ? "Disable desktop notifications"
      : "Enable desktop notifications";
    return;
  }

  elements.enableNotificationsButton.textContent = state.notificationsEnabled
    ? "Grant desktop notifications"
    : "Enable desktop notifications";
}

function persistEnterToSendPreference() {
  localStorage.setItem(ENTER_TO_SEND_PREF_KEY, state.enterToSendEnabled ? "on" : "off");
}

function setEnterToSendEnabled(enabled) {
  state.enterToSendEnabled = Boolean(enabled);
  persistEnterToSendPreference();
  updateEnterToSendToggleButton();
}

function updateEnterToSendToggleButton() {
  if (!elements.toggleEnterToSendButton) {
    return;
  }

  elements.toggleEnterToSendButton.textContent = state.enterToSendEnabled
    ? "Enter to send: On (Shift+Enter newline)"
    : "Enter to send: Off (Enter newline)";
}

function isMobileViewport() {
  return window.matchMedia("(max-width: 900px)").matches;
}

function setSidebarOpen(open) {
  state.sidebarOpen = Boolean(open);
  elements.sidebar.classList.toggle("open", state.sidebarOpen);
  elements.sidebarBackdrop.classList.toggle("hidden", !state.sidebarOpen);
}

function setAuthMode(mode) {
  state.authMode = mode === "signup" ? "signup" : "login";

  const signup = state.authMode === "signup";

  elements.loginTabButton.classList.toggle("active", !signup);
  elements.signupTabButton.classList.toggle("active", signup);
  elements.authConfirmRow.classList.toggle("hidden", !signup);
  elements.authSubmitButton.textContent = signup ? "Create Account" : "Login";
  elements.authSwitchText.textContent = signup
    ? "Have an account? Switch to Login."
    : "Need an account? Switch to Sign Up.";
  setAuthPasswordVisibility(false);
  setAuthConfirmPasswordVisibility(false);
}

function setPasswordInputVisibility({ input, button, visible }) {
  if (!input) {
    return;
  }

  const shouldShow = Boolean(visible);
  input.type = shouldShow ? "text" : "password";

  if (!button) {
    return;
  }

  button.setAttribute("aria-label", shouldShow ? "Hide password" : "Show password");
  button.setAttribute("aria-pressed", shouldShow ? "true" : "false");
  button.title = shouldShow ? "Hide password" : "Show password";
  button.classList.toggle("is-visible", shouldShow);
}

function setAuthPasswordVisibility(visible) {
  setPasswordInputVisibility({
    input: elements.authPasswordInput,
    button: elements.authPasswordToggleButton,
    visible
  });
}

function setAuthConfirmPasswordVisibility(visible) {
  setPasswordInputVisibility({
    input: elements.authConfirmInput,
    button: elements.authConfirmToggleButton,
    visible
  });
}

function syncAttachMenuButtonState() {
  const enabled = !elements.attachButton.disabled;
  elements.attachCameraButton.disabled = !enabled;
  elements.attachUploadButton.disabled = !enabled;
  elements.attachPhotoButton.disabled = !enabled || !isMobileViewport();
  if (elements.attachVoiceNoteButton) {
    elements.attachVoiceNoteButton.disabled = !enabled;
  }
}

function resizeComposerInput() {
  const input = elements.messageInput;

  if (!input) {
    return;
  }

  const minHeight = 42;
  const maxHeight = 128;
  input.style.height = "auto";
  const nextHeight = Math.max(minHeight, Math.min(maxHeight, input.scrollHeight));
  input.style.height = `${nextHeight}px`;
  input.style.overflowY = input.scrollHeight > maxHeight ? "auto" : "hidden";
}

function composerCanAcceptFiles() {
  return Boolean(state.currentUser && state.activeChat && !elements.messageInput.disabled);
}

function hasFileDragPayload(dataTransfer) {
  const types = dataTransfer?.types;

  if (!types) {
    return false;
  }

  return Array.from(types).includes("Files");
}

function setComposerDragActive(active) {
  elements.composerForm.classList.toggle("drag-active", Boolean(active));
}

function resetComposerDragState() {
  composerDragDepth = 0;
  setComposerDragActive(false);
}

function setComposerEnabled(enabled) {
  elements.messageInput.disabled = !enabled;
  elements.sendButton.disabled = !enabled;
  elements.attachButton.disabled = !enabled;
  syncAttachMenuButtonState();
  elements.stickerButton.disabled = !enabled;

  if (!enabled) {
    stopLocalTyping();
    closeAttachMenu();
    closeStickerMenu();
    resetComposerDragState();
    if (state.voiceNoteRecorder) {
      stopVoiceNoteRecording(false);
    }
  }

  resizeComposerInput();
  updateVoiceNoteButton();
}

function createEmptyItem(message) {
  const item = document.createElement("li");
  item.className = "empty-state";
  item.textContent = message;
  return item;
}

function findFriendByKey(userKey) {
  return state.relationships.friends.find((friend) => friend.key === userKey) ?? null;
}

function resolveUserByKey(userKey) {
  if (!userKey) {
    return null;
  }

  const friend = findFriendByKey(userKey);

  if (friend) {
    return friend;
  }

  if (!state.activeChat) {
    return null;
  }

  return state.activeChat.participants.find((participant) => participant.key === userKey) ?? null;
}

function currentDirectTarget() {
  if (!state.currentUser || !state.activeChat || state.activeChat.type === "group") {
    return null;
  }

  return (
    state.activeChat.participants.find(
      (participant) => participant.key !== state.currentUser.key
    ) ?? null
  );
}

function setProfileUser(userKey) {
  state.profileUserKey = userKey || null;
  renderProfilePanel();
}

function updateSelfStrip() {
  if (!state.currentUser) {
    elements.selfName.textContent = "Not signed in";
    setAvatar(elements.selfAvatar, { name: "?" });
    return;
  }

  elements.selfName.textContent = state.currentUser.name;
  setAvatar(elements.selfAvatar, state.currentUser);
}

function renderSettingsAvatarPreview() {
  const fallbackName =
    elements.settingsDisplayNameInput.value.trim() || state.currentUser?.name || "?";

  setAvatar(elements.settingsAvatarPreview, {
    name: fallbackName,
    avatarDataUrl: state.settingsAvatarDataUrl
  });
}

function formatSettingsSessionIpAddress(rawValue) {
  const ipAddress = String(rawValue ?? "").trim();

  if (!ipAddress) {
    return "Unknown";
  }

  if (ipAddress.startsWith("::ffff:")) {
    return ipAddress.slice(7);
  }

  if (ipAddress === "::1") {
    return "127.0.0.1";
  }

  return ipAddress;
}

function browserNameForSessionUserAgent(rawUserAgent) {
  const ua = String(rawUserAgent ?? "").toLowerCase();

  if (!ua) {
    return "";
  }

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

  return "";
}

function osNameForSession(session) {
  const ua = String(session?.userAgent ?? "").toLowerCase();

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

  const fallback = String(session?.deviceName ?? session?.label ?? "").toLowerCase();

  if (fallback.includes("windows")) {
    return "Windows";
  }

  if (fallback.includes("android")) {
    return "Android";
  }

  if (fallback.includes("ios") || fallback.includes("iphone") || fallback.includes("ipad")) {
    return "iOS";
  }

  if (fallback.includes("mac")) {
    return "macOS";
  }

  if (fallback.includes("linux")) {
    return "Linux";
  }

  return "Device";
}

function formatSettingsSessionPrimaryLabel(session) {
  const os = osNameForSession(session);
  const browser = browserNameForSessionUserAgent(session?.userAgent);

  if (browser) {
    return `${os} • ${browser}`.toUpperCase();
  }

  const fallbackLabel = String(session?.label ?? "").trim();
  return (fallbackLabel || os).toUpperCase();
}

function formatSettingsSessionRelativeTime(isoDate) {
  const timestamp = Date.parse(String(isoDate ?? ""));

  if (!Number.isFinite(timestamp)) {
    return "active time unknown";
  }

  const diffMs = Date.now() - timestamp;
  const absMs = Math.abs(diffMs);
  const steps = [
    { unit: "year", ms: 1000 * 60 * 60 * 24 * 365 },
    { unit: "month", ms: 1000 * 60 * 60 * 24 * 30 },
    { unit: "week", ms: 1000 * 60 * 60 * 24 * 7 },
    { unit: "day", ms: 1000 * 60 * 60 * 24 },
    { unit: "hour", ms: 1000 * 60 * 60 },
    { unit: "minute", ms: 1000 * 60 }
  ];

  if (absMs < 60 * 1000) {
    return diffMs >= 0 ? "active just now" : "active in under a minute";
  }

  for (const step of steps) {
    if (absMs >= step.ms) {
      const value = Math.floor(absMs / step.ms);
      const suffix = value === 1 ? "" : "s";

      if (diffMs >= 0) {
        return `active ${value} ${step.unit}${suffix} ago`;
      }

      return `active in ${value} ${step.unit}${suffix}`;
    }
  }

  return "active just now";
}

function renderSettingsSessions() {
  if (
    !elements.settingsSessionsList ||
    !elements.settingsSessionsEmpty ||
    !elements.revokeOtherSessionsButton
  ) {
    return;
  }

  const sessions = Array.isArray(state.settingsSessions) ? state.settingsSessions : [];
  const hasOtherSession = sessions.some((session) => !session?.isCurrent);

  elements.settingsSessionsList.innerHTML = "";
  elements.revokeOtherSessionsButton.disabled =
    !state.currentUser || state.settingsSessionsLoading || !hasOtherSession;

  if (state.settingsSessionsLoading) {
    elements.settingsSessionsEmpty.textContent = "Loading sessions...";
    elements.settingsSessionsEmpty.classList.remove("hidden");
    return;
  }

  if (sessions.length === 0) {
    elements.settingsSessionsEmpty.textContent = "No active sessions found.";
    elements.settingsSessionsEmpty.classList.remove("hidden");
    return;
  }

  elements.settingsSessionsEmpty.classList.add("hidden");

  for (const session of sessions) {
    const item = document.createElement("li");
    item.className = "settings-session-item";

    const main = document.createElement("div");
    main.className = "settings-session-main";

    const icon = document.createElement("div");
    icon.className = "settings-session-icon";
    icon.setAttribute("aria-hidden", "true");
    icon.innerHTML =
      '<svg viewBox="0 0 24 24" fill="none"><rect x="4.5" y="5.5" width="15" height="10.5" rx="2" stroke="currentColor" stroke-width="1.8"/><path d="M9 19h6m-4.5-3h3" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>';
    main.append(icon);

    const body = document.createElement("div");
    body.className = "settings-session-body";

    const head = document.createElement("div");
    head.className = "settings-session-head";

    const label = document.createElement("p");
    label.className = "settings-session-label";
    label.textContent = formatSettingsSessionPrimaryLabel(session);
    head.append(label);

    const actions = document.createElement("div");
    actions.className = "settings-session-actions";

    if (session?.isCurrent === true) {
      const badge = document.createElement("span");
      badge.className = "settings-session-current";
      badge.textContent = "This device";
      actions.append(badge);
    }

    if (!session?.isCurrent) {
      const revokeButton = document.createElement("button");
      revokeButton.type = "button";
      revokeButton.className = "action-button settings-session-revoke";
      revokeButton.textContent = "Sign Out";
      revokeButton.addEventListener("click", () => {
        if (!state.currentUser || state.settingsSessionsLoading) {
          return;
        }

        state.settingsSessionsLoading = true;
        renderSettingsSessions();

        socket.emit("revoke_session", { sessionId: session.sessionId }, (response) => {
          state.settingsSessionsLoading = false;

          if (!response?.ok) {
            showToast(response?.error ?? "Could not sign out that device.");
            renderSettingsSessions();
            return;
          }

          state.settingsSessions = Array.isArray(response.sessions) ? response.sessions : [];
          renderSettingsSessions();
          showToast("Device signed out.");
        });
      });
      actions.append(revokeButton);
    }

    if (actions.childElementCount > 0) {
      head.append(actions);
    }

    body.append(head);

    const meta = document.createElement("p");
    meta.className = "settings-session-meta";
    meta.textContent = `IP ${formatSettingsSessionIpAddress(session?.ipAddress)} | ${formatSettingsSessionRelativeTime(
      session?.lastSeenAt ?? session?.createdAt
    )}`;
    body.append(meta);

    main.append(body);
    item.append(main);
    elements.settingsSessionsList.append(item);
  }
}

function refreshSettingsSessions(showErrors = true) {
  if (!state.currentUser) {
    return;
  }

  state.settingsSessionsLoading = true;
  renderSettingsSessions();

  socket.emit("list_sessions", {}, (response) => {
    state.settingsSessionsLoading = false;

    if (!response?.ok) {
      state.settingsSessions = [];
      renderSettingsSessions();

      if (showErrors) {
        showToast(response?.error ?? "Could not load active sessions.");
      }
      return;
    }

    state.settingsSessions = Array.isArray(response.sessions) ? response.sessions : [];
    renderSettingsSessions();
  });
}

function openSettingsModal() {
  if (!state.currentUser) {
    return;
  }

  if (isMobileViewport()) {
    setSidebarOpen(false);
  }

  state.settingsAvatarDataUrl = state.currentUser.avatarDataUrl ?? null;
  elements.settingsDisplayNameInput.value = state.currentUser.name ?? "";
  elements.settingsAvatarInput.value = "";
  elements.settingsCurrentPasswordInput.value = "";
  elements.settingsNewPasswordInput.value = "";
  elements.settingsDeletePasswordInput.value = "";
  state.settingsSessions = [];
  state.settingsSessionsLoading = true;
  renderSettingsAvatarPreview();
  renderSettingsSessions();
  updateNotificationToggleButton();
  updateEnterToSendToggleButton();
  elements.settingsModal.classList.remove("hidden");
  refreshSettingsSessions();
}

function updateHeaderActions() {
  const directTarget = currentDirectTarget();
  const isDirect = Boolean(directTarget);
  const isGroup = state.activeChat?.type === "group";
  const isTempChat = state.activeChat?.type === "temp";
  const hasExtraGroupCandidate =
    isDirect && state.relationships.friends.some((friend) => friend.key !== directTarget.key);
  const hasGroupAddCandidate =
    isGroup &&
    state.relationships.friends.some(
      (friend) =>
        !state.activeChat?.participants?.some((participant) => participant.key === friend.key)
    );
  const canUseGroupButton =
    Boolean(state.currentUser) && ((isDirect && hasExtraGroupCandidate) || Boolean(hasGroupAddCandidate));

  elements.voiceCallButton.disabled = !isDirect;
  elements.videoCallButton.disabled = !isDirect;
  if (elements.chatPrefsButton) {
    elements.chatPrefsButton.disabled = !state.activeChat;
  }
  elements.toggleGroupPanelButton.disabled = !canUseGroupButton;
  elements.openTempChatButton.disabled = !isDirect || isTempChat;
  elements.toggleGroupPanelButton.title = isGroup ? "Add members" : "Create group";
  elements.toggleGroupPanelButton.setAttribute("aria-label", isGroup ? "Add members" : "Create group");

  if (!canUseGroupButton) {
    hideGroupQuickPopover();
  }

  elements.closeTempChatButton.classList.toggle("hidden", !isTempChat);

  if (isTempChat) {
    const alreadyVoted = Array.isArray(state.activeChat.closeVotes)
      ? state.activeChat.closeVotes.includes(state.currentUser.key)
      : false;

    elements.closeTempChatButton.disabled = alreadyVoted;
    elements.closeTempChatButton.textContent = alreadyVoted ? "Waiting" : "Close Temp";
  }

  updateVoiceNoteButton();
}

function renderProfilePanel() {
  const chat = state.activeChat;

  if (!chat) {
    elements.profileEmpty.classList.remove("hidden");
    elements.profileContent.classList.add("hidden");
    elements.profileGroupContent.classList.add("hidden");
    return;
  }

  if (chat.type === "group") {
    elements.profileEmpty.classList.add("hidden");
    elements.profileContent.classList.add("hidden");
    elements.profileGroupContent.classList.remove("hidden");
    elements.profileGroupTitle.textContent = `${chat.name || "Group chat"} (Group)`;
    const participants = [...chat.participants].sort((left, right) => {
      const onlineDelta = Number(Boolean(right.online)) - Number(Boolean(left.online));

      if (onlineDelta !== 0) {
        return onlineDelta;
      }

      return String(left.name ?? "").localeCompare(String(right.name ?? ""));
    });
    const onlineCount = participants.filter((participant) => participant.online).length;
    elements.profileGroupMeta.textContent = `${onlineCount} online - ${participants.length} member${
      participants.length === 1 ? "" : "s"
    }`;
    elements.profileGroupList.innerHTML = "";

    for (const participant of participants) {
      const isCurrentUser = participant.key === state.currentUser?.key;
      const item = document.createElement("li");
      item.className = "profile-group-item";

      const avatar = document.createElement("div");
      avatar.className = "avatar profile-group-avatar";
      setAvatar(avatar, participant);

      const copy = document.createElement("div");
      copy.className = "profile-group-copy";

      const name = document.createElement("p");
      name.className = "profile-group-name";
      name.textContent = participant.name;

      const meta = document.createElement("div");
      meta.className = "profile-group-meta-row";

      const status = document.createElement("span");
      status.className = `profile-group-status-dot ${participant.online ? "online" : "offline"}`;

      const statusText = document.createElement("span");
      statusText.className = "profile-group-status-text";
      statusText.textContent = participant.online ? "Online" : "Offline";

      meta.append(status, statusText);

      if (isCurrentUser) {
        const youPill = document.createElement("span");
        youPill.className = "profile-group-pill";
        youPill.textContent = "You";
        meta.append(youPill);
      }

      copy.append(name, meta);
      item.append(avatar, copy);
      elements.profileGroupList.append(item);
    }

    return;
  }

  const directTarget = currentDirectTarget();
  const fallback = resolveUserByKey(state.profileUserKey);
  const user = directTarget ?? fallback;

  if (!user) {
    elements.profileEmpty.classList.remove("hidden");
    elements.profileContent.classList.add("hidden");
    elements.profileGroupContent.classList.add("hidden");
    return;
  }

  elements.profileEmpty.classList.add("hidden");
  elements.profileGroupContent.classList.add("hidden");
  elements.profileContent.classList.remove("hidden");

  setAvatar(elements.profileAvatar, user);
  elements.profileName.textContent = user.name;
  elements.profileState.className = `profile-state ${user.online ? "online" : "offline"}`;
  elements.profileState.textContent = user.online ? "Online" : "Offline";

  if (state.activeChat?.type === "temp") {
    elements.profileMeta.textContent = "Temp chat enabled";
  } else if (state.activeChat?.type === "group") {
    elements.profileMeta.textContent = "Group member";
  } else {
    elements.profileMeta.textContent = "Friend";
  }
}

function refreshGroupQuickOptionsIfOpen() {
  if (!isGroupQuickPopoverOpen()) {
    return;
  }

  const candidateCount = renderGroupCreateModal();

  if (candidateCount === 0) {
    hideGroupQuickPopover();
  }
}

function renderIncomingRequests() {
  elements.incomingRequests.innerHTML = "";

  const incoming = state.relationships.incomingRequests;

  if (incoming.length === 0) {
    elements.incomingRequests.append(createEmptyItem("No incoming requests."));
    return;
  }

  for (const request of incoming) {
    const item = document.createElement("li");
    item.className = "list-card";

    const head = document.createElement("div");
    head.className = "list-head";

    const name = document.createElement("span");
    name.className = "list-title";
    name.textContent = request.name;

    const status = document.createElement("span");
    status.className = `status-chip ${request.online ? "online" : "offline"}`;
    status.textContent = request.online ? "Online" : "Offline";

    head.append(name, status);

    const actions = document.createElement("div");
    actions.className = "inline-actions";

    const acceptButton = document.createElement("button");
    acceptButton.type = "button";
    acceptButton.className = "button-primary";
    acceptButton.textContent = "Accept";
    acceptButton.addEventListener("click", () => {
      socket.emit("accept_friend_request", { fromUserKey: request.key }, (response) => {
        if (!response?.ok) {
          showToast(response?.error ?? "Could not accept request.");
        }
      });
    });

    const declineButton = document.createElement("button");
    declineButton.type = "button";
    declineButton.className = "action-button";
    declineButton.textContent = "Decline";
    declineButton.addEventListener("click", () => {
      socket.emit("decline_friend_request", { fromUserKey: request.key }, (response) => {
        if (!response?.ok) {
          showToast(response?.error ?? "Could not decline request.");
        }
      });
    });

    actions.append(acceptButton, declineButton);
    item.append(head, actions);

    elements.incomingRequests.append(item);
  }
}

function renderOutgoingRequests() {
  elements.outgoingRequests.innerHTML = "";

  const outgoing = state.relationships.outgoingRequests;

  if (outgoing.length === 0) {
    elements.outgoingRequests.append(createEmptyItem("No outgoing requests."));
    return;
  }

  for (const request of outgoing) {
    const item = document.createElement("li");
    item.className = "list-card";

    const head = document.createElement("div");
    head.className = "list-head";

    const name = document.createElement("span");
    name.className = "list-title";
    name.textContent = request.name;

    const status = document.createElement("span");
    status.className = "status-chip pending";
    status.textContent = "Pending";

    head.append(name, status);

    const actions = document.createElement("div");
    actions.className = "inline-actions";

    const cancelButton = document.createElement("button");
    cancelButton.type = "button";
    cancelButton.className = "action-button";
    cancelButton.textContent = "Cancel";
    cancelButton.addEventListener("click", () => {
      socket.emit("cancel_friend_request", { toUserKey: request.key }, (response) => {
        if (!response?.ok) {
          showToast(response?.error ?? "Could not cancel request.");
        }
      });
    });

    actions.append(cancelButton);
    item.append(head, actions);

    elements.outgoingRequests.append(item);
  }
}

function openChatWith(target, isTemp) {
  storeActiveChatDraft();

  const withUserKey = typeof target === "object" && target ? target.key : null;
  const withUser = typeof target === "string" ? target : target?.name;

  socket.emit("open_chat", { withUserKey, withUser, isTemp }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not open chat.");
      return;
    }

    state.messageSearchQuery = "";
    elements.messageSearchInput.value = "";
    state.activeChat = response.chat;
    storeLastOpenChatId(response.chat.id);
    const unreadBeforeOpen = Number(state.unreadByChatId[response.chat.id] ?? 0);
    if (unreadBeforeOpen > 0) {
      rememberUnreadAnchorForChat(response.chat, unreadBeforeOpen);
    }
    state.unreadByChatId[response.chat.id] = 0;

    renderActiveChat();
    renderChatList();

    if (isMobileViewport()) {
      setSidebarOpen(false);
    }
  });
}

function renderFriends() {
  elements.friendsList.innerHTML = "";
  elements.friendCount.textContent = String(state.relationships.friends.length);

  if (state.relationships.friends.length === 0) {
    elements.friendsList.append(createEmptyItem("No friends yet."));
    refreshGroupQuickOptionsIfOpen();
    return;
  }

  const target = currentDirectTarget();

  for (const friend of state.relationships.friends) {
    const item = document.createElement("li");
    item.className = "friend-row";

    if (target && target.key === friend.key) {
      item.classList.add("active");
    }

    const consumeLongPress = bindLongPress(item, (x, y) => {
      showFriendActions(friend, x, y);
    });

    item.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      showFriendActions(friend, event.clientX, event.clientY);
    });

    item.addEventListener("click", () => {
      if (consumeLongPress()) {
        return;
      }

      setProfileUser(friend.key);
      openChatWith(friend, false);
    });

    const avatar = document.createElement("div");
    avatar.className = "avatar";
    setAvatar(avatar, friend);

    const body = document.createElement("div");
    body.className = "friend-body";

    const name = document.createElement("p");
    name.className = "friend-name";
    name.textContent = friend.name;

    const status = document.createElement("p");
    status.className = `friend-status ${friend.online ? "online" : "offline"}`;
    status.textContent = friend.online ? "Online" : "Offline";

    body.append(name, status);
    item.append(avatar, body);

    elements.friendsList.append(item);
  }

  refreshGroupQuickOptionsIfOpen();
}

function renderRelationshipState() {
  renderIncomingRequests();
  renderOutgoingRequests();
  renderFriends();
}

function loadChat(chatId, onLoaded) {
  storeActiveChatDraft();

  socket.emit("load_chat", { chatId }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not load chat.");

      if (typeof onLoaded === "function") {
        onLoaded(false);
      }
      return;
    }

    state.messageSearchQuery = "";
    elements.messageSearchInput.value = "";
    state.activeChat = response.chat;
    storeLastOpenChatId(response.chat.id);
    const unreadBeforeOpen = Number(state.unreadByChatId[chatId] ?? 0);
    if (unreadBeforeOpen > 0) {
      rememberUnreadAnchorForChat(response.chat, unreadBeforeOpen);
    }
    state.unreadByChatId[chatId] = 0;

    renderActiveChat();
    renderChatList();

    if (typeof onLoaded === "function") {
      onLoaded(true);
    }

    if (isMobileViewport()) {
      setSidebarOpen(false);
    }
  });
}

function loadChatAsync(chatId) {
  return new Promise((resolve) => {
    loadChat(chatId, resolve);
  });
}

function renderChatList() {
  elements.chatList.innerHTML = "";

  if (state.chatSummaries.length === 0) {
    elements.chatList.append(createEmptyItem("No chats yet."));
    return;
  }

  for (const summary of state.chatSummaries) {
    const item = document.createElement("li");
    item.className = "list-card chat-card";

    if (state.activeChat?.id === summary.id) {
      item.classList.add("active");
    }

    const head = document.createElement("div");
    head.className = "list-head";

    const titleWrap = document.createElement("div");
    titleWrap.className = "chat-title-wrap";

    const title = document.createElement("span");
    title.className = "list-title";
    title.textContent = summary.title;
    titleWrap.append(title);

    if (summary.isTemp) {
      const tempBadge = document.createElement("span");
      tempBadge.className = "temp-badge";
      tempBadge.textContent = "TEMP";
      titleWrap.append(tempBadge);
    } else if (summary.type === "group") {
      const groupBadge = document.createElement("span");
      groupBadge.className = "temp-badge group-badge";
      groupBadge.textContent = "GROUP";
      titleWrap.append(groupBadge);
    }

    const right = document.createElement("div");
    right.className = "chat-right";

    const time = document.createElement("span");
    time.className = "chat-time";
    time.textContent = formatSummaryTime(summary.lastMessageAt ?? summary.updatedAt);

    right.append(time);

    const closeButton = document.createElement("button");
    closeButton.type = "button";
    closeButton.className = "chat-close-button";
    closeButton.title = "Close chat";
    closeButton.setAttribute("aria-label", "Close chat");
    closeButton.textContent = "x";
    closeButton.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();

      socket.emit("hide_chat", { chatId: summary.id }, (response) => {
        if (!response?.ok) {
          showToast(response?.error ?? "Could not close chat.");
          return;
        }

        state.chatSummaries = state.chatSummaries.filter((entry) => entry.id !== summary.id);
        delete state.unreadByChatId[summary.id];
        clearStoredLastOpenChatIdIfMatches(summary.id);

        if (state.activeChat?.id === summary.id) {
          state.activeChat = null;
          renderActiveChat();
        }

        renderChatList();
      });
    });
    right.append(closeButton);

    const unread = Number(state.unreadByChatId[summary.id] ?? 0);
    if (unread > 0) {
      const unreadBadge = document.createElement("span");
      unreadBadge.className = "unread-badge";
      unreadBadge.textContent = unread > 9 ? "9+" : String(unread);
      right.append(unreadBadge);
    }

    head.append(titleWrap, right);

    const preview = document.createElement("p");
    preview.className = "chat-preview";
    preview.textContent = summary.lastMessage || "No messages yet.";

    item.append(head, preview);

    let consumeLongPress = () => false;

    if (summary.type === "group") {
      consumeLongPress = bindLongPress(item, (x, y) => {
        showGroupChatActions(summary, x, y);
      });

      item.addEventListener("contextmenu", (event) => {
        event.preventDefault();
        showGroupChatActions(summary, event.clientX, event.clientY);
      });
    }

    item.addEventListener("click", () => {
      if (consumeLongPress()) {
        return;
      }

      hideGroupQuickPopover();
      loadChat(summary.id);
    });

    elements.chatList.append(item);
  }
}

function formatAudioDurationSeconds(valueInSeconds, useRoundedSeconds = false) {
  const numericValue = Number(valueInSeconds);

  if (!Number.isFinite(numericValue)) {
    return "--:--";
  }

  const totalSeconds = Math.max(0, useRoundedSeconds ? Math.round(numericValue) : Math.floor(numericValue));

  if (totalSeconds === 0) {
    return "0:00";
  }

  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }

  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function sanitizeAttachmentDurationSeconds(value) {
  const numeric = Number(value);

  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }

  return Math.min(24 * 60 * 60, Math.round(numeric * 100) / 100);
}

function volumeIconMarkup(mode) {
  switch (mode) {
    case "muted":
      return `
        <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
          <path d="M11 5 6 9H3v6h3l5 4z"></path>
          <path d="m16.5 9.5 4 4"></path>
          <path d="m20.5 9.5-4 4"></path>
        </svg>
      `;
    case "low":
      return `
        <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
          <path d="M11 5 6 9H3v6h3l5 4z"></path>
          <path d="M16 12a3.2 3.2 0 0 0-1.7-2.85"></path>
        </svg>
      `;
    default:
      return `
        <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
          <path d="M11 5 6 9H3v6h3l5 4z"></path>
          <path d="M16 8.5a5 5 0 0 1 0 7"></path>
          <path d="M18.8 6a8.4 8.4 0 0 1 0 12"></path>
        </svg>
      `;
  }
}

function createMessageAudioPlayer(dataUrl, preferredTotalSeconds = null) {
  const player = document.createElement("div");
  player.className = "message-audio-player";

  const playButton = document.createElement("button");
  playButton.type = "button";
  playButton.className = "message-audio-play";
  playButton.textContent = "\u25B6";
  playButton.setAttribute("aria-label", "Play audio");

  const timeLabel = document.createElement("span");
  timeLabel.className = "message-audio-time";
  const initialTotalSeconds = sanitizeAttachmentDurationSeconds(preferredTotalSeconds);
  timeLabel.textContent = `0:00 / ${
    initialTotalSeconds !== null ? formatAudioDurationSeconds(initialTotalSeconds) : "--:--"
  }`;

  const seek = document.createElement("input");
  seek.type = "range";
  seek.className = "message-audio-seek";
  seek.min = "0";
  seek.max = "1000";
  seek.step = "1";
  seek.value = "0";
  seek.disabled = true;
  seek.setAttribute("aria-label", "Seek audio");

  const volumeWrap = document.createElement("div");
  volumeWrap.className = "message-audio-volume";

  const muteButton = document.createElement("button");
  muteButton.type = "button";
  muteButton.className = "message-audio-mute";
  muteButton.setAttribute("aria-label", "Mute audio");

  const volumeSeek = document.createElement("input");
  volumeSeek.type = "range";
  volumeSeek.className = "message-audio-volume-seek";
  volumeSeek.min = "0";
  volumeSeek.max = "100";
  volumeSeek.step = "1";
  volumeSeek.value = "100";
  volumeSeek.setAttribute("aria-label", "Volume");

  const audio = document.createElement("audio");
  audio.className = "message-audio";
  audio.preload = "auto";
  audio.src = dataUrl;
  audio.load();

  let lastNonZeroVolume = 1;
  let playbackRafId = 0;
  let playPrimingUntil = 0;
  let totalSeconds = initialTotalSeconds;

  const setRangeProgress = (input, ratio, cssVariableName) => {
    if (!input) {
      return;
    }

    const normalized = Math.max(0, Math.min(1, Number(ratio) || 0));
    input.style.setProperty(cssVariableName, `${Math.round(normalized * 100)}%`);
  };

  const syncDuration = () => {
    const duration = Number(audio.duration);

    if (!Number.isFinite(duration) || duration <= 0) {
      seek.disabled = true;
      updateTimeAndSeek();
      return;
    }

    totalSeconds = sanitizeAttachmentDurationSeconds(duration) ?? totalSeconds;
    seek.disabled = false;
    updateTimeAndSeek();
  };

  const updateTimeAndSeek = () => {
    const duration = Number(audio.duration);
    const current = Math.max(0, Number(audio.currentTime || 0));
    const currentLabel = formatAudioDurationSeconds(current, true);
    const totalLabel =
      totalSeconds !== null ? formatAudioDurationSeconds(totalSeconds) : "--:--";

    if (!Number.isFinite(duration) || duration <= 0) {
      seek.value = "0";
      setRangeProgress(seek, 0, "--seek-progress");
      timeLabel.textContent = `${currentLabel} / ${totalLabel}`;
      return;
    }

    totalSeconds = sanitizeAttachmentDurationSeconds(duration) ?? totalSeconds;
    const ratio = Math.max(0, Math.min(1, current / duration));
    seek.value = String(Math.round(ratio * 1000));
    setRangeProgress(seek, ratio, "--seek-progress");
    timeLabel.textContent = `${currentLabel} / ${formatAudioDurationSeconds(totalSeconds ?? duration)}`;
  };

  const stopPlaybackTicker = () => {
    if (playbackRafId) {
      window.cancelAnimationFrame(playbackRafId);
      playbackRafId = 0;
    }
  };

  const startPlaybackTicker = () => {
    stopPlaybackTicker();
    playPrimingUntil = Date.now() + 700;

    const tick = () => {
      updateTimeAndSeek();

      if (!audio.ended && (!audio.paused || Date.now() < playPrimingUntil)) {
        playbackRafId = window.requestAnimationFrame(tick);
      } else {
        playbackRafId = 0;
      }
    };

    playbackRafId = window.requestAnimationFrame(tick);
  };

  const syncPlayButton = () => {
    const isPlaying = !audio.paused && !audio.ended;
    playButton.textContent = isPlaying ? "\u275A\u275A" : "\u25B6";
    playButton.setAttribute("aria-label", isPlaying ? "Pause audio" : "Play audio");
  };

  const syncMuteButton = () => {
    const muted = audio.muted || audio.volume === 0;
    const iconMode = muted ? "muted" : audio.volume < 0.5 ? "low" : "high";
    muteButton.innerHTML = volumeIconMarkup(iconMode);
    muteButton.setAttribute("aria-label", muted ? "Unmute audio" : "Mute audio");
    const normalizedVolume = Math.max(0, Math.min(1, Number(audio.volume) || 0));
    volumeSeek.value = String(Math.round(normalizedVolume * 100));
    setRangeProgress(volumeSeek, normalizedVolume, "--volume-progress");
  };

  playButton.addEventListener("click", async () => {
    if (audio.paused || audio.ended) {
      playButton.textContent = "\u275A\u275A";
      playButton.setAttribute("aria-label", "Pause audio");
      startPlaybackTicker();

      try {
        await audio.play();
      } catch {
        stopPlaybackTicker();
        syncPlayButton();
        updateTimeAndSeek();
        return;
      }
    } else {
      stopPlaybackTicker();
      audio.pause();
    }

    syncPlayButton();
    updateTimeAndSeek();
  });

  muteButton.addEventListener("click", () => {
    const currentlyMuted = audio.muted || audio.volume === 0;

    if (currentlyMuted) {
      const restored = Math.max(0.05, Math.min(1, Number(lastNonZeroVolume) || 1));
      audio.muted = false;
      audio.volume = restored;
      syncMuteButton();
      return;
    }

    lastNonZeroVolume = Math.max(0.05, Math.min(1, Number(audio.volume) || 1));
    audio.muted = true;
    audio.volume = 0;
    syncMuteButton();
  });

  volumeSeek.addEventListener("input", () => {
    const volume = Math.max(0, Math.min(1, Number(volumeSeek.value) / 100));
    audio.muted = volume === 0;
    audio.volume = volume;

    if (volume > 0) {
      lastNonZeroVolume = volume;
    }

    syncMuteButton();
  });

  seek.addEventListener("input", () => {
    const duration = Number(audio.duration);

    if (!Number.isFinite(duration) || duration <= 0) {
      return;
    }

    const ratio = Math.max(0, Math.min(1, Number(seek.value) / 1000));
    audio.currentTime = ratio * duration;
    updateTimeAndSeek();
  });

  audio.addEventListener("loadedmetadata", syncDuration);
  audio.addEventListener("durationchange", syncDuration);
  audio.addEventListener("timeupdate", updateTimeAndSeek);
  audio.addEventListener("play", () => {
    playPrimingUntil = 0;
    syncPlayButton();
    updateTimeAndSeek();
    startPlaybackTicker();
  });
  audio.addEventListener("pause", () => {
    syncPlayButton();
    updateTimeAndSeek();
    stopPlaybackTicker();
  });
  audio.addEventListener("ended", () => {
    audio.currentTime = 0;
    syncPlayButton();
    updateTimeAndSeek();
    stopPlaybackTicker();
  });
  audio.addEventListener("volumechange", syncMuteButton);

  syncPlayButton();
  syncMuteButton();
  setRangeProgress(seek, 0, "--seek-progress");
  updateTimeAndSeek();

  volumeWrap.append(muteButton, volumeSeek);
  player.append(playButton, timeLabel, seek, volumeWrap, audio);
  return player;
}

function renderMessageAttachments(message) {
  const attachments = Array.isArray(message.attachments) ? message.attachments : [];

  if (attachments.length === 0) {
    return null;
  }

  const wrapper = document.createElement("div");
  wrapper.className = "message-attachments";

  for (const attachment of attachments) {
    const type = String(attachment.type ?? "");
    const name = String(attachment.name ?? "file");
    const dataUrl = String(attachment.dataUrl ?? "");

    if (type.startsWith("image/")) {
      const image = document.createElement("img");
      image.className = "message-image";
      image.src = dataUrl;
      image.alt = name;
      wrapper.append(image);

      const link = document.createElement("a");
      link.className = "attachment-link";
      link.href = dataUrl;
      link.download = name;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = `Download ${name}`;
      wrapper.append(link);
      continue;
    }

    if (type.startsWith("audio/")) {
      wrapper.append(
        createMessageAudioPlayer(
          dataUrl,
          sanitizeAttachmentDurationSeconds(attachment?.durationSeconds)
        )
      );

      const voiceLink = document.createElement("a");
      voiceLink.className = "attachment-link";
      voiceLink.href = dataUrl;
      voiceLink.download = name;
      voiceLink.target = "_blank";
      voiceLink.rel = "noopener noreferrer";
      voiceLink.textContent = name.toLowerCase().startsWith("voice-note")
        ? "Download voice note"
        : `Download ${name}`;
      wrapper.append(voiceLink);
      continue;
    }

    if (type.startsWith("video/")) {
      const video = document.createElement("video");
      video.className = "message-video";
      video.src = dataUrl;
      video.controls = true;
      video.preload = "metadata";
      wrapper.append(video);

      const videoLink = document.createElement("a");
      videoLink.className = "attachment-link";
      videoLink.href = dataUrl;
      videoLink.download = name;
      videoLink.target = "_blank";
      videoLink.rel = "noopener noreferrer";
      videoLink.textContent = `Download ${name}`;
      wrapper.append(videoLink);
      continue;
    }

    const fileLink = document.createElement("a");
    fileLink.className = "attachment-link";
    fileLink.href = dataUrl;
    fileLink.download = name;
    fileLink.target = "_blank";
    fileLink.rel = "noopener noreferrer";
    fileLink.textContent = name;
    wrapper.append(fileLink);
  }

  return wrapper;
}

function isLikelyFileUrl(url) {
  const pathname = String(url?.pathname ?? "").toLowerCase();
  return /\.(pdf|zip|rar|7z|doc|docx|xls|xlsx|ppt|pptx|csv|txt|rtf|mp3|wav|m4a|ogg|mp4|mov|avi|mkv|jpg|jpeg|png|gif|webp|svg)$/i.test(
    pathname
  );
}

function trimUrlPunctuation(rawToken) {
  let token = String(rawToken ?? "");
  let trailing = "";

  while (token.length > 0 && /[),.!?:;]$/.test(token)) {
    trailing = token.slice(-1) + trailing;
    token = token.slice(0, -1);
  }

  return { token, trailing };
}

function buildClickableMessageText(textValue) {
  const fragment = document.createDocumentFragment();
  const source = String(textValue ?? "");
  const urlPattern = /\b((https?:\/\/|www\.)[^\s]+)/gi;

  let lastIndex = 0;
  let match;

  while ((match = urlPattern.exec(source)) !== null) {
    const start = match.index;
    const raw = match[0];
    const { token, trailing } = trimUrlPunctuation(raw);

    if (start > lastIndex) {
      fragment.append(document.createTextNode(source.slice(lastIndex, start)));
    }

    const hrefValue = token.startsWith("www.") ? `https://${token}` : token;
    let parsedUrl = null;

    try {
      parsedUrl = new URL(hrefValue);
    } catch {}

    if (parsedUrl && (parsedUrl.protocol === "http:" || parsedUrl.protocol === "https:")) {
      const link = document.createElement("a");
      link.className = "message-link";
      link.href = parsedUrl.href;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = token;

      if (isLikelyFileUrl(parsedUrl)) {
        link.setAttribute("download", "");
      }

      fragment.append(link);
    } else {
      fragment.append(document.createTextNode(raw));
    }

    if (trailing) {
      fragment.append(document.createTextNode(trailing));
    }

    lastIndex = start + raw.length;
  }

  if (lastIndex < source.length) {
    fragment.append(document.createTextNode(source.slice(lastIndex)));
  }

  return fragment;
}

function normalizeMessageLinkPreview(rawPreview) {
  if (!rawPreview || typeof rawPreview !== "object") {
    return null;
  }

  const rawUrl = String(rawPreview.url ?? "").trim();

  if (!rawUrl) {
    return null;
  }

  let parsedUrl = null;

  try {
    parsedUrl = new URL(rawUrl);
  } catch {
    return null;
  }

  if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
    return null;
  }

  const title = String(rawPreview.title ?? "").trim().slice(0, 160) || parsedUrl.hostname;
  const description = String(rawPreview.description ?? "").trim().slice(0, 260);
  const siteName = String(rawPreview.siteName ?? "").trim().slice(0, 90) || parsedUrl.hostname;
  const authorName = String(rawPreview.authorName ?? "").trim().slice(0, 90);
  const providerKeyRaw = String(rawPreview.providerKey ?? "").trim().toLowerCase();
  const host = parsedUrl.hostname.toLowerCase();
  let providerKey = providerKeyRaw;

  if (!providerKey) {
    if (host.includes("youtube") || host === "youtu.be") {
      providerKey = "youtube";
    } else if (host.includes("tiktok.com") || host === "vm.tiktok.com" || host === "vt.tiktok.com") {
      providerKey = "tiktok";
    } else {
      providerKey = "website";
    }
  }
  const imageUrlRaw = String(rawPreview.imageUrl ?? "").trim();
  let imageUrl = null;

  if (imageUrlRaw) {
    try {
      const parsedImageUrl = new URL(imageUrlRaw);

      if (parsedImageUrl.protocol === "http:" || parsedImageUrl.protocol === "https:") {
        imageUrl = parsedImageUrl.toString();
      }
    } catch {}
  }

  if (!imageUrl && providerKey === "youtube") {
    const videoId = youtubeVideoIdFromUrl(parsedUrl.toString());

    if (videoId) {
      imageUrl = `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`;
    }
  }

  let resolvedAuthorName = authorName || null;

  if (providerKey === "tiktok") {
    const parsedUsername = tiktokUsernameFromUrl(parsedUrl.toString());

    if (!resolvedAuthorName && parsedUsername) {
      resolvedAuthorName = parsedUsername;
    }

    if (resolvedAuthorName && !resolvedAuthorName.startsWith("@")) {
      resolvedAuthorName = `@${resolvedAuthorName}`;
    }
  }

  return {
    url: parsedUrl.toString(),
    title,
    description: description || null,
    siteName,
    imageUrl,
    authorName: resolvedAuthorName,
    providerKey
  };
}

function linkPreviewAccentColor(providerKey) {
  switch (String(providerKey ?? "").trim().toLowerCase()) {
    case "youtube":
      return "#f23f43";
    case "tiktok":
      return "#25f4ee";
    case "twitter":
      return "#1d9bf0";
    case "github":
      return "#a8a8a8";
    default:
      return "#72767d";
  }
}

function normalizePreviewLabel(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/+$/, "");
}

function linkPreviewProviderLabel(preview) {
  const providerKey = String(preview?.providerKey ?? "").trim().toLowerCase();

  switch (providerKey) {
    case "youtube":
      return "YouTube";
    case "tiktok":
      return "TikTok";
    case "twitter":
      return "X";
    case "github":
      return "GitHub";
    default:
      return String(preview?.siteName ?? "").trim();
  }
}

function looksLikeHostOrDomainLabel(value) {
  const normalized = normalizePreviewLabel(value);

  if (!normalized) {
    return true;
  }

  if (!normalized.includes(".")) {
    return false;
  }

  return /^[a-z0-9.-]+\.[a-z]{2,}(\/.*)?$/i.test(normalized);
}

function linkPreviewDisplayTitle(preview, providerLabel, hostLabel) {
  const provider = String(preview?.providerKey ?? "").trim().toLowerCase();
  const rawTitle = String(preview?.title ?? "").trim();
  const normalizedTitle = normalizePreviewLabel(rawTitle);
  const normalizedHost = normalizePreviewLabel(hostLabel);
  const normalizedSite = normalizePreviewLabel(preview?.siteName ?? "");

  if (provider === "youtube" || provider === "tiktok") {
    if (
      rawTitle &&
      !looksLikeHostOrDomainLabel(rawTitle) &&
      normalizedTitle !== normalizePreviewLabel(providerLabel)
    ) {
      return rawTitle;
    }

    const description = String(preview?.description ?? "").trim();

    if (description && !looksLikeHostOrDomainLabel(description)) {
      return description;
    }

    return provider === "youtube" ? "YouTube video" : "TikTok video";
  }

  if (
    !rawTitle ||
    normalizedTitle === normalizedHost ||
    normalizedTitle === normalizedSite ||
    normalizedTitle === normalizePreviewLabel(providerLabel)
  ) {
    return providerLabel ? `Open on ${providerLabel}` : rawTitle || hostLabel;
  }

  return rawTitle;
}

function normalizedUrlPathSegments(pathname) {
  return String(pathname ?? "")
    .split("/")
    .map((part) => part.trim())
    .filter(Boolean);
}

function youtubeVideoIdFromUrl(rawUrl) {
  try {
    const parsed = new URL(String(rawUrl ?? ""));
    const host = parsed.hostname.toLowerCase();
    const segments = normalizedUrlPathSegments(parsed.pathname);
    const idPattern = /^[A-Za-z0-9_-]{6,20}$/;

    if (host === "youtu.be") {
      const candidate = segments[0] ?? "";
      return idPattern.test(candidate) ? candidate : null;
    }

    if (!host.includes("youtube")) {
      return null;
    }

    if (segments[0] === "watch") {
      const candidate = String(parsed.searchParams.get("v") ?? "").trim();
      return idPattern.test(candidate) ? candidate : null;
    }

    if (segments[0] === "shorts" || segments[0] === "live" || segments[0] === "embed") {
      const candidate = segments[1] ?? "";
      return idPattern.test(candidate) ? candidate : null;
    }

    return null;
  } catch {
    return null;
  }
}

function tiktokVideoIdFromUrl(rawUrl) {
  try {
    const parsed = new URL(String(rawUrl ?? ""));
    const host = parsed.hostname.toLowerCase();

    if (!host.includes("tiktok.com")) {
      return null;
    }

    const segments = normalizedUrlPathSegments(parsed.pathname);
    const videoIndex = segments.findIndex((segment) => segment.toLowerCase() === "video");

    if (videoIndex >= 0 && videoIndex + 1 < segments.length) {
      const candidate = segments[videoIndex + 1];
      return /^\d{6,}$/.test(candidate) ? candidate : null;
    }

    const match = parsed.pathname.match(/\/video\/(\d{6,})/i);
    return match ? match[1] : null;
  } catch {
    return null;
  }
}

function tiktokUsernameFromUrl(rawUrl) {
  try {
    const parsed = new URL(String(rawUrl ?? ""));
    const host = parsed.hostname.toLowerCase();

    if (!host.includes("tiktok.com")) {
      return null;
    }

    const segments = normalizedUrlPathSegments(parsed.pathname);
    const username = segments.find((segment) => segment.startsWith("@")) ?? null;

    if (!username || username.length < 2) {
      return null;
    }

    return username;
  } catch {
    return null;
  }
}

function linkPreviewEmbedInfo(preview) {
  if (!preview || typeof preview !== "object") {
    return null;
  }

  const provider = String(preview.providerKey ?? "").trim().toLowerCase();

  if (provider === "youtube") {
    const videoId = youtubeVideoIdFromUrl(preview.url);

    if (!videoId) {
      return null;
    }

    const params = new URLSearchParams({
      autoplay: "1",
      rel: "0",
      modestbranding: "1",
      playsinline: "1"
    });
    const pageOrigin = String(window.location?.origin ?? "").trim();

    if (/^https?:\/\//i.test(pageOrigin)) {
      params.set("origin", pageOrigin);
      params.set("widget_referrer", pageOrigin);
    }

    return {
      provider,
      embedUrl: `https://www.youtube.com/embed/${videoId}?${params.toString()}`
    };
  }

  if (provider === "tiktok") {
    const videoId = tiktokVideoIdFromUrl(preview.url);

    if (!videoId) {
      return null;
    }

    const primaryParams = new URLSearchParams({
      autoplay: "1",
      controls: "1",
      description: "0",
      music_info: "0",
      progress_bar: "1",
      timestamp: "1",
      rel: "0"
    });
    const fallbackParams = new URLSearchParams({
      autoplay: "1",
      controls: "1",
      description: "0",
      music_info: "0",
      rel: "0"
    });

    return {
      provider,
      embedUrl: `https://www.tiktok.com/player/v1/${videoId}?${primaryParams.toString()}`,
      fallbackEmbedUrl: `https://www.tiktok.com/embed/v2/${videoId}?${fallbackParams.toString()}`
    };
  }

  return null;
}

function buildLinkPreviewEmbedIframe(embedInfo, preview) {
  if (!embedInfo?.embedUrl) {
    return null;
  }

  const iframe = document.createElement("iframe");
  iframe.className = "message-link-preview-iframe";
  iframe.src = embedInfo.embedUrl;
  iframe.loading = "eager";
  iframe.referrerPolicy = "strict-origin-when-cross-origin";
  iframe.allowFullscreen = true;
  iframe.title = `${preview?.siteName || "Link"} video preview`;

  if (embedInfo.provider === "youtube") {
    iframe.setAttribute(
      "allow",
      "accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
    );
  } else if (embedInfo.provider === "tiktok") {
    iframe.setAttribute(
      "allow",
      "autoplay; clipboard-write; encrypted-media; picture-in-picture; web-share"
    );
  } else {
    iframe.setAttribute("allow", "autoplay; encrypted-media; picture-in-picture");
  }

  return iframe;
}

function renderMessageLinkPreview(message) {
  const preview = normalizeMessageLinkPreview(message?.linkPreview);

  if (!preview || message?.deleted) {
    return null;
  }

  const card = document.createElement("article");
  card.className = "message-link-preview";
  card.classList.add(`provider-${preview.providerKey}`);
  card.style.setProperty("--preview-accent", linkPreviewAccentColor(preview.providerKey));

  let hostLabel = "";

  try {
    hostLabel = new URL(preview.url).hostname;
  } catch {}

  const providerLabel = linkPreviewProviderLabel(preview) || hostLabel;
  const titleText = linkPreviewDisplayTitle(preview, providerLabel, hostLabel);
  const isVideoProvider = preview.providerKey === "youtube" || preview.providerKey === "tiktok";
  const hideSiteLine =
    isVideoProvider ||
    normalizePreviewLabel(providerLabel) === normalizePreviewLabel(titleText) ||
    normalizePreviewLabel(titleText) === normalizePreviewLabel(`open on ${providerLabel}`);

  const body = document.createElement("div");
  body.className = "message-link-preview-body";

  if (!hideSiteLine) {
    const site = document.createElement("p");
    site.className = "message-link-preview-site";
    site.textContent = providerLabel || preview.siteName;
    body.append(site);
  }

  if (preview.authorName) {
    const author = document.createElement("p");
    author.className = "message-link-preview-author";
    author.textContent = preview.authorName;
    body.append(author);
  }

  const title = document.createElement("a");
  title.className = "message-link-preview-title";
  title.textContent = titleText;
  title.href = preview.url;
  title.target = "_blank";
  title.rel = "noopener noreferrer";

  if (preview.providerKey === "youtube" || preview.providerKey === "tiktok") {
    title.classList.add("accent");
  }
  body.append(title);

  if (preview.description) {
    const description = document.createElement("p");
    description.className = "message-link-preview-description";
    description.textContent = preview.description;
    body.append(description);
  }

  const embedInfo = linkPreviewEmbedInfo(preview);

  if (preview.imageUrl || embedInfo) {
    const media = document.createElement("div");
    media.className = "message-link-preview-media";

    if (embedInfo?.provider === "tiktok") {
      media.classList.add("tiktok");
    }

    if (preview.imageUrl) {
      const image = document.createElement("img");
      image.className = "message-link-preview-image";
      image.src = preview.imageUrl;
      image.alt = preview.title;
      image.loading = "lazy";
      image.decoding = "async";
      image.referrerPolicy = "no-referrer";
      image.addEventListener("error", () => {
        image.remove();
      });
      media.append(image);
    }

    if (embedInfo) {
      const playButton = document.createElement("button");
      playButton.type = "button";
      playButton.className = "message-link-preview-play";
      playButton.setAttribute("aria-label", "Play video preview");
      playButton.innerHTML =
        '<span class="message-link-preview-play-icon" aria-hidden="true">\u25B6</span>';

      playButton.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();

        if (media.dataset.embedActive === "1") {
          return;
        }

        const tryMountEmbed = (embedUrl, isFallbackAttempt) => {
          const iframe = buildLinkPreviewEmbedIframe(
            {
              ...embedInfo,
              embedUrl
            },
            preview
          );

          if (!iframe) {
            return;
          }

          media.dataset.embedActive = "1";
          media.classList.add("loading");
          playButton.disabled = true;

          let settled = false;
          const loadTimeoutMs = embedInfo.provider === "tiktok" ? 9000 : 6500;

          const failAttempt = (errorMessage) => {
            if (settled) {
              return;
            }

            settled = true;
            iframe.remove();
            media.classList.remove("loading");

            const hasFallback =
              embedInfo.provider === "tiktok" &&
              !isFallbackAttempt &&
              String(embedInfo.fallbackEmbedUrl ?? "").trim().length > 0;

            if (hasFallback) {
              tryMountEmbed(embedInfo.fallbackEmbedUrl, true);
              return;
            }

            media.dataset.embedActive = "0";
            playButton.disabled = false;
            showToast(errorMessage);
          };

          const loadTimeout = window.setTimeout(() => {
            failAttempt("Embedded video is taking too long. Open with the title link instead.");
          }, loadTimeoutMs);

          iframe.addEventListener("load", () => {
            if (settled) {
              return;
            }

            settled = true;
            clearTimeout(loadTimeout);
            media.classList.remove("loading");
            media.classList.add("playing");
          });

          iframe.addEventListener("error", () => {
            clearTimeout(loadTimeout);
            failAttempt("Could not load embedded video.");
          });

          media.append(iframe);
        };

        tryMountEmbed(embedInfo.embedUrl, false);
      });

      media.append(playButton);
    }

    body.append(media);
  }

  card.append(body);
  return card;
}

function addRoundedRectPath(context, x, y, width, height, radius) {
  const normalizedRadius = Math.max(0, Math.min(radius, Math.floor(Math.min(width, height) / 2)));
  context.beginPath();
  context.moveTo(x + normalizedRadius, y);
  context.lineTo(x + width - normalizedRadius, y);
  context.quadraticCurveTo(x + width, y, x + width, y + normalizedRadius);
  context.lineTo(x + width, y + height - normalizedRadius);
  context.quadraticCurveTo(x + width, y + height, x + width - normalizedRadius, y + height);
  context.lineTo(x + normalizedRadius, y + height);
  context.quadraticCurveTo(x, y + height, x, y + height - normalizedRadius);
  context.lineTo(x, y + normalizedRadius);
  context.quadraticCurveTo(x, y, x + normalizedRadius, y);
  context.closePath();
}

function drawMediaCover(context, media, x, y, width, height) {
  const sourceWidth = Math.max(1, Number(media?.videoWidth) || width);
  const sourceHeight = Math.max(1, Number(media?.videoHeight) || height);
  const sourceAspect = sourceWidth / sourceHeight;
  const targetAspect = width / height;
  let sx = 0;
  let sy = 0;
  let sw = sourceWidth;
  let sh = sourceHeight;

  if (sourceAspect > targetAspect) {
    sw = Math.max(1, Math.floor(sourceHeight * targetAspect));
    sx = Math.max(0, Math.floor((sourceWidth - sw) / 2));
  } else if (sourceAspect < targetAspect) {
    sh = Math.max(1, Math.floor(sourceWidth / targetAspect));
    sy = Math.max(0, Math.floor((sourceHeight - sh) / 2));
  }

  context.drawImage(media, sx, sy, sw, sh, x, y, width, height);
}

function voteCloseTempChat() {
  if (!state.activeChat || state.activeChat.type !== "temp") {
    return;
  }

  socket.emit("vote_close_temp_chat", { chatId: state.activeChat.id }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not request close.");
    }
  });
}

function renderSystemMessage(chat, message, latestCloseRequestId) {
  const card = document.createElement("article");
  card.className = "system-message";
  const isCallLog = String(message.kind ?? "").startsWith("call_");

  if (isCallLog) {
    card.classList.add("system-message-call-log");
  }

  const text = document.createElement("p");
  text.className = "system-text";

  if (isCallLog) {
    text.classList.add("system-text-call-log");
  }

  text.textContent = message.text;
  card.append(text);

  if (message.kind !== "temp_close_request") {
    return card;
  }

  const isLatest = message.id === latestCloseRequestId;
  const closeVotes = Array.isArray(chat.closeVotes) ? chat.closeVotes : [];
  const pending = closeVotes.length > 0;

  if (!isLatest || !pending) {
    return card;
  }

  const actions = document.createElement("div");
  actions.className = "system-actions";

  const agreeButton = document.createElement("button");
  agreeButton.type = "button";
  agreeButton.className = "action-button";
  agreeButton.textContent = closeVotes.includes(state.currentUser.key)
    ? "Waiting for other member"
    : "Agree and Close Temp";
  agreeButton.disabled = closeVotes.includes(state.currentUser.key);
  agreeButton.addEventListener("click", () => {
    voteCloseTempChat();
  });

  actions.append(agreeButton);
  card.append(actions);

  return card;
}

function renderMessages(chat) {
  elements.messages.innerHTML = "";
  state.activeUnreadAnchorMessageId = null;

  if (!chat || chat.messages.length === 0) {
    const placeholder = document.createElement("div");
    placeholder.className = "placeholder";
    placeholder.textContent = chat
      ? "No messages yet."
      : "Select a friend or existing chat to start messaging.";
    elements.messages.append(placeholder);
    updateJumpToUnreadButton();
    return;
  }

  const normalizedQuery = state.messageSearchQuery.trim().toLowerCase();
  const messagesToRender = normalizedQuery
    ? chat.messages.filter((message) => messageMatchesSearch(message, normalizedQuery))
    : chat.messages;

  if (messagesToRender.length === 0) {
    const placeholder = document.createElement("div");
    placeholder.className = "placeholder";
    placeholder.textContent = `No messages found for "${state.messageSearchQuery.trim()}".`;
    elements.messages.append(placeholder);
    updateJumpToUnreadButton();
    return;
  }

  const unreadAnchorMessageId = normalizedQuery ? null : resolveUnreadAnchorForChat(chat);
  state.activeUnreadAnchorMessageId = unreadAnchorMessageId;
  let unreadDividerRendered = false;

  let latestCloseRequestId = null;

  if (chat.type === "temp") {
    for (let index = chat.messages.length - 1; index >= 0; index -= 1) {
      const message = chat.messages[index];
      if (message.kind === "temp_close_request") {
        latestCloseRequestId = message.id;
        break;
      }
    }
  }

  for (const message of messagesToRender) {
    if (!unreadDividerRendered && unreadAnchorMessageId && message.id === unreadAnchorMessageId) {
      elements.messages.append(createUnreadDivider());
      unreadDividerRendered = true;
    }

    if (message.senderKey === "system" || message.kind) {
      elements.messages.append(renderSystemMessage(chat, message, latestCloseRequestId));
      continue;
    }

    const card = document.createElement("article");
    card.dataset.messageId = message.id;
    card.className = `message ${
      message.senderKey === state.currentUser.key ? "self" : "other"
    }`;

    const meta = document.createElement("div");
    meta.className = "message-meta";
    const sentTime = formatMessageTime(message.sentAt);
    const editedSuffix = message.deleted ? "" : message.editedAt ? " (edited)" : "";
    meta.textContent = `${message.senderName} - ${sentTime}${editedSuffix}`;

    card.append(meta);

    const replyReference = resolveReplyReference(chat, message);
    if (replyReference) {
      const replyButton = document.createElement("button");
      replyButton.type = "button";
      replyButton.className = "reply-reference";

      const replyName = document.createElement("span");
      replyName.className = "reply-reference-name";
      replyName.textContent = replyReference.senderName;

      const replyText = document.createElement("span");
      replyText.className = "reply-reference-text";
      replyText.textContent = replyReference.preview;

      replyButton.append(replyName, replyText);
      replyButton.addEventListener("click", () => {
        if (replyReference.messageId) {
          scrollToMessage(replyReference.messageId);
        }
      });
      card.append(replyButton);
    }

    if (message.deleted) {
      const deleted = document.createElement("p");
      deleted.className = "message-deleted";
      deleted.textContent = "Message deleted";
      card.append(deleted);
    } else if (message.text) {
      const text = document.createElement("div");
      text.className = "message-text";
      text.append(buildClickableMessageText(message.text));
      card.append(text);
    }

    if (!message.deleted) {
      const linkPreview = renderMessageLinkPreview(message);

      if (linkPreview) {
        card.append(linkPreview);
      }
    }

    if (!message.deleted) {
      const attachments = renderMessageAttachments(message);
      if (attachments) {
        card.append(attachments);
      }
    }

    const reactions = renderReactions(chat, message);
    if (reactions) {
      card.append(reactions);
    }

    const readReceipt = renderReadReceipt(chat, message);
    if (readReceipt) {
      card.append(readReceipt);
    }

    if (message.senderKey !== "system") {
      const openMenu = (x, y) => {
        showMessageActions(chat.id, message, x, y);
      };

      bindLongPress(card, openMenu);
      card.addEventListener("contextmenu", (event) => {
        event.preventDefault();
        openMenu(event.clientX, event.clientY);
      });
    }

    elements.messages.append(card);
  }

  if (!normalizedQuery) {
    elements.messages.scrollTop = elements.messages.scrollHeight;
  }

  updateJumpToUnreadButton();
}
function renderAttachmentPreview() {
  elements.attachmentPreview.innerHTML = "";

  if (state.pendingAttachments.length === 0) {
    elements.attachmentPreview.classList.add("hidden");
    return;
  }

  elements.attachmentPreview.classList.remove("hidden");

  for (const attachment of state.pendingAttachments) {
    const item = document.createElement("div");
    item.className = "attachment-chip";
    const preparing = attachment.isPreparing === true;
    const progress = Math.max(0, Math.min(100, Number(attachment.progress) || 0));

    if (preparing || state.composerSending) {
      item.classList.add("with-progress");
    }

    if (
      String(attachment.type ?? "").startsWith("image/") &&
      String(attachment.dataUrl ?? "").startsWith("data:")
    ) {
      const thumb = document.createElement("img");
      thumb.src = attachment.dataUrl;
      thumb.alt = attachment.name;
      thumb.className = "attachment-thumb";
      item.append(thumb);
    }

    const name = document.createElement("span");
    name.textContent = attachment.name;

    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "chip-remove";
    remove.textContent = "x";
    remove.disabled = state.composerSending;
    remove.addEventListener("click", () => {
      state.pendingAttachments = state.pendingAttachments.filter(
        (entry) => entry.id !== attachment.id
      );
      renderAttachmentPreview();
    });

    item.append(name, remove);

    if (preparing || state.composerSending) {
      const status = document.createElement("p");
      status.className = "attachment-progress-label";
      status.textContent = state.composerSending
        ? "Sending..."
        : `Preparing ${Math.max(0, Math.min(100, progress))}%`;
      item.append(status);

      const progressTrack = document.createElement("div");
      progressTrack.className = "attachment-progress-track";
      const progressFill = document.createElement("div");
      progressFill.className = "attachment-progress-fill";
      progressFill.style.width = `${state.composerSending ? 100 : progress}%`;
      progressTrack.append(progressFill);
      item.append(progressTrack);
    }

    elements.attachmentPreview.append(item);
  }
}

function clearPendingAttachments() {
  state.composerSending = false;
  state.pendingAttachments = [];
  elements.fileInput.value = "";
  elements.photoInput.value = "";
  elements.cameraInput.value = "";
  renderAttachmentPreview();
}

function renderActiveChat() {
  const chat = state.activeChat;
  hideGroupQuickPopover();

  if (!chat || localTypingChatId !== chat.id) {
    stopLocalTyping();
  }

  if (state.voiceNoteRecorder && state.voiceNoteChatId && state.voiceNoteChatId !== chat?.id) {
    stopVoiceNoteRecording(false, true);
  }

  if (!chat) {
    activeComposerDraftChatId = null;
    elements.messageSearchInput.disabled = true;
    elements.chatTitle.textContent = "Select a chat";
    elements.chatSubtitle.textContent = "Tap a friend to start chatting.";
    setComposerEnabled(false);
    elements.messageInput.value = "";
    resizeComposerInput();
    clearPendingAttachments();
    clearReplyTarget();
    renderTypingIndicator();
    renderMessages(null);
    renderCallChat();
    updateHeaderActions();
    renderProfilePanel();
    applyActiveChatAppearance();
    updateVoiceNoteButton();
    return;
  }

  elements.messageSearchInput.disabled = false;
  const prefs = activeChatViewerPrefs();
  const hasNickname = Boolean(prefs.nickname);

  if (chat.type === "group") {
    elements.chatTitle.textContent = hasNickname ? prefs.nickname : `${chat.name || "Group chat"} (Group)`;
    elements.chatSubtitle.textContent = `Group chat with ${chat.participants.length} members`;
  } else {
    const target = currentDirectTarget();
    if (chat.type === "temp") {
      elements.chatTitle.textContent = hasNickname
        ? prefs.nickname
        : target
          ? `${target.name} (Temp)`
          : "Temp chat";
    } else {
      elements.chatTitle.textContent = hasNickname ? prefs.nickname : target ? target.name : "Direct chat";
    }
    elements.chatSubtitle.textContent =
      chat.type === "temp"
        ? "Temp chat: both users must agree to close."
        : "Direct messages";

    if (target) {
      setProfileUser(target.key);
    }
  }

  if (
    state.replyingTo &&
    !chat.messages.some((message) => message.id === state.replyingTo.messageId)
  ) {
    clearReplyTarget();
  } else {
    renderReplyPreview();
  }

  setComposerEnabled(true);
  if (activeComposerDraftChatId !== chat.id) {
    restoreDraftForActiveChat();
    activeComposerDraftChatId = chat.id;
  }
  applyActiveChatAppearance();
  renderMessages(chat);
  renderTypingIndicator();
  renderCallChat();
  updateHeaderActions();
  renderProfilePanel();
  markActiveChatRead();
  updateVoiceNoteButton();
}

function readFileAsDataUrl(file, onProgress) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();

    if (typeof onProgress === "function") {
      onProgress(0);
      reader.onprogress = (event) => {
        if (!event.lengthComputable) {
          return;
        }

        const ratio = event.total > 0 ? event.loaded / event.total : 0;
        const percentage = Math.max(0, Math.min(100, Math.round(ratio * 100)));
        onProgress(percentage);
      };
    }

    reader.onload = () => resolve(String(reader.result ?? ""));
    reader.onerror = () => reject(new Error("Could not read file."));
    reader.onloadend = () => {
      if (typeof onProgress === "function") {
        onProgress(100);
      }
    };
    reader.readAsDataURL(file);
  });
}

function serializeAttachmentsForSend(attachments) {
  const source = Array.isArray(attachments) ? attachments : [];
  return source
    .filter((attachment) => {
      return (
        attachment &&
        !attachment.isPreparing &&
        String(attachment.dataUrl ?? "").startsWith("data:")
      );
    })
    .map((attachment) => ({
      name: attachment.name,
      type: attachment.type,
      size: attachment.size,
      dataUrl: attachment.dataUrl,
      durationSeconds: sanitizeAttachmentDurationSeconds(attachment.durationSeconds)
    }));
}

function sendMessageToActiveChat({
  chatId = null,
  text = "",
  attachments = [],
  replyToMessageId = null,
  clearComposerOnSuccess = false,
  onSuccess,
  onFailure
} = {}) {
  const targetChatId = String(chatId ?? state.activeChat?.id ?? "").trim();

  if (!targetChatId) {
    showToast("Open a chat first.");
    return Promise.resolve(false);
  }

  const outgoingText = String(text ?? "").trim();
  const pendingAttachments = Array.isArray(attachments) ? attachments : [];
  const preparingCount = pendingAttachments.filter((attachment) => attachment?.isPreparing).length;

  if (preparingCount > 0) {
    showToast("Please wait for attachments to finish uploading.");
    return Promise.resolve(false);
  }

  const outgoingAttachments = serializeAttachmentsForSend(attachments);

  if (!outgoingText && outgoingAttachments.length === 0) {
    return Promise.resolve(false);
  }

  const shouldShowComposerSendingState =
    clearComposerOnSuccess &&
    state.activeChat?.id === targetChatId &&
    outgoingAttachments.length > 0 &&
    !chatId;

  if (shouldShowComposerSendingState) {
    state.composerSending = true;
    renderAttachmentPreview();
  }

  return new Promise((resolve) => {
    socket.emit(
      "send_message",
      {
        chatId: targetChatId,
        text: outgoingText,
        attachments: outgoingAttachments,
        replyToMessageId: replyToMessageId ?? null
      },
      (response) => {
        if (shouldShowComposerSendingState) {
          state.composerSending = false;
          renderAttachmentPreview();
        }

        if (!response?.ok) {
          showToast(response?.error ?? "Failed to send message.");

          if (typeof onFailure === "function") {
            onFailure(response);
          }

          resolve(false);
          return;
        }

        const shouldClearComposer =
          clearComposerOnSuccess && state.activeChat?.id === targetChatId;

        if (shouldClearComposer) {
          const activeChatId = state.activeChat?.id;
          elements.messageInput.value = "";
          clearStoredDraft(activeChatId);
          resizeComposerInput();
          stopLocalTyping();
          clearReplyTarget();
          clearPendingAttachments();
          closeStickerMenu();
          closeAttachMenu();
        }

        if (typeof onSuccess === "function") {
          onSuccess(response);
        }

        resolve(true);
      }
    );
  });
}

function submitComposerMessage() {
  return sendMessageToActiveChat({
    text: elements.messageInput.value,
    attachments: state.pendingAttachments,
    replyToMessageId: state.replyingTo?.messageId ?? null,
    clearComposerOnSuccess: true
  });
}

async function createAttachmentFromFile(file, options = {}) {
  const attachmentId = String(options?.id ?? crypto.randomUUID());
  const progressHandler =
    typeof options?.onProgress === "function" ? options.onProgress : null;
  const durationSeconds = sanitizeAttachmentDurationSeconds(options?.durationSeconds);
  const dataUrl = await readFileAsDataUrl(file, (progressValue) => {
    if (!progressHandler) {
      return;
    }

    progressHandler(attachmentId, progressValue);
  });

  return {
    id: attachmentId,
    name: file.name || "file",
    type: file.type || "application/octet-stream",
    size: file.size,
    dataUrl,
    durationSeconds
  };
}

async function addSelectedFiles(fileList, options = {}) {
  const sendImmediately = options.sendImmediately === true;
  const files = [...fileList];

  if (files.length === 0) {
    return [];
  }

  if (files.length > 4 || (!sendImmediately && state.pendingAttachments.length + files.length > 4)) {
    showToast("You can attach up to 4 files at once.");
    return [];
  }

  const preparedAttachments = [];

  for (const file of files) {
    if (file.size > 5 * 1024 * 1024) {
      showToast(`${file.name} is larger than 5 MB.`);
      continue;
    }

    const attachmentId = crypto.randomUUID();
    let pendingEntry = null;

    if (!sendImmediately) {
      pendingEntry = {
        id: attachmentId,
        name: file.name || "file",
        type: file.type || "application/octet-stream",
        size: file.size,
        dataUrl: "",
        isPreparing: true,
        progress: 0
      };
      state.pendingAttachments.push(pendingEntry);
      renderAttachmentPreview();
    }

    try {
      const prepared = await createAttachmentFromFile(file, {
        id: attachmentId,
        onProgress: (id, progressValue) => {
          if (sendImmediately) {
            return;
          }

          const target = state.pendingAttachments.find((entry) => entry.id === id);

          if (!target) {
            return;
          }

          target.progress = Math.max(0, Math.min(100, Number(progressValue) || 0));
          renderAttachmentPreview();
        }
      });

      if (sendImmediately) {
        preparedAttachments.push(prepared);
      } else {
        const target = state.pendingAttachments.find((entry) => entry.id === attachmentId);

        if (!target) {
          continue;
        }

        Object.assign(target, prepared, {
          isPreparing: false,
          progress: 100
        });
        preparedAttachments.push(prepared);
        renderAttachmentPreview();
      }
    } catch {
      if (!sendImmediately) {
        state.pendingAttachments = state.pendingAttachments.filter(
          (entry) => entry.id !== attachmentId
        );
        renderAttachmentPreview();
      }

      showToast(`Could not attach ${file.name}.`);
    }
  }

  if (preparedAttachments.length === 0) {
    return [];
  }

  if (sendImmediately) {
    await sendMessageToActiveChat({
      text: "",
      attachments: preparedAttachments,
      replyToMessageId: null,
      clearComposerOnSuccess: false
    });
    return preparedAttachments;
  }

  return preparedAttachments;
}

function voiceNoteSupported() {
  return Boolean(window.MediaRecorder && navigator.mediaDevices?.getUserMedia);
}

function preferredVoiceNoteMimeType() {
  if (!window.MediaRecorder || typeof MediaRecorder.isTypeSupported !== "function") {
    return "";
  }

  const candidates = ["audio/webm;codecs=opus", "audio/webm", "audio/ogg;codecs=opus"];

  for (const candidate of candidates) {
    if (MediaRecorder.isTypeSupported(candidate)) {
      return candidate;
    }
  }

  return "";
}

function voiceNoteExtensionFromMimeType(mimeType) {
  const normalized = String(mimeType ?? "").toLowerCase();

  if (normalized.includes("ogg")) {
    return "ogg";
  }

  if (normalized.includes("mp4")) {
    return "mp4";
  }

  return "webm";
}

function formatVoiceNoteDuration(milliseconds) {
  const totalSeconds = Math.max(0, Math.floor(Number(milliseconds || 0) / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function stopVoiceNoteTracks() {
  const stream = state.voiceNoteStream;

  if (!stream) {
    return;
  }

  for (const track of stream.getTracks()) {
    track.stop();
  }
}

function normalizeVoiceNoteMimeType(value) {
  const normalized = String(value ?? "").toLowerCase();

  if (normalized.includes("ogg")) {
    return "audio/ogg";
  }

  if (normalized.includes("mp4") || normalized.includes("m4a")) {
    return "audio/mp4";
  }

  return "audio/webm";
}

function resetVoiceNoteState({ preserveFinalizing = false } = {}) {
  clearVoiceNoteDurationTimer();
  stopVoiceNoteTracks();
  state.voiceNoteStream = null;
  state.voiceNoteRecorder = null;
  state.voiceNoteChunks = [];
  state.voiceNoteMimeType = "";
  state.voiceNoteStartedAt = 0;
  state.voiceNoteChatId = null;

  if (!preserveFinalizing) {
    state.voiceNoteFinalizing = false;
  }

  updateVoiceNoteButton();
}

function updateVoiceNoteButton() {
  if (!elements.attachVoiceNoteButton) {
    return;
  }

  if (!voiceNoteSupported()) {
    elements.attachVoiceNoteButton.disabled = true;
    elements.attachVoiceNoteButton.textContent = "Voice N/A";
    elements.attachVoiceNoteButton.classList.remove("recording");
    return;
  }

  if (state.voiceNoteFinalizing) {
    elements.attachVoiceNoteButton.disabled = true;
    elements.attachVoiceNoteButton.textContent = "Sending voice...";
    elements.attachVoiceNoteButton.classList.remove("recording");
    return;
  }

  const enabled = Boolean(state.currentUser && state.activeChat && !elements.messageInput.disabled);
  const isRecording = Boolean(state.voiceNoteRecorder);
  elements.attachVoiceNoteButton.disabled = !enabled && !isRecording;

  if (!isRecording) {
    elements.attachVoiceNoteButton.textContent = "Voice Note";
    elements.attachVoiceNoteButton.classList.remove("recording");
    return;
  }

  const elapsed = Date.now() - Number(state.voiceNoteStartedAt || Date.now());
  elements.attachVoiceNoteButton.textContent = `Stop Voice ${formatVoiceNoteDuration(elapsed)}`;
  elements.attachVoiceNoteButton.classList.add("recording");
}

async function finalizeVoiceNoteRecording(chunks, mimeType, chatId, durationSeconds = null) {
  const safeChunks = Array.isArray(chunks) ? chunks : [];

  if (safeChunks.length === 0) {
    showToast("No voice note captured.");
    state.voiceNoteFinalizing = false;
    updateVoiceNoteButton();
    return;
  }

  const normalizedMimeType = normalizeVoiceNoteMimeType(mimeType);
  const blob = new Blob(safeChunks, {
    type: normalizedMimeType
  });

  if (blob.size > VOICE_NOTE_MAX_SIZE_BYTES) {
    showToast("Voice note is larger than 5 MB.");
    state.voiceNoteFinalizing = false;
    updateVoiceNoteButton();
    return;
  }

  const file = new File([blob], `voice-note-${Date.now()}.${voiceNoteExtensionFromMimeType(blob.type)}`, {
    type: normalizeVoiceNoteMimeType(blob.type)
  });

  try {
    const attachment = await createAttachmentFromFile(file, {
      durationSeconds
    });
    const sent = await sendMessageToActiveChat({
      chatId,
      text: "",
      attachments: [attachment],
      replyToMessageId: null,
      clearComposerOnSuccess: false
    });

    if (sent) {
      showToast("Voice note sent.");
    }
  } catch {
    showToast("Could not send voice note.");
  } finally {
    state.voiceNoteFinalizing = false;
    updateVoiceNoteButton();
  }
}

function stopVoiceNoteRecording(send = true, showCanceledToast = false) {
  const recorder = state.voiceNoteRecorder;

  if (!recorder) {
    return;
  }

  const shouldSend = Boolean(send);
  const mimeType = state.voiceNoteMimeType;
  const chatId = state.voiceNoteChatId;
  const startedAtTimestamp = Number(state.voiceNoteStartedAt || Date.now());
  const elapsedSeconds = Math.max(0, (Date.now() - startedAtTimestamp) / 1000);
  const estimatedDurationSeconds = sanitizeAttachmentDurationSeconds(elapsedSeconds);
  state.voiceNoteFinalizing = shouldSend;
  clearVoiceNoteDurationTimer();
  updateVoiceNoteButton();
  let handled = false;

  const finalizeOnce = () => {
    if (handled) {
      return;
    }

    handled = true;
    const chunksSnapshot = [...state.voiceNoteChunks];
    resetVoiceNoteState({ preserveFinalizing: shouldSend });

    if (!shouldSend) {
      if (showCanceledToast) {
        showToast("Voice note canceled.");
      }
      return;
    }

    finalizeVoiceNoteRecording(chunksSnapshot, mimeType, chatId, estimatedDurationSeconds);
  };

  recorder.onstop = finalizeOnce;
  recorder.onerror = () => {
    finalizeOnce();

    if (shouldSend) {
      showToast("Voice note canceled.");
    }
  };

  try {
    if (recorder.state === "inactive") {
      finalizeOnce();
      return;
    }

    recorder.stop();
  } catch {
    finalizeOnce();
  }
}

async function startVoiceNoteRecording() {
  if (!voiceNoteSupported()) {
    showToast("Voice notes are not supported in this browser.");
    return;
  }

  if (!state.activeChat || elements.messageInput.disabled) {
    showToast("Open a chat first.");
    return;
  }

  if (state.voiceNoteFinalizing) {
    return;
  }

  if (state.voiceNoteRecorder) {
    return;
  }

  let stream;

  try {
    stream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: false
    });
  } catch {
    showToast("Microphone access is required for voice notes.");
    return;
  }

  const mimeType = preferredVoiceNoteMimeType();
  let recorder;

  try {
    recorder = mimeType ? new MediaRecorder(stream, { mimeType }) : new MediaRecorder(stream);
  } catch {
    for (const track of stream.getTracks()) {
      track.stop();
    }
    showToast("Could not start voice recording.");
    return;
  }

  state.voiceNoteStream = stream;
  state.voiceNoteRecorder = recorder;
  state.voiceNoteChunks = [];
  state.voiceNoteMimeType = mimeType || recorder.mimeType || "audio/webm";
  state.voiceNoteStartedAt = Date.now();
  state.voiceNoteChatId = state.activeChat.id;
  state.voiceNoteFinalizing = false;

  recorder.ondataavailable = (event) => {
    if (event.data && event.data.size > 0) {
      state.voiceNoteChunks.push(event.data);
    }
  };

  recorder.onerror = () => {
    stopVoiceNoteRecording(false, true);
  };

  try {
    recorder.start(250);
  } catch {
    resetVoiceNoteState();
    showToast("Could not start voice recording.");
    return;
  }

  updateVoiceNoteButton();
  clearVoiceNoteDurationTimer();
  voiceNoteDurationTimer = window.setInterval(() => {
    updateVoiceNoteButton();

    if (Date.now() - Number(state.voiceNoteStartedAt || Date.now()) >= VOICE_NOTE_MAX_DURATION_MS) {
      stopVoiceNoteRecording(true, false);
    }
  }, 250);
}

function toggleVoiceNoteRecording() {
  if (state.voiceNoteRecorder) {
    stopVoiceNoteRecording(true, false);
    return;
  }

  startVoiceNoteRecording();
}

function buildSpacePrefixedInsert(value) {
  const input = elements.messageInput;
  const selectionCollapsed = input.selectionStart === input.selectionEnd;
  const cursorPosition = Number(input.selectionStart ?? 0);
  const previousCharacter = input.value.slice(Math.max(0, cursorPosition - 1), cursorPosition);
  const needsLeadingSpace = selectionCollapsed && cursorPosition > 0 && !/\s/.test(previousCharacter);
  return `${needsLeadingSpace ? " " : ""}${value}`;
}

async function sendGifAttachmentFromUrl(gifUrl) {
  const url = String(gifUrl ?? "").trim();

  if (!url) {
    return;
  }

  try {
    const response = await fetch(url);

    if (!response.ok) {
      throw new Error("Failed to fetch GIF.");
    }

    const blob = await response.blob();
    const safeBlob = blob.type ? blob : blob.slice(0, blob.size, "image/gif");
    const extension = safeBlob.type.includes("webp") ? "webp" : safeBlob.type.includes("png") ? "png" : "gif";
    const file = new File([safeBlob], `gif-${Date.now()}.${extension}`, {
      type: safeBlob.type || "image/gif"
    });
    const attachment = await createAttachmentFromFile(file);

    if (attachment.size > 5 * 1024 * 1024) {
      throw new Error("GIF is larger than 5 MB.");
    }

    const sent = await sendMessageToActiveChat({
      text: "",
      attachments: [attachment],
      replyToMessageId: null,
      clearComposerOnSuccess: false
    });

    if (sent) {
      closeStickerMenu();
    }
  } catch {
    const insert = `${buildSpacePrefixedInsert(url)} `;
    insertTextAtCursor(elements.messageInput, insert);
    showToast("GIF attached as a link instead.");
    closeStickerMenu();
  }
}

function normalizeLocalVideoCorner(value) {
  const normalized = String(value ?? "").trim();
  return LOCAL_VIDEO_CORNERS.includes(normalized) ? normalized : "top-right";
}

function normalizeScreenShareInsetCorner(value) {
  const normalized = String(value ?? "").trim();
  return LOCAL_VIDEO_CORNERS.includes(normalized) ? normalized : "top-left";
}

function clampUnit(value) {
  const numeric = Number(value);

  if (!Number.isFinite(numeric)) {
    return 0;
  }

  return Math.max(0, Math.min(1, numeric));
}

function screenShareInsetBounds(frameWidth, frameHeight, insetWidth, insetHeight) {
  const marginX = Math.max(12, Math.round(frameWidth * 0.015));
  const marginTop = marginX + Math.max(4, Math.round(frameHeight * 0.012));
  const marginBottom = marginX;
  const minX = marginX;
  const maxX = Math.max(minX, frameWidth - insetWidth - marginX);
  const minY = marginTop;
  const maxY = Math.max(minY, frameHeight - insetHeight - marginBottom);

  return {
    minX,
    maxX,
    minY,
    maxY
  };
}

function cornerFromPoint(containerRect, clientX, clientY) {
  const centerX = clientX - containerRect.left;
  const centerY = clientY - containerRect.top;
  const horizontal = centerX <= containerRect.width / 2 ? "left" : "right";
  const vertical = centerY <= containerRect.height / 2 ? "top" : "bottom";
  return `${vertical}-${horizontal}`;
}

function screenShareInsetPositionForCorner(corner, frameWidth, frameHeight, insetWidth, insetHeight) {
  const normalizedCorner = normalizeScreenShareInsetCorner(corner);
  const bounds = screenShareInsetBounds(frameWidth, frameHeight, insetWidth, insetHeight);
  const x = normalizedCorner.endsWith("right") ? bounds.maxX : bounds.minX;
  const y = normalizedCorner.startsWith("bottom") ? bounds.maxY : bounds.minY;
  return { x, y };
}

function applyLocalVideoCorner(corner, persistPreference = true) {
  const normalizedCorner = normalizeLocalVideoCorner(corner);

  if (persistPreference) {
    preferredLocalVideoCorner = normalizedCorner;
  }

  if (state.call) {
    state.call.localVideoCorner = normalizedCorner;
  }

  if (!elements.callVideos) {
    return;
  }

  for (const knownCorner of LOCAL_VIDEO_CORNERS) {
    elements.callVideos.classList.remove(`local-video-${knownCorner}`);
  }

  elements.callVideos.classList.add(`local-video-${normalizedCorner}`);
  elements.localVideo.style.left = "";
  elements.localVideo.style.top = "";
  elements.localVideo.style.right = "";
  elements.localVideo.style.bottom = "";
}

function cornerFromLocalVideoPosition(containerRect, videoRect) {
  const centerX = videoRect.left + videoRect.width / 2;
  const centerY = videoRect.top + videoRect.height / 2;
  return cornerFromPoint(containerRect, centerX, centerY);
}

function stopLocalVideoDrag() {
  if (!localVideoDragState) {
    return;
  }

  if (!elements.callVideos || !elements.localVideo) {
    localVideoDragState = null;
    return;
  }

  const { pointerId } = localVideoDragState;
  localVideoDragState = null;
  elements.localVideo.classList.remove("dragging");

  try {
    elements.localVideo.releasePointerCapture(pointerId);
  } catch {}

  const containerRect = elements.callVideos.getBoundingClientRect();
  const videoRect = elements.localVideo.getBoundingClientRect();
  const corner = cornerFromLocalVideoPosition(containerRect, videoRect);
  applyLocalVideoCorner(corner);
}

function beginLocalVideoDrag(event) {
  if (!state.call || state.call.mode !== "video") {
    return;
  }

  if (!elements.callVideos || !elements.localVideo) {
    return;
  }

  if (event.pointerType === "mouse" && event.button !== 0) {
    return;
  }

  event.preventDefault();

  const containerRect = elements.callVideos.getBoundingClientRect();
  const videoRect = elements.localVideo.getBoundingClientRect();
  const offsetX = event.clientX - videoRect.left;
  const offsetY = event.clientY - videoRect.top;

  localVideoDragState = {
    pointerId: event.pointerId,
    offsetX,
    offsetY
  };

  elements.localVideo.classList.add("dragging");
  elements.localVideo.setPointerCapture(event.pointerId);

  const updateDragPosition = (moveEvent) => {
    if (!localVideoDragState || moveEvent.pointerId !== localVideoDragState.pointerId) {
      return;
    }

    const currentContainerRect = elements.callVideos.getBoundingClientRect();
    const width = elements.localVideo.offsetWidth;
    const height = elements.localVideo.offsetHeight;
    const margin = 8;
    const rawLeft = moveEvent.clientX - currentContainerRect.left - localVideoDragState.offsetX;
    const rawTop = moveEvent.clientY - currentContainerRect.top - localVideoDragState.offsetY;
    const left = Math.min(
      Math.max(margin, rawLeft),
      Math.max(margin, currentContainerRect.width - width - margin)
    );
    const top = Math.min(
      Math.max(margin, rawTop),
      Math.max(margin, currentContainerRect.height - height - margin)
    );

    elements.localVideo.style.left = `${Math.round(left)}px`;
    elements.localVideo.style.top = `${Math.round(top)}px`;
    elements.localVideo.style.right = "auto";
    elements.localVideo.style.bottom = "auto";
  };

  const endDrag = (endEvent) => {
    if (!localVideoDragState || endEvent.pointerId !== localVideoDragState.pointerId) {
      return;
    }

    elements.localVideo.removeEventListener("pointermove", updateDragPosition);
    elements.localVideo.removeEventListener("pointerup", endDrag);
    elements.localVideo.removeEventListener("pointercancel", endDrag);
    stopLocalVideoDrag();
  };

  elements.localVideo.addEventListener("pointermove", updateDragPosition);
  elements.localVideo.addEventListener("pointerup", endDrag);
  elements.localVideo.addEventListener("pointercancel", endDrag);
}

function stopRemoteVideoDrag() {
  if (!remoteVideoDragState) {
    return;
  }

  const { pointerId } = remoteVideoDragState;
  remoteVideoDragState = null;
  elements.remoteCameraVideo.classList.remove("dragging");

  try {
    elements.remoteCameraVideo.releasePointerCapture(pointerId);
  } catch {}
}

function beginRemoteVideoDrag(event) {
  if (
    !state.call ||
    state.call.mode !== "video" ||
    state.call.connected !== true ||
    state.call.remoteScreenSharing !== true ||
    state.call.drawEnabled === true ||
    elements.remoteCameraVideo.classList.contains("hidden")
  ) {
    return;
  }

  if (!elements.callVideos || !elements.remoteCameraVideo) {
    return;
  }

  if (event.pointerType === "mouse" && event.button !== 0) {
    return;
  }

  event.preventDefault();

  const containerRect = elements.callVideos.getBoundingClientRect();
  const videoRect = elements.remoteCameraVideo.getBoundingClientRect();
  const offsetX = event.clientX - videoRect.left;
  const offsetY = event.clientY - videoRect.top;

  remoteVideoDragState = {
    pointerId: event.pointerId,
    offsetX,
    offsetY
  };

  elements.remoteCameraVideo.classList.add("dragging");
  elements.remoteCameraVideo.setPointerCapture(event.pointerId);

  const finish = (endEvent, shouldCommit) => {
    if (!remoteVideoDragState || endEvent.pointerId !== remoteVideoDragState.pointerId) {
      return;
    }

    elements.remoteCameraVideo.removeEventListener("pointermove", handlePointerMove);
    elements.remoteCameraVideo.removeEventListener("pointerup", handlePointerUp);
    elements.remoteCameraVideo.removeEventListener("pointercancel", handlePointerCancel);

    stopRemoteVideoDrag();

    if (!shouldCommit || !elements.callVideos || !elements.remoteCameraVideo) {
      return;
    }

    const currentContainerRect = elements.callVideos.getBoundingClientRect();
    const currentRect = elements.remoteCameraVideo.getBoundingClientRect();
    const corner = cornerFromLocalVideoPosition(currentContainerRect, currentRect);
    const margin = 8;
    const width = elements.remoteCameraVideo.offsetWidth;
    const height = elements.remoteCameraVideo.offsetHeight;
    const targetLeft = corner.endsWith("right")
      ? Math.max(margin, currentContainerRect.width - width - margin)
      : margin;
    const targetTop = corner.startsWith("bottom")
      ? Math.max(margin, currentContainerRect.height - height - margin)
      : margin;
    elements.remoteCameraVideo.style.left = `${Math.round(targetLeft)}px`;
    elements.remoteCameraVideo.style.top = `${Math.round(targetTop)}px`;
    elements.remoteCameraVideo.style.right = "auto";
    elements.remoteCameraVideo.style.bottom = "auto";
  };

  const handlePointerMove = (moveEvent) => {
    if (!remoteVideoDragState || moveEvent.pointerId !== remoteVideoDragState.pointerId) {
      return;
    }

    const currentContainerRect = elements.callVideos.getBoundingClientRect();
    const width = elements.remoteCameraVideo.offsetWidth;
    const height = elements.remoteCameraVideo.offsetHeight;
    const margin = 8;
    const rawLeft = moveEvent.clientX - currentContainerRect.left - remoteVideoDragState.offsetX;
    const rawTop = moveEvent.clientY - currentContainerRect.top - remoteVideoDragState.offsetY;
    const left = Math.min(
      Math.max(margin, rawLeft),
      Math.max(margin, currentContainerRect.width - width - margin)
    );
    const top = Math.min(
      Math.max(margin, rawTop),
      Math.max(margin, currentContainerRect.height - height - margin)
    );
    elements.remoteCameraVideo.style.left = `${Math.round(left)}px`;
    elements.remoteCameraVideo.style.top = `${Math.round(top)}px`;
    elements.remoteCameraVideo.style.right = "auto";
    elements.remoteCameraVideo.style.bottom = "auto";
  };

  const handlePointerUp = (endEvent) => {
    finish(endEvent, true);
  };

  const handlePointerCancel = (endEvent) => {
    finish(endEvent, false);
  };

  elements.remoteCameraVideo.addEventListener("pointermove", handlePointerMove);
  elements.remoteCameraVideo.addEventListener("pointerup", handlePointerUp);
  elements.remoteCameraVideo.addEventListener("pointercancel", handlePointerCancel);
}

function toggleCallChatPanel() {
  if (!state.call) {
    return;
  }

  state.call.chatOpen = !(state.call.chatOpen !== false);
  renderCallChatVisibility();
}

function beginCallAnnotationStroke(event) {
  if (!state.call || state.call.mode !== "video" || state.call.drawEnabled !== true) {
    return;
  }

  if (event.pointerType === "mouse" && event.button !== 0) {
    return;
  }

  const startPoint = annotationPointFromPointerEvent(event);

  if (!startPoint) {
    return;
  }

  event.preventDefault();
  callAnnotationPointerState = {
    pointerId: event.pointerId,
    x: startPoint.x,
    y: startPoint.y
  };

  try {
    elements.callAnnotationCanvas.setPointerCapture(event.pointerId);
  } catch {}
}

function updateCallAnnotationStroke(event) {
  if (!callAnnotationPointerState || !state.call || state.call.drawEnabled !== true) {
    return;
  }

  if (event.pointerId !== callAnnotationPointerState.pointerId) {
    return;
  }

  const nextPoint = annotationPointFromPointerEvent(event);

  if (!nextPoint) {
    return;
  }

  const fromX = callAnnotationPointerState.x;
  const fromY = callAnnotationPointerState.y;
  const toX = nextPoint.x;
  const toY = nextPoint.y;

  drawCallAnnotationSegment(
    fromX,
    fromY,
    toX,
    toY,
    state.call.annotationColor,
    state.call.annotationWidth
  );

  socket.emit("call_annotation", {
    chatId: state.call.chatId,
    type: "segment",
    fromX,
    fromY,
    toX,
    toY,
    color: state.call.annotationColor,
    width: state.call.annotationWidth
  });

  callAnnotationPointerState.x = toX;
  callAnnotationPointerState.y = toY;
}

function endCallAnnotationStroke(event) {
  if (!callAnnotationPointerState || event.pointerId !== callAnnotationPointerState.pointerId) {
    return;
  }

  callAnnotationPointerState = null;

  try {
    elements.callAnnotationCanvas.releasePointerCapture(event.pointerId);
  } catch {}
}

function applyRemoteCallAnnotation(payload) {
  if (!payload || !state.call || state.call.chatId !== String(payload.chatId ?? "")) {
    return;
  }

  if (state.call.mode !== "video") {
    return;
  }

  resizeCallAnnotationCanvas();

  if (payload.type === "clear") {
    clearCallAnnotation();
    return;
  }

  drawCallAnnotationSegment(
    payload.fromX,
    payload.fromY,
    payload.toX,
    payload.toY,
    payload.color,
    payload.width
  );
}

function createVoiceMeter(stream) {
  if (!stream || !window.AudioContext) {
    return null;
  }

  const audioTracks = typeof stream.getAudioTracks === "function" ? stream.getAudioTracks() : [];

  if (!Array.isArray(audioTracks) || audioTracks.length === 0) {
    return null;
  }

  try {
    const context = new AudioContext();
    context.resume().catch(() => {});
    const source = context.createMediaStreamSource(stream);
    const analyser = context.createAnalyser();
    analyser.fftSize = 512;
    analyser.smoothingTimeConstant = 0.2;
    source.connect(analyser);

    return {
      context,
      source,
      analyser,
      data: new Uint8Array(analyser.fftSize)
    };
  } catch {
    return null;
  }
}

function disposeVoiceMeter(meter) {
  if (!meter) {
    return;
  }

  try {
    meter.source?.disconnect();
  } catch {}

  try {
    meter.analyser?.disconnect();
  } catch {}

  try {
    meter.context?.close?.();
  } catch {}
}

function getVoiceLevel(meter) {
  if (!meter?.analyser || !meter?.data) {
    return 0;
  }

  meter.analyser.getByteTimeDomainData(meter.data);

  let sumSquares = 0;
  for (const value of meter.data) {
    const normalized = (value - 128) / 128;
    sumSquares += normalized * normalized;
  }

  const rms = Math.sqrt(sumSquares / meter.data.length);
  return Number.isFinite(rms) ? rms : 0;
}

function updateVoiceActivityUi() {
  if (!state.call) {
    elements.voiceCallAvatar.classList.remove("speaking");
    elements.voiceCallSelfAvatar.classList.remove("speaking");
    elements.remoteVideo.classList.remove("speaking");
    elements.remoteCameraVideo.classList.remove("speaking");
    elements.localVideo.classList.remove("speaking");
    return;
  }

  const localSpeaking = state.call.localSpeaking === true && state.call.micEnabled !== false;
  const remoteSpeaking = state.call.remoteSpeaking === true && state.call.remoteMicEnabled !== false;

  elements.voiceCallAvatar.classList.toggle("speaking", remoteSpeaking);
  elements.voiceCallSelfAvatar.classList.toggle("speaking", localSpeaking);
  elements.remoteVideo.classList.toggle("speaking", remoteSpeaking);
  elements.remoteCameraVideo.classList.toggle("speaking", remoteSpeaking);
  elements.localVideo.classList.toggle("speaking", localSpeaking);
}

function attachCallVoiceMeters() {
  if (!state.call) {
    return;
  }

  if (!state.call.localVoiceMeter && state.call.localStream) {
    state.call.localVoiceMeter = createVoiceMeter(state.call.localStream);
  }

  if (!state.call.remoteVoiceMeter && state.call.remoteStream) {
    state.call.remoteVoiceMeter = createVoiceMeter(state.call.remoteStream);
  }
}

function tickVoiceActivity() {
  if (!state.call) {
    clearCallVoiceActivityTimer();
    return;
  }

  const now = Date.now();
  attachCallVoiceMeters();

  const localLevel = getVoiceLevel(state.call.localVoiceMeter);
  const remoteLevel = getVoiceLevel(state.call.remoteVoiceMeter);

  if (localLevel >= VOICE_ACTIVITY_THRESHOLD) {
    state.call.localSpeakingUntil = now + VOICE_ACTIVITY_HOLD_MS;
  }

  if (remoteLevel >= VOICE_ACTIVITY_THRESHOLD) {
    state.call.remoteSpeakingUntil = now + VOICE_ACTIVITY_HOLD_MS;
  }

  const nextLocalSpeaking = Number(state.call.localSpeakingUntil ?? 0) > now;
  const nextRemoteSpeaking = Number(state.call.remoteSpeakingUntil ?? 0) > now;
  const changed =
    nextLocalSpeaking !== Boolean(state.call.localSpeaking) ||
    nextRemoteSpeaking !== Boolean(state.call.remoteSpeaking);

  state.call.localSpeaking = nextLocalSpeaking;
  state.call.remoteSpeaking = nextRemoteSpeaking;

  if (changed) {
    updateVoiceActivityUi();
  }
}

function startVoiceActivityMonitor() {
  clearCallVoiceActivityTimer();
  tickVoiceActivity();

  callVoiceActivityTimer = window.setInterval(() => {
    tickVoiceActivity();
  }, VOICE_ACTIVITY_POLL_MS);
}

function renderCallConnectionState() {
  const awaitingRemote = Boolean(state.call) && !state.call.connected;
  elements.callOverlay.classList.toggle("awaiting-remote", awaitingRemote);
  elements.voiceCallRemoteUser.classList.toggle("hidden", awaitingRemote);
}

function startCallUi(mode, remoteName, statusText) {
  if (state.call && typeof state.call.chatOpen !== "boolean") {
    state.call.chatOpen = !isMobileViewport();
  }

  elements.callOverlay.classList.remove("hidden");
  elements.callOverlay.classList.toggle("voice-mode", mode === "voice");
  elements.callOverlay.classList.remove("connected");
  elements.callTitle.textContent = `${mode === "video" ? "Video" : "Voice"} Call with ${remoteName}`;
  elements.callStatus.textContent = statusText;
  elements.callModeBadge.textContent = mode === "video" ? "Video" : "Voice";
  elements.voiceCallName.textContent = remoteName;
  elements.voiceCallSelfName.textContent = state.currentUser?.name || "You";

  const remoteUser = resolveUserByKey(state.call?.remoteUserKey) ?? {
    name: remoteName
  };
  setAvatar(elements.voiceCallAvatar, remoteUser);
  setAvatar(elements.voiceCallSelfAvatar, state.currentUser ?? { name: "You" });

  elements.voiceCallPanel.classList.toggle("hidden", mode !== "voice");
  elements.remoteCameraVideo.classList.add("hidden");
  resetRemoteCameraVideoPosition();
  applyLocalVideoCorner(state.call?.localVideoCorner ?? preferredLocalVideoCorner, false);
  resizeCallAnnotationCanvas();
  setCallDrawEnabled(state.call?.drawEnabled === true && mode === "video");
  renderCallConnectionState();
  renderCallDuration();
  renderCallQualityBadge();
  renderRemoteMediaIndicators();
  renderCallChat();
  updateVoiceActivityUi();
  syncCallFocusState();
  updateCallControlButtons();
}

function setCallStatus(text) {
  elements.callStatus.textContent = text;
}

function callQualityLabel(level) {
  switch (String(level ?? "").trim()) {
    case "good":
      return "Good";
    case "fair":
      return "Fair";
    case "poor":
      return "Poor";
    case "reconnecting":
      return "Reconnecting";
    default:
      return "Checking";
  }
}

function setCallQuality(level, details = {}) {
  if (!state.call) {
    return;
  }

  state.call.qualityLevel = String(level ?? "checking").trim() || "checking";
  state.call.qualityRttMs = Number.isFinite(Number(details.rttMs)) ? Number(details.rttMs) : null;
  state.call.qualityLossRatio =
    Number.isFinite(Number(details.lossRatio)) && Number(details.lossRatio) >= 0
      ? Number(details.lossRatio)
      : null;
  renderCallQualityBadge();
}

function renderCallQualityBadge() {
  if (!state.call) {
    elements.callQualityBadge.classList.add("hidden");
    elements.callQualityBadge.className = "call-quality-badge hidden";
    elements.callQualityBadge.textContent = "Quality: Checking";
    elements.callQualityBadge.removeAttribute("title");
    return;
  }

  const level = state.call.qualityLevel || "checking";
  const label = callQualityLabel(level);
  elements.callQualityBadge.className = `call-quality-badge ${level}`;
  elements.callQualityBadge.textContent = `Quality: ${label}`;

  const details = [];
  if (Number.isFinite(state.call.qualityRttMs)) {
    details.push(`RTT ${state.call.qualityRttMs}ms`);
  }
  if (typeof state.call.qualityLossRatio === "number") {
    details.push(`Loss ${Math.round(state.call.qualityLossRatio * 100)}%`);
  }

  if (details.length > 0) {
    elements.callQualityBadge.title = details.join(" | ");
  } else {
    elements.callQualityBadge.removeAttribute("title");
  }

  elements.callQualityBadge.classList.remove("hidden");
}

function summarizeCallQuality(statsReport) {
  let bestRttMs = null;
  let totalPackets = 0;
  let totalLost = 0;

  statsReport.forEach((stat) => {
    if (stat.type === "candidate-pair" && stat.state === "succeeded") {
      const isSelected =
        Boolean(stat.nominated) ||
        Boolean(stat.selected) ||
        Number(stat.bytesReceived ?? 0) > 0 ||
        Number(stat.bytesSent ?? 0) > 0;

      if (!isSelected) {
        return;
      }

      const rttSeconds = Number(stat.currentRoundTripTime);

      if (!Number.isFinite(rttSeconds) || rttSeconds < 0) {
        return;
      }

      const rttMs = Math.round(rttSeconds * 1000);

      if (bestRttMs === null || rttMs < bestRttMs) {
        bestRttMs = rttMs;
      }
    }

    if (stat.type === "inbound-rtp" && !stat.isRemote && stat.kind === "audio") {
      const packetsReceived = Math.max(0, Number(stat.packetsReceived ?? 0));
      const packetsLost = Math.max(0, Number(stat.packetsLost ?? 0));

      totalPackets += packetsReceived + packetsLost;
      totalLost += packetsLost;
    }
  });

  const lossRatio = totalPackets > 0 ? totalLost / totalPackets : null;
  let score = 0;

  if (bestRttMs !== null) {
    if (bestRttMs <= 130) {
      score += 2;
    } else if (bestRttMs <= 260) {
      score += 1;
    }
  }

  if (lossRatio !== null) {
    if (lossRatio <= 0.03) {
      score += 2;
    } else if (lossRatio <= 0.08) {
      score += 1;
    }
  }

  let level = "fair";

  if (bestRttMs === null && lossRatio === null) {
    level = "checking";
  } else if (score >= 3) {
    level = "good";
  } else if (score <= 1) {
    level = "poor";
  }

  return {
    level,
    rttMs: bestRttMs,
    lossRatio
  };
}

async function sampleCallQuality() {
  if (!state.call?.peer || !state.call.connected) {
    return;
  }

  try {
    const statsReport = await state.call.peer.getStats();
    const quality = summarizeCallQuality(statsReport);
    setCallQuality(quality.level, quality);
  } catch {
    setCallQuality("checking");
  }
}

function startCallQualityPolling() {
  clearCallQualityTimer();
  sampleCallQuality();

  callQualityTimer = window.setInterval(() => {
    sampleCallQuality();
  }, CALL_QUALITY_POLL_MS);
}

function formatCallDurationLabel(milliseconds) {
  const totalSeconds = Math.max(0, Math.floor(Number(milliseconds || 0) / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function renderCallDuration() {
  if (!state.call?.connected || !state.call.connectedAt) {
    elements.callDuration.textContent = "Duration 00:00";
    elements.callDuration.classList.add("hidden");
    return;
  }

  const elapsedMs = Date.now() - state.call.connectedAt;
  elements.callDuration.textContent = `Duration ${formatCallDurationLabel(elapsedMs)}`;
  elements.callDuration.classList.remove("hidden");
}

function startCallDurationTimer() {
  clearCallDurationTimer();
  renderCallDuration();
  callDurationTimer = window.setInterval(() => {
    renderCallDuration();
  }, 1000);
}

function renderCallChatVisibility() {
  const hasCall = Boolean(state.call);
  const chatOpen = hasCall && state.call?.chatOpen !== false;

  elements.callMain.classList.toggle("chat-collapsed", !chatOpen);
  elements.callChatPanel.classList.toggle("hidden", !chatOpen);
  elements.toggleCallChatButton.disabled = !hasCall;
  elements.toggleCallChatButton.textContent = chatOpen ? "Hide Chat" : "Call Chat";
}

function renderCallChat() {
  const container = elements.callChatMessages;
  const shouldStickToBottom =
    container.scrollHeight - container.scrollTop - container.clientHeight < 32;
  container.innerHTML = "";

  if (!state.call) {
    elements.callChatInput.disabled = true;
    elements.callChatSendButton.disabled = true;
    renderCallChatVisibility();
    return;
  }

  const canSend = state.call.connected === true;
  elements.callChatInput.disabled = !canSend;
  elements.callChatSendButton.disabled = !canSend;

  const callMessages = Array.isArray(state.call.callMessages) ? state.call.callMessages : [];
  const recentMessages = callMessages.slice(-CALL_CHAT_MESSAGE_LIMIT);

  for (const message of recentMessages) {
    const card = document.createElement("article");
    card.className = `call-chat-message ${
      message.senderKey === "system"
        ? "system"
        : message.senderKey === state.currentUser?.key
          ? "self"
          : "other"
    }`;

    const meta = document.createElement("p");
    meta.className = "call-chat-meta";
    meta.textContent =
      message.senderKey === "system"
        ? "System"
        : `${message.senderName} - ${formatMessageTime(message.sentAt)}`;

    const body = document.createElement("p");
    body.className = "call-chat-body";
    body.textContent = String(message.text ?? "");

    card.append(meta, body);
    container.append(card);
  }

  if (recentMessages.length === 0) {
    const empty = document.createElement("p");
    empty.className = "call-chat-empty";
    empty.textContent = canSend ? "No messages yet." : "Call chat unlocks once the call connects.";
    container.append(empty);
  }

  if (shouldStickToBottom) {
    container.scrollTop = container.scrollHeight;
  }

  renderCallChatVisibility();
}

function sendCallChatMessage() {
  if (!state.call || !state.call.connected) {
    return;
  }

  const text = elements.callChatInput.value.trim();

  if (!text) {
    return;
  }

  socket.emit(
    "call_chat_message",
    {
      chatId: state.call.chatId,
      text
    },
    (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Failed to send call chat message.");
        return;
      }

      elements.callChatInput.value = "";
    }
  );
}

function resizeCallAnnotationCanvas() {
  if (!elements.callAnnotationCanvas || !elements.callVideos) {
    return;
  }

  const rect = elements.callVideos.getBoundingClientRect();
  const width = Math.max(1, Math.round(rect.width));
  const height = Math.max(1, Math.round(rect.height));
  const dpr = Math.max(1, window.devicePixelRatio || 1);
  const targetWidth = Math.round(width * dpr);
  const targetHeight = Math.round(height * dpr);

  if (
    elements.callAnnotationCanvas.width === targetWidth &&
    elements.callAnnotationCanvas.height === targetHeight
  ) {
    return;
  }

  elements.callAnnotationCanvas.width = targetWidth;
  elements.callAnnotationCanvas.height = targetHeight;
  elements.callAnnotationCanvas.style.width = `${width}px`;
  elements.callAnnotationCanvas.style.height = `${height}px`;

  const context = elements.callAnnotationCanvas.getContext("2d");
  if (context) {
    context.lineCap = "round";
    context.lineJoin = "round";
    context.clearRect(0, 0, targetWidth, targetHeight);
  }
}

function clearCallAnnotation() {
  const context = elements.callAnnotationCanvas.getContext("2d");
  if (!context) {
    return;
  }

  context.clearRect(0, 0, elements.callAnnotationCanvas.width, elements.callAnnotationCanvas.height);
}

function drawCallAnnotationSegment(fromX, fromY, toX, toY, color, width) {
  const context = elements.callAnnotationCanvas.getContext("2d");

  if (!context) {
    return;
  }

  const clamp = (value) => Math.min(1, Math.max(0, Number(value) || 0));
  const normalizedFromX = clamp(fromX);
  const normalizedFromY = clamp(fromY);
  const normalizedToX = clamp(toX);
  const normalizedToY = clamp(toY);
  const dpr = Math.max(1, window.devicePixelRatio || 1);
  const lineWidth = Math.max(1, Number(width) || CALL_ANNOTATION_DEFAULT_WIDTH) * dpr;

  context.strokeStyle = String(color || CALL_ANNOTATION_DEFAULT_COLOR);
  context.lineWidth = lineWidth;
  context.beginPath();
  context.moveTo(
    normalizedFromX * elements.callAnnotationCanvas.width,
    normalizedFromY * elements.callAnnotationCanvas.height
  );
  context.lineTo(
    normalizedToX * elements.callAnnotationCanvas.width,
    normalizedToY * elements.callAnnotationCanvas.height
  );
  context.stroke();
}

function annotationPointFromPointerEvent(event) {
  const rect = elements.callAnnotationCanvas.getBoundingClientRect();

  if (rect.width <= 0 || rect.height <= 0) {
    return null;
  }

  return {
    x: Math.min(1, Math.max(0, (event.clientX - rect.left) / rect.width)),
    y: Math.min(1, Math.max(0, (event.clientY - rect.top) / rect.height))
  };
}

function setCallDrawEnabled(enabled) {
  if (!state.call) {
    return;
  }

  state.call.drawEnabled = Boolean(enabled);
  elements.callAnnotationCanvas.classList.toggle("draw-enabled", state.call.drawEnabled);
  elements.callAnnotationCanvas.style.pointerEvents = state.call.drawEnabled ? "auto" : "none";
  elements.toggleDrawButton.classList.toggle("active", state.call.drawEnabled);
  elements.toggleDrawButton.textContent = state.call.drawEnabled ? "Draw On" : "Draw Off";

  if (!state.call.drawEnabled) {
    callAnnotationPointerState = null;
  }
}

function toggleCallDraw() {
  if (!state.call || state.call.mode !== "video" || !state.call.connected) {
    return;
  }

  setCallDrawEnabled(!state.call.drawEnabled);
}

function clearCallDrawAndSync() {
  if (!state.call || state.call.mode !== "video" || !state.call.connected) {
    return;
  }

  clearCallAnnotation();
  socket.emit("call_annotation", {
    chatId: state.call.chatId,
    type: "clear"
  });
}

function isLiveTrack(track) {
  return Boolean(track && track.readyState === "live");
}

function inferScreenShareTrack(track) {
  if (!track) {
    return false;
  }

  const settings = track.getSettings?.() ?? {};
  const label = String(track.label ?? "").toLowerCase();
  return Boolean(settings.displaySurface) || /(screen|window|display|share)/.test(label);
}

function setVideoElementTrack(element, track) {
  if (!element) {
    return;
  }

  if (!isLiveTrack(track)) {
    element.srcObject = null;
    return;
  }

  const currentTrack = element.srcObject?.getVideoTracks?.()[0] ?? null;

  if (currentTrack?.id === track.id && currentTrack.readyState === "live") {
    return;
  }

  element.srcObject = new MediaStream([track]);
}

function resetRemoteCameraVideoPosition() {
  elements.remoteCameraVideo.style.left = "";
  elements.remoteCameraVideo.style.top = "";
  elements.remoteCameraVideo.style.right = "";
  elements.remoteCameraVideo.style.bottom = "";
}

function syncRemoteVideoElements() {
  if (!state.call || state.call.mode !== "video") {
    elements.remoteCameraVideo.srcObject = null;
    elements.remoteCameraVideo.classList.add("hidden");
    return;
  }

  if (!state.call.remoteVideoTracks) {
    state.call.remoteVideoTracks = {
      screen: null,
      camera: null
    };
  }

  const screenTrack = isLiveTrack(state.call.remoteVideoTracks.screen)
    ? state.call.remoteVideoTracks.screen
    : null;
  const cameraTrack = isLiveTrack(state.call.remoteVideoTracks.camera)
    ? state.call.remoteVideoTracks.camera
    : null;
  const sharingPreferred = state.call.remoteScreenSharing === true || Boolean(screenTrack);
  const mainTrack = sharingPreferred ? screenTrack ?? cameraTrack : cameraTrack ?? screenTrack;
  const showRemoteCameraTile = sharingPreferred && Boolean(screenTrack) && Boolean(cameraTrack);

  setVideoElementTrack(elements.remoteVideo, mainTrack);
  state.call.remoteStream = elements.remoteVideo.srcObject ?? null;
  state.call.remoteScreenSharing = sharingPreferred;

  if (showRemoteCameraTile) {
    setVideoElementTrack(elements.remoteCameraVideo, cameraTrack);
    elements.remoteCameraVideo.classList.remove("hidden");
  } else {
    elements.remoteCameraVideo.srcObject = null;
    elements.remoteCameraVideo.classList.add("hidden");
    resetRemoteCameraVideoPosition();
    stopRemoteVideoDrag();
  }
}

function renderRemoteVideoPlaceholder() {
  if (!state.call || state.call.mode !== "video" || !state.call.connected) {
    elements.remoteVideoPlaceholder.classList.add("hidden");
    return;
  }

  const remoteName = String(state.call.remoteName ?? "Other user");
  const screenSharing =
    state.call.remoteScreenSharing === true || isLiveTrack(state.call.remoteVideoTracks?.screen);
  const cameraEnabled = state.call.remoteCameraEnabled !== false;
  const mainTrack = elements.remoteVideo.srcObject?.getVideoTracks?.()[0] ?? null;
  const hasMainTrack = isLiveTrack(mainTrack);

  let showPlaceholder = false;
  let text = `${remoteName} camera is off`;

  if (!hasMainTrack && screenSharing) {
    showPlaceholder = true;
    text = `Waiting for ${remoteName}'s screen...`;
  } else if (!screenSharing && !cameraEnabled) {
    showPlaceholder = true;
    text = `${remoteName} camera is off`;
  } else if (!screenSharing && !hasMainTrack) {
    showPlaceholder = true;
    text = `Waiting for ${remoteName}'s video...`;
  }

  elements.remoteVideoPlaceholderText.textContent = text;
  elements.remoteVideoPlaceholder.classList.toggle("hidden", !showPlaceholder);
}

function syncRemoteScreenShareLayout() {
  syncRemoteVideoElements();

  const remoteSharing =
    Boolean(state.call) &&
    state.call.mode === "video" &&
    state.call.connected === true &&
    state.call.remoteScreenSharing === true;
  elements.callVideos.classList.toggle("remote-screen-share", remoteSharing);

  if (!remoteSharing) {
    stopRemoteVideoDrag();
  }
}

function renderRemoteMediaIndicators() {
  if (!state.call || !state.call.connected) {
    elements.remoteMediaStatus.classList.add("hidden");
    elements.remoteCameraIndicator.classList.add("hidden");
    syncRemoteScreenShareLayout();
    renderRemoteVideoPlaceholder();
    return;
  }

  const remoteName = String(state.call.remoteName ?? "Other user");
  const micEnabled = state.call.remoteMicEnabled !== false;
  const cameraEnabled = state.call.remoteCameraEnabled !== false;
  const screenSharing =
    state.call.remoteScreenSharing === true || isLiveTrack(state.call.remoteVideoTracks?.screen);
  const isVideoCall = state.call.mode === "video";

  elements.remoteMediaStatus.classList.remove("hidden");

  elements.remoteMicIndicator.classList.toggle("off", !micEnabled);
  elements.remoteMicIndicator.textContent = micEnabled
    ? `${remoteName}: mic on`
    : `${remoteName}: muted`;

  elements.remoteCameraIndicator.classList.toggle("hidden", !isVideoCall);
  elements.remoteCameraIndicator.classList.toggle("off", !cameraEnabled && !screenSharing);

  if (isVideoCall) {
    if (screenSharing) {
      elements.remoteCameraIndicator.textContent = `${remoteName}: sharing screen`;
    } else {
      elements.remoteCameraIndicator.textContent = cameraEnabled
        ? `${remoteName}: cam on`
        : `${remoteName}: cam off`;
    }
  }

  syncRemoteScreenShareLayout();
  renderRemoteVideoPlaceholder();
}

function emitCallMediaState() {
  if (!state.call) {
    return;
  }

  socket.emit(
    "call_media_state",
    {
      chatId: state.call.chatId,
      micEnabled: state.call.micEnabled !== false,
      cameraEnabled: state.call.cameraEnabled !== false,
      screenSharing: state.call.isScreenSharing === true
    },
    () => {}
  );
}

function setTrackEnabled(stream, kind, enabled) {
  if (!stream) {
    return;
  }

  for (const track of stream.getTracks()) {
    if (track.kind === kind) {
      track.enabled = enabled;
    }
  }
}

function isScreenShareSupported() {
  return Boolean(navigator.mediaDevices?.getDisplayMedia);
}

function findVideoSender(peer) {
  if (!peer) {
    return null;
  }

  return (
    peer.getSenders().find((sender) => sender.track && sender.track.kind === "video") ??
    null
  );
}

async function createCompositeScreenShareTrack(displayStream, cameraStream) {
  const displayTrack = displayStream?.getVideoTracks?.()[0] ?? null;

  if (!displayTrack) {
    return null;
  }

  const settings = displayTrack.getSettings?.() ?? {};
  const width = Math.max(640, Number(settings.width) || 1280);
  const height = Math.max(360, Number(settings.height) || 720);
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;

  const context = canvas.getContext("2d", { alpha: false });

  if (!context) {
    return null;
  }

  const displayVideo = document.createElement("video");
  displayVideo.srcObject = displayStream;
  displayVideo.muted = true;
  displayVideo.playsInline = true;
  displayVideo.autoplay = true;

  const cameraTrack = cameraStream?.getVideoTracks?.()[0] ?? null;
  const cameraVideo = document.createElement("video");

  if (cameraTrack) {
    cameraVideo.srcObject = new MediaStream([cameraTrack]);
    cameraVideo.muted = true;
    cameraVideo.playsInline = true;
    cameraVideo.autoplay = true;
  }

  await Promise.allSettled([
    displayVideo.play(),
    cameraTrack ? cameraVideo.play() : Promise.resolve()
  ]);

  let frameHandle = 0;
  let disposed = false;

  const drawFrame = () => {
    if (disposed) {
      return;
    }

    if (displayVideo.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
      try {
        context.drawImage(displayVideo, 0, 0, width, height);
      } catch {
        context.fillStyle = "#0b0b0b";
        context.fillRect(0, 0, width, height);
      }
    } else {
      context.fillStyle = "#0b0b0b";
      context.fillRect(0, 0, width, height);
    }

    const fallbackLocalVideo =
      elements.localVideo &&
      elements.localVideo.srcObject &&
      elements.localVideo.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA
        ? elements.localVideo
        : null;
    const cameraSourceElement =
      cameraVideo.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA ? cameraVideo : fallbackLocalVideo;
    const shouldDrawCameraInset =
      state.call?.cameraEnabled !== false &&
      Boolean(cameraSourceElement);

    if (shouldDrawCameraInset) {
      const cameraSourceAspect =
        Number(cameraSourceElement.videoWidth) > 0 && Number(cameraSourceElement.videoHeight) > 0
          ? cameraSourceElement.videoWidth / cameraSourceElement.videoHeight
          : 16 / 9;
      const insetAspect = Math.max(1.28, Math.min(1.85, cameraSourceAspect));
      let insetWidth = Math.max(170, Math.round(width * 0.3));
      insetWidth = Math.min(Math.round(width * 0.42), insetWidth);
      let insetHeight = Math.round(insetWidth / insetAspect);
      const maxInsetHeight = Math.max(96, Math.round(height * 0.34));

      if (insetHeight > maxInsetHeight) {
        insetHeight = maxInsetHeight;
        insetWidth = Math.round(insetHeight * insetAspect);
      }

      const bounds = screenShareInsetBounds(width, height, insetWidth, insetHeight);
      const requestedPosition = state.call?.screenShareInsetPosition;
      let x;
      let y;

      if (
        requestedPosition &&
        Number.isFinite(Number(requestedPosition.x)) &&
        Number.isFinite(Number(requestedPosition.y))
      ) {
        const normalizedX = clampUnit(requestedPosition.x);
        const normalizedY = clampUnit(requestedPosition.y);
        x = bounds.minX + normalizedX * (bounds.maxX - bounds.minX);
        y = bounds.minY + normalizedY * (bounds.maxY - bounds.minY);
      } else {
        const insetPosition = screenShareInsetPositionForCorner(
          state.call?.screenShareInsetCorner,
          width,
          height,
          insetWidth,
          insetHeight
        );
        x = insetPosition.x;
        y = insetPosition.y;
      }

      const radius = Math.max(10, Math.round(Math.min(insetWidth, insetHeight) * 0.11));

      context.save();
      context.fillStyle = "rgba(0, 0, 0, 0.58)";
      addRoundedRectPath(context, x - 4, y - 4, insetWidth + 8, insetHeight + 8, radius + 4);
      context.fill();
      context.restore();

      context.save();
      addRoundedRectPath(context, x, y, insetWidth, insetHeight, radius);
      context.clip();

      try {
        drawMediaCover(context, cameraSourceElement, x, y, insetWidth, insetHeight);
      } catch {
        // Ignore transient draw errors while camera frames warm up.
      }

      context.restore();
      context.save();
      context.strokeStyle = "rgba(255, 255, 255, 0.26)";
      context.lineWidth = 2;
      addRoundedRectPath(context, x, y, insetWidth, insetHeight, radius);
      context.stroke();
      context.restore();
    }

    frameHandle = window.requestAnimationFrame(drawFrame);
  };

  drawFrame();

  const compositeStream = canvas.captureStream(24);
  const compositeTrack = compositeStream.getVideoTracks()[0] ?? null;

  if (!compositeTrack) {
    disposed = true;
    window.cancelAnimationFrame(frameHandle);
    displayVideo.pause();
    displayVideo.srcObject = null;
    cameraVideo.pause();
    cameraVideo.srcObject = null;
    for (const track of compositeStream.getTracks()) {
      track.stop();
    }
    return null;
  }

  const stop = () => {
    if (disposed) {
      return;
    }

    disposed = true;
    window.cancelAnimationFrame(frameHandle);

    displayVideo.pause();
    displayVideo.srcObject = null;
    cameraVideo.pause();
    cameraVideo.srcObject = null;

    for (const track of compositeStream.getTracks()) {
      track.stop();
    }
  };

  return {
    track: compositeTrack,
    stop
  };
}

async function refreshScreenShareOutboundTrack() {
  if (!state.call?.isScreenSharing) {
    return;
  }

  const videoSender = findVideoSender(state.call.peer);
  const displayStream = state.call.screenShareStream;
  const displayTrack = displayStream?.getVideoTracks?.()[0] ?? null;
  const cameraTrack = state.call.localStream?.getVideoTracks?.()[0] ?? null;

  if (!videoSender || !displayTrack) {
    return;
  }

  if (cameraTrack) {
    cameraTrack.enabled = state.call.cameraEnabled !== false;
  }

  if (typeof state.call.screenShareCleanup === "function") {
    state.call.screenShareCleanup();
  }

  let nextCleanup = null;
  let outboundTrack = displayTrack;

  if (cameraTrack && state.call.cameraEnabled !== false) {
    const composite = await createCompositeScreenShareTrack(displayStream, state.call.localStream);

    if (composite?.track) {
      outboundTrack = composite.track;
      nextCleanup = composite.stop;
    }
  }

  try {
    await videoSender.replaceTrack(outboundTrack);
    state.call.screenShareCleanup = nextCleanup;
  } catch {
    if (typeof nextCleanup === "function") {
      nextCleanup();
    }
  }
}

async function stopScreenShare(options = {}) {
  if (!state.call?.isScreenSharing) {
    return;
  }

  const silent = options.silent === true;
  const shareStream = state.call.screenShareStream;
  const shareCleanup = state.call.screenShareCleanup;
  const shareSender = state.call.screenShareSender;
  const cameraTrack = state.call.localStream?.getVideoTracks?.()[0] ?? null;

  if (shareSender && state.call.peer) {
    try {
      state.call.peer.removeTrack(shareSender);
    } catch {}
  }

  if (shareStream) {
    for (const track of shareStream.getTracks()) {
      track.onended = null;
      track.stop();
    }
  }

  if (typeof shareCleanup === "function") {
    shareCleanup();
  }

  if (cameraTrack) {
    cameraTrack.enabled = state.call.cameraEnabled !== false;
    elements.localVideo.srcObject = state.call.localStream;
  } else {
    elements.localVideo.srcObject = null;
  }

  state.call.screenShareStream = null;
  state.call.screenShareCleanup = null;
  state.call.screenShareSender = null;
  state.call.isScreenSharing = false;
  state.call.screenShareInsetPosition = null;
  emitCallMediaState();
  updateCallControlButtons();
  await emitCallRenegotiationOffer();

  if (!silent) {
    showToast("Screen sharing stopped.");
  }
}

async function startScreenShare() {
  if (!state.call || state.call.mode !== "video" || state.call.connected !== true) {
    return;
  }

  if (!isScreenShareSupported()) {
    showToast("Screen sharing is not supported in this browser.");
    return;
  }

  let displayStream;

  try {
    displayStream = await navigator.mediaDevices.getDisplayMedia({
      video: true,
      audio: false
    });
    const displayTrack = displayStream.getVideoTracks()[0] ?? null;

    if (!displayTrack) {
      for (const track of displayStream.getTracks()) {
        track.stop();
      }
      showToast("Could not start screen share.");
      return;
    }

    const screenShareSender = state.call.peer.addTrack(displayTrack, displayStream);

    displayTrack.onended = () => {
      if (state.call?.screenShareStream === displayStream) {
        stopScreenShare({ silent: true });
      }
    };

    state.call.screenShareStream = displayStream;
    state.call.screenShareCleanup = null;
    state.call.screenShareSender = screenShareSender;
    state.call.isScreenSharing = true;
    elements.localVideo.srcObject = state.call.localStream;
    emitCallMediaState();
    updateCallControlButtons();

    const renegotiated = await emitCallRenegotiationOffer();

    if (!renegotiated) {
      if (screenShareSender && state.call?.peer) {
        try {
          state.call.peer.removeTrack(screenShareSender);
        } catch {}
      }
      for (const track of displayStream.getTracks()) {
        track.onended = null;
        track.stop();
      }
      state.call.screenShareStream = null;
      state.call.screenShareSender = null;
      state.call.isScreenSharing = false;
      emitCallMediaState();
      updateCallControlButtons();
      showToast("Could not start screen share.");
      return;
    }

    showToast("Screen sharing started.");
  } catch {
    if (displayStream) {
      for (const track of displayStream.getTracks()) {
        track.stop();
      }
    }
    showToast("Screen sharing canceled.");
  }
}

async function toggleScreenShare() {
  if (!state.call || state.call.mode !== "video") {
    return;
  }

  if (state.call.isScreenSharing) {
    await stopScreenShare();
    return;
  }

  await startScreenShare();
}

function updateCallControlButtons() {
  const hasCall = Boolean(state.call);
  const mode = state.call?.mode ?? "voice";
  const micEnabled = hasCall ? state.call?.micEnabled !== false : false;
  const cameraEnabled = hasCall ? state.call?.cameraEnabled !== false : false;
  const isScreenSharing = hasCall ? state.call?.isScreenSharing === true : false;

  elements.toggleMicButton.disabled = !hasCall;
  elements.toggleMicButton.textContent = micEnabled ? "Mic On" : "Mic Off";
  elements.toggleMicButton.classList.toggle("off", !micEnabled);

  const isVideoCall = hasCall && mode === "video";
  const canDraw = isVideoCall && state.call?.connected === true;
  elements.toggleCameraButton.classList.toggle("hidden", !isVideoCall);
  elements.toggleCameraButton.disabled = !isVideoCall;
  elements.toggleCameraButton.textContent = cameraEnabled ? "Cam On" : "Cam Off";
  elements.toggleCameraButton.classList.toggle("off", !cameraEnabled);

  const canUseScreenShare = isVideoCall && isScreenShareSupported() && state.call?.connected === true;
  elements.toggleScreenShareButton.classList.toggle("hidden", !isVideoCall);
  elements.toggleScreenShareButton.disabled = !canUseScreenShare;
  elements.toggleScreenShareButton.textContent = isScreenSharing ? "Stop Share" : "Share Screen";
  elements.toggleScreenShareButton.classList.toggle("active", isScreenSharing);

  elements.toggleDrawButton.classList.toggle("hidden", !isVideoCall);
  elements.clearDrawButton.classList.toggle("hidden", !isVideoCall);
  elements.toggleDrawButton.disabled = !canDraw;
  elements.clearDrawButton.disabled = !canDraw;

  if ((!isVideoCall || !canDraw) && state.call?.drawEnabled) {
    setCallDrawEnabled(false);
  }

  renderCallChatVisibility();
}

function setCallConnected(connected, options = {}) {
  if (!state.call) {
    return;
  }

  const preserveConnectedAt = options.preserveConnectedAt === true;
  const wasConnected = Boolean(state.call.connected);
  state.call.connected = connected;

  if (!connected) {
    clearCallQualityTimer();
    clearCallVoiceActivityTimer();

    if (!preserveConnectedAt) {
      state.call.connectedAt = null;
      clearCallDurationTimer();
    }
  } else {
    clearCallReconnectTimer();

    if (!state.call.connectedAt) {
      state.call.connectedAt = Date.now();
    }

    if (!wasConnected) {
      startCallDurationTimer();
    }

    state.call.reconnecting = false;
    startCallQualityPolling();
    startVoiceActivityMonitor();
  }

  renderCallDuration();
  renderCallQualityBadge();
  renderCallConnectionState();
  elements.callOverlay.classList.toggle("connected", connected);
  renderRemoteMediaIndicators();
  resizeCallAnnotationCanvas();
  updateVoiceActivityUi();
  renderCallChat();

  if (connected) {
    emitCallMediaState();
  }
}

function callEndMessageForReason(reason, fallback = "Call ended.") {
  switch (String(reason ?? "").trim()) {
    case "missed":
      return "Call missed.";
    case "busy":
      return "User is busy on another call.";
    case "no_answer":
      return "No answer after 5 rings. Call ended.";
    case "no_media":
      return "Call failed: media device unavailable.";
    case "unavailable":
      return "User is unavailable.";
    case "declined":
      return "Call was declined.";
    case "network_drop":
      return "Call dropped due to connection issues.";
    default:
      return fallback;
  }
}

function outgoingRingLabel(ringCount) {
  const remaining = Math.max(0, MAX_RING_COUNT - ringCount);
  return `Ringing... ${ringCount}/${MAX_RING_COUNT} (${remaining} left)`;
}

function startOutgoingRingCountdown() {
  clearOutgoingRingTimer();

  if (!state.call) {
    return;
  }

  state.call.ringCount = 0;

  const ringTick = () => {
    if (!state.call || state.call.connected) {
      clearOutgoingRingTimer();
      return;
    }

    state.call.ringCount += 1;

    if (state.call.ringCount > MAX_RING_COUNT) {
      const { chatId } = state.call;
      clearOutgoingRingTimer();
      socket.emit("end_call", { chatId, reason: "no_answer" }, () => {});
      showToast("No answer. Call ended.");
      cleanupCall(false);
      return;
    }

    setCallStatus(outgoingRingLabel(state.call.ringCount));
  };

  ringTick();
  outgoingRingTimer = window.setInterval(ringTick, RING_INTERVAL_MS);
}

function updateIncomingRingLabel() {
  if (!state.pendingIncomingCall) {
    return;
  }

  const ringCount = Number(state.pendingIncomingCall.ringCount ?? 0);
  const remaining = Math.max(0, MAX_RING_COUNT - ringCount);
  elements.incomingCallRings.textContent = `Ring ${ringCount}/${MAX_RING_COUNT} - Auto-decline in ${remaining}`;
}

function startIncomingRingCountdown() {
  clearIncomingRingTimer();

  const ringTick = () => {
    if (!state.pendingIncomingCall) {
      clearIncomingRingTimer();
      return;
    }

    state.pendingIncomingCall.ringCount = Number(state.pendingIncomingCall.ringCount ?? 0) + 1;

    if (state.pendingIncomingCall.ringCount > MAX_RING_COUNT) {
      const fromName = state.pendingIncomingCall.fromName;
      declineIncomingCall("missed");
      showToast(`Missed call from ${fromName}.`);
      return;
    }

    updateIncomingRingLabel();
  };

  ringTick();
  incomingRingTimer = window.setInterval(ringTick, RING_INTERVAL_MS);
}

async function emitCallReconnectOffer() {
  if (!state.call?.peer) {
    return false;
  }

  if (state.call.reconnectOfferPending) {
    return false;
  }

  state.call.reconnectOfferPending = true;
  const { chatId, peer } = state.call;

  try {
    if (typeof peer.restartIce === "function") {
      peer.restartIce();
    }

    const offer = await peer.createOffer({ iceRestart: true });
    await peer.setLocalDescription(offer);

    socket.emit("call_reconnect_offer", { chatId, offer }, (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Reconnect attempt failed.");
      }
    });

    return true;
  } catch {
    return false;
  } finally {
    if (state.call?.chatId === chatId) {
      state.call.reconnectOfferPending = false;
    }
  }
}

async function emitCallRenegotiationOffer() {
  if (!state.call?.peer || state.call.connected !== true) {
    return false;
  }

  if (state.call.renegotiationPending) {
    return false;
  }

  const { chatId, peer } = state.call;

  if (peer.signalingState !== "stable") {
    return false;
  }

  state.call.renegotiationPending = true;

  try {
    const offer = await peer.createOffer();
    await peer.setLocalDescription(offer);

    return await new Promise((resolve) => {
      socket.emit("call_renegotiate_offer", { chatId, offer }, (response) => {
        if (!response?.ok) {
          showToast(response?.error ?? "Could not update call media.");
          resolve(false);
          return;
        }

        resolve(true);
      });
    });
  } catch {
    return false;
  } finally {
    if (state.call?.chatId === chatId) {
      state.call.renegotiationPending = false;
    }
  }
}

async function beginCallReconnect(peer) {
  if (!state.call || state.call.peer !== peer) {
    return;
  }

  if (state.call.reconnecting) {
    return;
  }

  state.call.reconnecting = true;
  state.call.reconnectAttempts = Number(state.call.reconnectAttempts ?? 0) + 1;
  setCallConnected(false, { preserveConnectedAt: true });
  setCallStatus("Connection lost. Reconnecting...");
  setCallQuality("reconnecting");
  clearCallReconnectTimer();

  const reconnectChatId = state.call.chatId;

  callReconnectTimer = window.setTimeout(() => {
    if (!state.call || state.call.chatId !== reconnectChatId) {
      return;
    }

    socket.emit("end_call", { chatId: reconnectChatId, reason: "network_drop" }, () => {});
    showToast("Call dropped due to connection issues.");
    cleanupCall(false);
  }, CALL_RECONNECT_GRACE_MS);

  const started = await emitCallReconnectOffer();

  if (!started && state.call?.chatId === reconnectChatId) {
    setCallStatus("Reconnecting...");
  }
}

function toggleMic() {
  if (!state.call?.localStream) {
    return;
  }

  state.call.micEnabled = !state.call.micEnabled;
  setTrackEnabled(state.call.localStream, "audio", state.call.micEnabled);

  if (!state.call.micEnabled) {
    state.call.localSpeaking = false;
    state.call.localSpeakingUntil = 0;
  }

  emitCallMediaState();
  updateVoiceActivityUi();
  updateCallControlButtons();
}

function toggleCamera() {
  if (!state.call?.localStream || state.call.mode !== "video") {
    return;
  }

  state.call.cameraEnabled = !state.call.cameraEnabled;
  setTrackEnabled(state.call.localStream, "video", state.call.cameraEnabled);
  emitCallMediaState();
  updateCallControlButtons();
}

function cleanupCall(shouldEmitEndSignal = false) {
  clearOutgoingRingTimer();
  clearIncomingRingTimer();
  clearCallDurationTimer();
  clearCallQualityTimer();
  clearCallReconnectTimer();
  clearCallVoiceActivityTimer();
  stopLocalVideoDrag();
  stopRemoteVideoDrag();

  if (!state.call) {
    elements.callOverlay.classList.add("hidden");
    elements.callOverlay.classList.remove("connected");
    elements.callOverlay.classList.remove("awaiting-remote");
    elements.incomingCallPrompt.classList.add("hidden");
    elements.callAnnotationCanvas.classList.remove("draw-enabled");
    elements.callAnnotationCanvas.style.pointerEvents = "none";
    elements.callChatInput.value = "";
    elements.remoteCameraVideo.srcObject = null;
    elements.remoteCameraVideo.classList.add("hidden");
    resetRemoteCameraVideoPosition();
    renderCallDuration();
    renderCallQualityBadge();
    renderCallConnectionState();
    renderRemoteMediaIndicators();
    clearCallAnnotation();
    renderCallChat();
    updateVoiceActivityUi();
    syncCallFocusState();
    updateCallControlButtons();
    return;
  }

  const {
    chatId,
    peer,
    localStream,
    remoteStream,
    screenShareStream,
    screenShareCleanup,
    screenShareSender,
    localVoiceMeter,
    remoteVoiceMeter,
    remoteVideoTracks
  } = state.call;

  if (shouldEmitEndSignal) {
    socket.emit("end_call", { chatId, reason: "ended" }, () => {});
  }

  if (peer) {
    peer.onicecandidate = null;
    peer.ontrack = null;
    peer.onconnectionstatechange = null;
    peer.close();
  }

  if (localStream) {
    for (const track of localStream.getTracks()) {
      track.stop();
    }
  }

  if (remoteStream) {
    for (const track of remoteStream.getTracks()) {
      track.stop();
    }
  }

  if (screenShareStream) {
    for (const track of screenShareStream.getTracks()) {
      track.onended = null;
      track.stop();
    }
  }

  if (screenShareSender && peer) {
    try {
      peer.removeTrack(screenShareSender);
    } catch {}
  }

  if (typeof screenShareCleanup === "function") {
    screenShareCleanup();
  }

  if (remoteVideoTracks && typeof remoteVideoTracks === "object") {
    for (const track of Object.values(remoteVideoTracks)) {
      try {
        track?.stop?.();
      } catch {}
    }
  }

  disposeVoiceMeter(localVoiceMeter);
  disposeVoiceMeter(remoteVoiceMeter);

  elements.localVideo.srcObject = null;
  elements.localVideo.style.left = "";
  elements.localVideo.style.top = "";
  elements.localVideo.style.right = "";
  elements.localVideo.style.bottom = "";
  elements.localVideo.classList.remove("speaking");
  elements.remoteVideo.srcObject = null;
  elements.remoteVideo.classList.remove("speaking");
  elements.remoteCameraVideo.srcObject = null;
  elements.remoteCameraVideo.classList.add("hidden");
  elements.remoteCameraVideo.style.left = "";
  elements.remoteCameraVideo.style.top = "";
  elements.remoteCameraVideo.style.right = "";
  elements.remoteCameraVideo.style.bottom = "";
  elements.remoteCameraVideo.classList.remove("speaking");
  clearCallAnnotation();
  callAnnotationPointerState = null;
  elements.callAnnotationCanvas.classList.remove("draw-enabled");
  elements.callAnnotationCanvas.style.pointerEvents = "none";

  state.call = null;
  state.pendingIncomingCall = null;
  elements.callChatInput.value = "";

  elements.callOverlay.classList.add("hidden");
  elements.callOverlay.classList.remove("connected");
  elements.callOverlay.classList.remove("awaiting-remote");
  elements.incomingCallPrompt.classList.add("hidden");
  renderCallDuration();
  renderCallQualityBadge();
  renderCallConnectionState();
  renderRemoteMediaIndicators();
  renderCallChat();
  updateVoiceActivityUi();
  syncCallFocusState();
  updateCallControlButtons();
}

function createPeerConnection(chatId) {
  const peer = new RTCPeerConnection({
    iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
  });

  peer.onicecandidate = (event) => {
    if (!event.candidate) {
      return;
    }

    socket.emit("call_ice_candidate", {
      chatId,
      candidate: event.candidate
    });
  };

  peer.ontrack = (event) => {
    if (!state.call) {
      return;
    }

    const track = event.track;

    if (!track) {
      return;
    }

    if (track.kind === "audio") {
      const audioStream = new MediaStream([track]);

      if (state.call.remoteVoiceMeter) {
        disposeVoiceMeter(state.call.remoteVoiceMeter);
      }

      state.call.remoteVoiceMeter = createVoiceMeter(audioStream);

      track.onended = () => {
        if (!state.call || state.call.remoteAudioTrackId !== track.id) {
          return;
        }

        if (state.call.remoteVoiceMeter) {
          disposeVoiceMeter(state.call.remoteVoiceMeter);
          state.call.remoteVoiceMeter = null;
        }

        state.call.remoteAudioTrackId = null;
        state.call.remoteSpeaking = false;
        state.call.remoteSpeakingUntil = 0;
        updateVoiceActivityUi();
      };

      state.call.remoteAudioTrackId = track.id;

      if (state.call.connected && !callVoiceActivityTimer) {
        startVoiceActivityMonitor();
      }

      return;
    }

    if (track.kind === "video" && state.call.mode === "video") {
      if (!state.call.remoteVideoTracks) {
        state.call.remoteVideoTracks = {
          screen: null,
          camera: null
        };
      }

      const trackType = inferScreenShareTrack(track) ? "screen" : "camera";
      state.call.remoteVideoTracks[trackType] = track;
      if (trackType === "screen") {
        state.call.remoteScreenSharing = true;
      }

      track.onended = () => {
        if (!state.call || !state.call.remoteVideoTracks) {
          return;
        }

        if (state.call.remoteVideoTracks[trackType]?.id === track.id) {
          state.call.remoteVideoTracks[trackType] = null;
        }

        syncRemoteVideoElements();
        renderRemoteMediaIndicators();
        renderRemoteVideoPlaceholder();
      };

      syncRemoteVideoElements();
      renderRemoteMediaIndicators();
      renderRemoteVideoPlaceholder();
    }
  };

  peer.onconnectionstatechange = () => {
    if (!state.call) {
      return;
    }

    const connectionState = peer.connectionState;

    if (connectionState === "connected") {
      setCallConnected(true);
      clearOutgoingRingTimer();
      setCallStatus("Connected");
      return;
    }

    if (connectionState === "connecting") {
      if (state.call.reconnecting) {
        setCallStatus("Reconnecting...");
      }
      return;
    }

    if (connectionState === "disconnected" || connectionState === "failed") {
      beginCallReconnect(peer);
      return;
    }

    if (connectionState === "closed" && state.call.reconnecting) {
      clearCallReconnectTimer();
    }
  };

  return peer;
}

async function getLocalMedia(mode) {
  if (!navigator.mediaDevices?.getUserMedia) {
    showToast("Your browser does not support calls.");
    return null;
  }

  try {
    return await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: mode === "video"
    });
  } catch {
    showToast("Camera or microphone access was denied.");
    return null;
  }
}

async function startOutgoingCall(mode) {
  if (state.call) {
    showToast("A call is already active.");
    return;
  }

  const target = currentDirectTarget();

  if (!target) {
    showToast("Open a direct chat first.");
    return;
  }

  if (!state.activeChat || state.activeChat.type === "group") {
    showToast("Calls work only in direct chats.");
    return;
  }

  const localStream = await getLocalMedia(mode);

  if (!localStream) {
    return;
  }

  const chatId = state.activeChat.id;
  const peer = createPeerConnection(chatId);

  for (const track of localStream.getTracks()) {
    peer.addTrack(track, localStream);
  }

  state.call = {
    chatId,
    mode,
    peer,
    localStream,
    remoteStream: null,
    remoteName: target.name,
    remoteUserKey: target.key,
    micEnabled: true,
    cameraEnabled: mode === "video",
    remoteMicEnabled: true,
    remoteCameraEnabled: mode === "video",
    remoteScreenSharing: false,
    connected: false,
    ringCount: 0,
    reconnecting: false,
    reconnectAttempts: 0,
    reconnectOfferPending: false,
    renegotiationPending: false,
    screenShareStream: null,
    screenShareCleanup: null,
    screenShareSender: null,
    isScreenSharing: false,
    drawEnabled: false,
    annotationColor: CALL_ANNOTATION_DEFAULT_COLOR,
    annotationWidth: CALL_ANNOTATION_DEFAULT_WIDTH,
    chatOpen: !isMobileViewport(),
    callMessages: [],
    localVideoCorner: preferredLocalVideoCorner,
    screenShareInsetCorner: "top-left",
    screenShareInsetPosition: null,
    remoteVideoTracks: {
      screen: null,
      camera: null
    },
    remoteAudioTrackId: null,
    localVoiceMeter: createVoiceMeter(localStream),
    remoteVoiceMeter: null,
    localSpeaking: false,
    remoteSpeaking: false,
    localSpeakingUntil: 0,
    remoteSpeakingUntil: 0,
    qualityLevel: "checking",
    qualityRttMs: null,
    qualityLossRatio: null
  };

  elements.localVideo.srcObject = localStream;
  startCallUi(mode, target.name, "Calling...");
  emitCallMediaState();
  startOutgoingRingCountdown();

  try {
    const offer = await peer.createOffer();
    await peer.setLocalDescription(offer);

    socket.emit(
      "call_offer",
      {
        chatId,
        mode,
        offer,
        micEnabled: state.call?.micEnabled !== false,
        cameraEnabled: state.call?.cameraEnabled !== false
      },
      (response) => {
        if (!response?.ok) {
          showToast(response?.error ?? "Could not start call.");
          cleanupCall(false);
        }
      }
    );
  } catch {
    showToast("Could not start call.");
    cleanupCall(false);
  }
}

function showIncomingCallPrompt(payload) {
  state.pendingIncomingCall = {
    ...payload,
    ringCount: 0
  };
  elements.incomingCallText.textContent = `${payload.fromName} is calling (${payload.mode}).`;
  elements.incomingCallRings.textContent = "Ringing...";
  elements.incomingCallPrompt.classList.remove("hidden");
  syncCallFocusState();
  startIncomingRingCountdown();
  notifyDesktop("Incoming call", `${payload.fromName} is calling you.`);
}

async function acceptIncomingCall() {
  const incoming = state.pendingIncomingCall;

  if (!incoming) {
    return;
  }

  clearIncomingRingTimer();
  elements.incomingCallPrompt.classList.add("hidden");

  if (!state.activeChat || state.activeChat.id !== incoming.chatId) {
    const loaded = await loadChatAsync(incoming.chatId);

    if (!loaded) {
      socket.emit("reject_call", { chatId: incoming.chatId, reason: "unavailable" }, () => {});
      state.pendingIncomingCall = null;
      syncCallFocusState();
      return;
    }
  }

  if (state.call) {
    socket.emit("reject_call", { chatId: incoming.chatId, reason: "busy" }, () => {});
    state.pendingIncomingCall = null;
    syncCallFocusState();
    return;
  }

  const localStream = await getLocalMedia(incoming.mode);

  if (!localStream) {
    socket.emit("reject_call", { chatId: incoming.chatId, reason: "no_media" }, () => {});
    state.pendingIncomingCall = null;
    syncCallFocusState();
    return;
  }

  const peer = createPeerConnection(incoming.chatId);

  for (const track of localStream.getTracks()) {
    peer.addTrack(track, localStream);
  }

  state.call = {
    chatId: incoming.chatId,
    mode: incoming.mode,
    peer,
    localStream,
    remoteStream: null,
    remoteName: incoming.fromName,
    remoteUserKey: incoming.fromKey ?? null,
    micEnabled: true,
    cameraEnabled: incoming.mode === "video",
    remoteMicEnabled: incoming.remoteMicEnabled !== false,
    remoteCameraEnabled: incoming.remoteCameraEnabled !== false,
    remoteScreenSharing: incoming.screenSharing === true,
    connected: false,
    ringCount: 0,
    reconnecting: false,
    reconnectAttempts: 0,
    reconnectOfferPending: false,
    renegotiationPending: false,
    screenShareStream: null,
    screenShareCleanup: null,
    screenShareSender: null,
    isScreenSharing: false,
    drawEnabled: false,
    annotationColor: CALL_ANNOTATION_DEFAULT_COLOR,
    annotationWidth: CALL_ANNOTATION_DEFAULT_WIDTH,
    chatOpen: !isMobileViewport(),
    callMessages: [],
    localVideoCorner: preferredLocalVideoCorner,
    screenShareInsetCorner: "top-left",
    screenShareInsetPosition: null,
    remoteVideoTracks: {
      screen: null,
      camera: null
    },
    remoteAudioTrackId: null,
    localVoiceMeter: createVoiceMeter(localStream),
    remoteVoiceMeter: null,
    localSpeaking: false,
    remoteSpeaking: false,
    localSpeakingUntil: 0,
    remoteSpeakingUntil: 0,
    qualityLevel: "checking",
    qualityRttMs: null,
    qualityLossRatio: null
  };

  elements.localVideo.srcObject = localStream;
  startCallUi(incoming.mode, incoming.fromName, "Connecting...");
  emitCallMediaState();

  try {
    await peer.setRemoteDescription(new RTCSessionDescription(incoming.offer));
    const answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);

    socket.emit(
      "call_answer",
      {
        chatId: incoming.chatId,
        answer,
        micEnabled: state.call?.micEnabled !== false,
        cameraEnabled: state.call?.cameraEnabled !== false
      },
      (response) => {
        if (!response?.ok) {
          showToast(response?.error ?? "Could not answer call.");
          cleanupCall(false);
        }
      }
    );
  } catch {
    showToast("Could not answer call.");
    cleanupCall(false);
  } finally {
    state.pendingIncomingCall = null;
    syncCallFocusState();
  }
}

function declineIncomingCall(reason = "declined") {
  if (!state.pendingIncomingCall) {
    return;
  }

  clearIncomingRingTimer();

  socket.emit(
    "reject_call",
    {
      chatId: state.pendingIncomingCall.chatId,
      reason
    },
    () => {}
  );

  state.pendingIncomingCall = null;
  elements.incomingCallPrompt.classList.add("hidden");
  syncCallFocusState();
}

elements.loginTabButton.addEventListener("click", () => {
  setAuthMode("login");
});

elements.signupTabButton.addEventListener("click", () => {
  setAuthMode("signup");
});

if (elements.authPasswordToggleButton) {
  elements.authPasswordToggleButton.addEventListener("click", () => {
    const shouldShow = elements.authPasswordInput.type === "password";
    setAuthPasswordVisibility(shouldShow);
    elements.authPasswordInput.focus();
  });
}

if (elements.authConfirmToggleButton) {
  elements.authConfirmToggleButton.addEventListener("click", () => {
    const shouldShow = elements.authConfirmInput.type === "password";
    setAuthConfirmPasswordVisibility(shouldShow);
    elements.authConfirmInput.focus();
  });
}

elements.authForm.addEventListener("submit", (event) => {
  event.preventDefault();

  const username = elements.authUsernameInput.value.trim();
  const password = elements.authPasswordInput.value;

  if (!username || !password) {
    showToast("Enter username and password.");
    return;
  }

  if (state.authMode === "signup") {
    const confirmPassword = elements.authConfirmInput.value;

    if (password !== confirmPassword) {
      showToast("Passwords do not match.");
      return;
    }
  }

  const eventName = state.authMode === "signup" ? "signup" : "login";

  socket.emit(
    eventName,
    { username, password, deviceName: currentDeviceName() },
    (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Authentication failed.");
        return;
      }

      completeAuth(response);
    }
  );
});

elements.friendForm.addEventListener("submit", (event) => {
  event.preventDefault();

  if (!state.currentUser) {
    return;
  }

  const username = elements.friendInput.value.trim();

  if (!username) {
    return;
  }

  socket.emit("send_friend_request", { username }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not send request.");
      return;
    }

    elements.friendInput.value = "";
    showToast("Friend request sent.");
  });
});

elements.toggleGroupPanelButton.addEventListener("click", () => {
  toggleGroupQuickPopover();
});

elements.createGroupFromDirectButton.addEventListener("click", () => {
  createGroupFromDirect();
});

elements.closeGroupCreateModalButton.addEventListener("click", () => {
  hideGroupQuickPopover();
});

elements.groupCreateSearchInput.addEventListener("input", () => {
  renderGroupCreateModal();
});

elements.groupCreateModal.addEventListener("click", (event) => {
  if (event.target === elements.groupCreateModal) {
    hideGroupQuickPopover();
  }
});

elements.composerForm.addEventListener("submit", (event) => {
  event.preventDefault();
  submitComposerMessage();
});

elements.composerForm.addEventListener("dragenter", (event) => {
  if (!hasFileDragPayload(event.dataTransfer)) {
    return;
  }

  event.preventDefault();
  composerDragDepth += 1;

  if (composerCanAcceptFiles()) {
    setComposerDragActive(true);
  }
});

elements.composerForm.addEventListener("dragover", (event) => {
  if (!hasFileDragPayload(event.dataTransfer)) {
    return;
  }

  event.preventDefault();

  if (event.dataTransfer) {
    event.dataTransfer.dropEffect = "copy";
  }

  if (composerCanAcceptFiles()) {
    setComposerDragActive(true);
  }
});

elements.composerForm.addEventListener("dragleave", () => {
  if (composerDragDepth <= 0) {
    return;
  }

  composerDragDepth = Math.max(0, composerDragDepth - 1);
  if (composerDragDepth === 0) {
    setComposerDragActive(false);
  }
});

elements.composerForm.addEventListener("drop", async (event) => {
  if (!hasFileDragPayload(event.dataTransfer)) {
    return;
  }

  event.preventDefault();
  resetComposerDragState();

  if (!composerCanAcceptFiles()) {
    return;
  }

  await addSelectedFiles(event.dataTransfer?.files ?? []);
});

elements.messageInput.addEventListener("paste", async (event) => {
  const files = [...(event.clipboardData?.files ?? [])];

  if (files.length === 0 || !composerCanAcceptFiles()) {
    return;
  }

  event.preventDefault();
  await addSelectedFiles(files);
});

elements.messageSearchInput.addEventListener("input", () => {
  state.messageSearchQuery = elements.messageSearchInput.value;
  renderMessages(state.activeChat);
});

if (elements.jumpToUnreadButton) {
  elements.jumpToUnreadButton.addEventListener("click", () => {
    if (!state.activeUnreadAnchorMessageId) {
      return;
    }

    scrollToMessage(state.activeUnreadAnchorMessageId);
  });
}

elements.messages.addEventListener("scroll", () => {
  updateJumpToUnreadButton();
});

elements.attachButton.addEventListener("click", () => {
  toggleAttachMenu();
});

elements.attachUploadButton.addEventListener("click", () => {
  closeAttachMenu();
  elements.fileInput.click();
});

elements.attachPhotoButton.addEventListener("click", () => {
  closeAttachMenu();

  if (!isMobileViewport()) {
    showToast("Photo picker is optimized for mobile devices.");
  }

  elements.photoInput.click();
});

elements.attachCameraButton.addEventListener("click", () => {
  closeAttachMenu();
  elements.cameraInput.click();
});

elements.stickerButton.addEventListener("click", () => {
  toggleStickerMenu();
});

elements.closeStickerMenuButton.addEventListener("click", () => {
  closeStickerMenu();
});

elements.emojiTabButton.addEventListener("click", () => {
  openStickerMenu("emoji");
});

elements.gifTabButton.addEventListener("click", () => {
  openStickerMenu("gif");
});

elements.emojiSearchInput.addEventListener("input", () => {
  renderEmojiPickerResults();
});

elements.stickerEmojiGrid.addEventListener("click", (event) => {
  const button = event.target.closest(".sticker-emoji");

  if (!button) {
    return;
  }

  const emoji = String(button.getAttribute("data-emoji") ?? "");

  if (!emoji) {
    return;
  }

  const insert = `${buildSpacePrefixedInsert(emoji)} `;
  insertTextAtCursor(elements.messageInput, insert);
});

elements.gifSearchForm.addEventListener("submit", (event) => {
  event.preventDefault();
  searchTenorGifs(elements.gifSearchInput.value);
});

elements.gifResults.addEventListener("click", (event) => {
  const button = event.target.closest(".gif-result");

  if (!button) {
    return;
  }

  const gifUrl = String(button.getAttribute("data-gif-url") ?? "").trim();

  if (!gifUrl) {
    return;
  }

  sendGifAttachmentFromUrl(gifUrl);
});

elements.fileInput.addEventListener("change", async () => {
  await addSelectedFiles(elements.fileInput.files);
  elements.fileInput.value = "";
});

elements.photoInput.addEventListener("change", async () => {
  await addSelectedFiles(elements.photoInput.files);
  elements.photoInput.value = "";
});

elements.cameraInput.addEventListener("change", async () => {
  await addSelectedFiles(elements.cameraInput.files, { sendImmediately: true });
  elements.cameraInput.value = "";
});

elements.messageInput.addEventListener("input", () => {
  resizeComposerInput();
  storeActiveChatDraft();
  touchLocalTypingState();
});

elements.messageInput.addEventListener("focus", () => {
  touchLocalTypingState();
});

elements.messageInput.addEventListener("keydown", (event) => {
  const shouldSend =
    event.key === "Enter" &&
    !event.shiftKey &&
    !event.altKey &&
    !event.ctrlKey &&
    !event.metaKey &&
    !event.isComposing;

  if (state.enterToSendEnabled && shouldSend) {
    event.preventDefault();

    if (state.activeChat && !elements.messageInput.disabled) {
      submitComposerMessage();
    }
  }

  window.requestAnimationFrame(() => {
    storeActiveChatDraft();
    resizeComposerInput();
    touchLocalTypingState();
  });
});

elements.messageInput.addEventListener("blur", () => {
  storeActiveChatDraft();
  touchLocalTypingState();
});

elements.voiceCallButton.addEventListener("click", () => {
  startOutgoingCall("voice");
});

elements.videoCallButton.addEventListener("click", () => {
  startOutgoingCall("video");
});

if (elements.chatPrefsButton) {
  elements.chatPrefsButton.addEventListener("click", (event) => {
    openActiveChatStyleMenu(event.currentTarget);
  });
}

elements.openTempChatButton.addEventListener("click", () => {
  const target = currentDirectTarget();

  if (!target) {
    showToast("Open a direct chat first.");
    return;
  }

  openChatWith(target, true);
});

elements.closeTempChatButton.addEventListener("click", () => {
  voteCloseTempChat();
});

if (elements.attachVoiceNoteButton) {
  elements.attachVoiceNoteButton.addEventListener("click", () => {
    toggleVoiceNoteRecording();
  });
}

elements.settingsButton.addEventListener("click", () => {
  openSettingsModal();
});

elements.closeSettingsButton.addEventListener("click", () => {
  elements.settingsModal.classList.add("hidden");
});

elements.inlinePromptForm.addEventListener("submit", (event) => {
  event.preventDefault();
  closeInlinePrompt(elements.inlinePromptInput.value);
});

elements.inlinePromptCancelButton.addEventListener("click", () => {
  closeInlinePrompt(null);
});

elements.inlinePromptModal.addEventListener("click", (event) => {
  if (event.target === elements.inlinePromptModal) {
    closeInlinePrompt(null);
  }
});

elements.inlinePromptInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && elements.inlinePromptInput.classList.contains("single-line")) {
    event.preventDefault();
    closeInlinePrompt(elements.inlinePromptInput.value);
  }
});

elements.settingsDisplayNameInput.addEventListener("input", () => {
  renderSettingsAvatarPreview();
});

elements.settingsAvatarInput.addEventListener("change", async () => {
  const [file] = [...(elements.settingsAvatarInput.files ?? [])];

  if (!file) {
    return;
  }

  if (file.size > 2 * 1024 * 1024) {
    showToast("Avatar must be 2 MB or smaller.");
    elements.settingsAvatarInput.value = "";
    return;
  }

  try {
    const dataUrl = await readFileAsDataUrl(file);

    if (!dataUrl.startsWith("data:image/")) {
      showToast("Avatar must be an image file.");
      return;
    }

    state.settingsAvatarDataUrl = dataUrl;
    renderSettingsAvatarPreview();
  } catch {
    showToast("Could not read avatar file.");
  } finally {
    elements.settingsAvatarInput.value = "";
  }
});

elements.saveProfileButton.addEventListener("click", () => {
  if (!state.currentUser) {
    return;
  }

  const displayName = elements.settingsDisplayNameInput.value.trim();

  if (!displayName) {
    showToast("Enter a username.");
    return;
  }

  socket.emit(
    "update_profile",
    {
      displayName,
      avatarDataUrl: state.settingsAvatarDataUrl
    },
    (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Could not update profile.");
        return;
      }

      state.currentUser = response.user;
      updateSelfStrip();
      renderRelationshipState();
      renderActiveChat();
      showToast("Profile updated.");
    }
  );
});

elements.changePasswordButton.addEventListener("click", () => {
  const currentPassword = elements.settingsCurrentPasswordInput.value;
  const newPassword = elements.settingsNewPasswordInput.value;

  if (!currentPassword || !newPassword) {
    showToast("Enter current and new password.");
    return;
  }

  socket.emit("change_password", { currentPassword, newPassword }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not change password.");
      return;
    }

    elements.settingsCurrentPasswordInput.value = "";
    elements.settingsNewPasswordInput.value = "";
    showToast("Password changed.");
  });
});

if (elements.revokeOtherSessionsButton) {
  elements.revokeOtherSessionsButton.addEventListener("click", () => {
    if (!state.currentUser || state.settingsSessionsLoading) {
      return;
    }

    state.settingsSessionsLoading = true;
    renderSettingsSessions();

    socket.emit("revoke_other_sessions", {}, (response) => {
      state.settingsSessionsLoading = false;

      if (!response?.ok) {
        showToast(response?.error ?? "Could not sign out other devices.");
        renderSettingsSessions();
        return;
      }

      state.settingsSessions = Array.isArray(response.sessions) ? response.sessions : [];
      renderSettingsSessions();

      const revokedCount = Number(response?.revokedCount ?? 0);

      if (revokedCount > 0) {
        showToast(`Signed out ${revokedCount} device${revokedCount === 1 ? "" : "s"}.`);
        return;
      }

      showToast("No other devices were signed in.");
    });
  });
}

elements.deleteAccountButton.addEventListener("click", () => {
  const password = elements.settingsDeletePasswordInput.value;

  if (!password) {
    showToast("Enter password to delete account.");
    return;
  }

  const confirmed = window.confirm("Delete your account permanently?");

  if (!confirmed) {
    return;
  }

  socket.emit("delete_account", { password }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not delete account.");
      return;
    }

    setSessionId(null);
    showToast("Account deleted.");
    cleanupCall(false);
    socket.disconnect();
    window.location.reload();
  });
});

elements.enableNotificationsButton.addEventListener("click", async () => {
  if (!("Notification" in window)) {
    showToast("Desktop notifications are not supported in this browser.");
    updateNotificationToggleButton();
    return;
  }

  if (Notification.permission === "denied") {
    setNotificationsEnabled(false);
    showToast("Desktop notifications are blocked by your browser settings.");
    return;
  }

  if (Notification.permission === "granted") {
    const nextEnabled = !state.notificationsEnabled;
    setNotificationsEnabled(nextEnabled);
    showToast(nextEnabled ? "Notifications enabled." : "Notifications disabled.");
    return;
  }

  if (!state.notificationsEnabled) {
    setNotificationsEnabled(true);
  }

  const permission = await Notification.requestPermission();

  if (permission === "granted") {
    setNotificationsEnabled(true);
    showToast("Notifications enabled.");
    return;
  }

  setNotificationsEnabled(false);
  showToast("Notifications blocked.");
});

if (elements.toggleEnterToSendButton) {
  elements.toggleEnterToSendButton.addEventListener("click", () => {
    const nextEnabled = !state.enterToSendEnabled;
    setEnterToSendEnabled(nextEnabled);
    showToast(nextEnabled ? "Enter now sends messages." : "Enter now inserts a new line.");
  });
}

elements.logoutButton.addEventListener("click", () => {
  const sessionId = state.sessionId;
  setSessionId(null);
  socket.emit("logout", { sessionId }, () => {});
  cleanupCall(false);
  socket.disconnect();
  window.location.reload();
});

elements.mobileSidebarButton.addEventListener("click", () => {
  setSidebarOpen(!state.sidebarOpen);
});

elements.sidebarBackdrop.addEventListener("click", () => {
  setSidebarOpen(false);
});

elements.acceptCallButton.addEventListener("click", () => {
  acceptIncomingCall();
});

elements.declineCallButton.addEventListener("click", () => {
  declineIncomingCall();
});

elements.toggleMicButton.addEventListener("click", () => {
  toggleMic();
});

elements.toggleCameraButton.addEventListener("click", () => {
  toggleCamera();
});

elements.toggleScreenShareButton.addEventListener("click", () => {
  toggleScreenShare();
});

elements.toggleDrawButton.addEventListener("click", () => {
  toggleCallDraw();
});

elements.clearDrawButton.addEventListener("click", () => {
  clearCallDrawAndSync();
});

elements.toggleCallChatButton.addEventListener("click", () => {
  toggleCallChatPanel();
});

elements.callChatForm.addEventListener("submit", (event) => {
  event.preventDefault();
  sendCallChatMessage();
});

elements.endCallButton.addEventListener("click", () => {
  cleanupCall(true);
});

elements.localVideo.addEventListener("pointerdown", (event) => {
  if (state.call?.drawEnabled) {
    return;
  }

  beginLocalVideoDrag(event);
});

elements.remoteCameraVideo.addEventListener("pointerdown", (event) => {
  beginRemoteVideoDrag(event);
});

elements.callAnnotationCanvas.addEventListener("pointerdown", (event) => {
  beginCallAnnotationStroke(event);
});

elements.callAnnotationCanvas.addEventListener("pointermove", (event) => {
  updateCallAnnotationStroke(event);
});

elements.callAnnotationCanvas.addEventListener("pointerup", (event) => {
  endCallAnnotationStroke(event);
});

elements.callAnnotationCanvas.addEventListener("pointercancel", (event) => {
  endCallAnnotationStroke(event);
});

window.addEventListener("resize", () => {
  if (!isMobileViewport()) {
    setSidebarOpen(false);
  }

  syncAttachMenuButtonState();
  stopLocalVideoDrag();
  stopRemoteVideoDrag();
  resizeCallAnnotationCanvas();
  renderCallChatVisibility();
  hideContextMenu();
  updateJumpToUnreadButton();
});

window.addEventListener("blur", () => {
  stopLocalVideoDrag();
  stopRemoteVideoDrag();
  callAnnotationPointerState = null;
  hideContextMenu();
  closeAttachMenu();
});

window.addEventListener(
  "scroll",
  () => {
    hideContextMenu();
  },
  true
);

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    if (!elements.inlinePromptModal.classList.contains("hidden")) {
      closeInlinePrompt(null);
      return;
    }

    if (!elements.attachMenu.classList.contains("hidden")) {
      closeAttachMenu();
      return;
    }

    if (!elements.stickerMenu.classList.contains("hidden")) {
      closeStickerMenu();
      return;
    }

    hideContextMenu();
    hideGroupQuickPopover();
  }
});

window.addEventListener("focus", () => {
  markActiveChatRead();
});

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    markActiveChatRead();
  }
});

document.addEventListener("pointerdown", (event) => {
  if (!contextMenuElement.classList.contains("hidden") && !contextMenuElement.contains(event.target)) {
    hideContextMenu();
  }

  if (!elements.stickerMenu.classList.contains("hidden")) {
    const clickedInsideStickerMenu =
      elements.stickerMenu.contains(event.target) || elements.stickerButton.contains(event.target);

    if (!clickedInsideStickerMenu) {
      closeStickerMenu();
    }
  }

  if (!elements.attachMenu.classList.contains("hidden")) {
    const clickedInsideAttachMenu =
      elements.attachMenu.contains(event.target) || elements.attachButton.contains(event.target);

    if (!clickedInsideAttachMenu) {
      closeAttachMenu();
    }
  }
});

socket.on("connect", () => {
  tryResumeSession();
});

socket.on("relationship_state", (payload) => {
  state.relationships = {
    friends: Array.isArray(payload?.friends) ? payload.friends : [],
    incomingRequests: Array.isArray(payload?.incomingRequests) ? payload.incomingRequests : [],
    outgoingRequests: Array.isArray(payload?.outgoingRequests) ? payload.outgoingRequests : []
  };

  renderRelationshipState();
  renderProfilePanel();
  updateHeaderActions();
});

socket.on("account_updated", (payload) => {
  const user = payload?.user;

  if (!user || !state.currentUser || user.key !== state.currentUser.key) {
    return;
  }

  state.currentUser = user;
  updateSelfStrip();
  renderRelationshipState();
  renderActiveChat();

  if (!elements.settingsModal.classList.contains("hidden")) {
    openSettingsModal();
  }
});

socket.on("account_deleted", () => {
  setSessionId(null);
  cleanupCall(false);
  showToast("Your account was deleted.");
  socket.disconnect();
  window.location.reload();
});

socket.on("sessions_updated", (payload) => {
  if (!state.currentUser) {
    return;
  }

  state.settingsSessions = Array.isArray(payload?.sessions) ? payload.sessions : [];
  state.settingsSessionsLoading = false;

  if (!elements.settingsModal.classList.contains("hidden")) {
    renderSettingsSessions();
  }
});

socket.on("session_revoked", (payload) => {
  const revokedSessionId = String(payload?.sessionId ?? "").trim();

  if (state.sessionId && revokedSessionId && revokedSessionId !== state.sessionId) {
    return;
  }

  setSessionId(null);
  cleanupCall(false);
  showToast("This device was signed out.");
  socket.disconnect();
  window.location.reload();
});

socket.on("chat_summaries", (payload) => {
  state.chatSummaries = Array.isArray(payload) ? payload : [];

  const activeStillExists = state.activeChat
    ? state.chatSummaries.some((summary) => summary.id === state.activeChat.id)
    : true;

  if (!activeStillExists) {
    clearStoredLastOpenChatIdIfMatches(state.activeChat?.id);
    state.activeChat = null;
    renderActiveChat();
  }

  const knownChatIds = new Set(state.chatSummaries.map((summary) => summary.id));
  for (const chatId of Object.keys(state.unreadByChatId)) {
    if (!knownChatIds.has(chatId)) {
      delete state.unreadByChatId[chatId];
    }
  }

  for (const chatId of Object.keys(state.unreadAnchorByChatId)) {
    if (!knownChatIds.has(chatId)) {
      delete state.unreadAnchorByChatId[chatId];
    }
  }

  if (!state.activeChat && !state.hasAttemptedLastChatRestore) {
    const pendingChatId = String(state.pendingLastOpenChatId ?? "").trim();
    const hasPendingChat =
      pendingChatId.length > 0 &&
      state.chatSummaries.some((summary) => String(summary.id) === pendingChatId);

    state.hasAttemptedLastChatRestore = true;

    if (hasPendingChat) {
      loadChat(pendingChatId, (loaded) => {
        if (!loaded) {
          clearStoredLastOpenChatIdIfMatches(pendingChatId);
        }
      });
    } else if (pendingChatId) {
      clearStoredLastOpenChatIdIfMatches(pendingChatId);
    }
  }

  renderChatList();
});

socket.on("chat_updated", (chat) => {
  if (!chat) {
    return;
  }

  if (state.activeChat?.id === chat.id) {
    state.activeChat = chat;
    renderActiveChat();
    markActiveChatRead();
  }
});

socket.on("chat_removed", (payload) => {
  const chatId = String(payload?.chatId ?? "");

  if (!chatId) {
    return;
  }

  delete state.unreadByChatId[chatId];
  delete state.unreadAnchorByChatId[chatId];
  clearStoredLastOpenChatIdIfMatches(chatId);

  if (state.activeChat?.id === chatId) {
    state.activeChat = null;
    renderActiveChat();
  }

  delete state.typingByChatId[chatId];

  showToast("Chat removed.");
});

socket.on("incoming_message", (payload) => {
  if (!payload) {
    return;
  }

  const isActive = state.activeChat?.id === payload.chatId;

  if (!isActive) {
    state.unreadByChatId[payload.chatId] = Number(state.unreadByChatId[payload.chatId] ?? 0) + 1;
    renderChatList();
    showToast(`${payload.from}: ${payload.preview}`);
    notifyDesktop(payload.from, payload.preview);
    return;
  }

  if (document.visibilityState !== "visible") {
    state.unreadByChatId[payload.chatId] = Number(state.unreadByChatId[payload.chatId] ?? 0) + 1;
    renderChatList();
    notifyDesktop(payload.from, payload.preview);
    return;
  }

  markActiveChatRead();
});

socket.on("typing_state", (payload) => {
  if (!payload) {
    return;
  }

  applyTypingState(payload);
});

socket.on("chat_invite", (payload) => {
  if (!payload) {
    return;
  }

  const message = `${payload.from} started ${payload.isTemp ? "a temp chat" : "a DM"} with you.`;
  showToast(message);
  notifyDesktop("New chat", message);
});

socket.on("group_created", (payload) => {
  if (!payload) {
    return;
  }

  const message = `${payload.from} added you to ${payload.groupName}.`;
  showToast(message);
  notifyDesktop("Group invitation", message);
});

socket.on("notification", (payload) => {
  const message = String(payload?.message ?? "").trim();

  if (!message) {
    return;
  }

  showToast(message);
  notifyDesktop("Shadow Chat", message);
});

socket.on("temp_chat_deleted", (payload) => {
  if (!payload) {
    return;
  }

  clearStoredLastOpenChatIdIfMatches(payload.chatId);

  if (state.activeChat?.id === payload.chatId) {
    state.activeChat = null;
    renderActiveChat();
  }

  showToast("Temp chat deleted after both users agreed.");
});

socket.on("incoming_call", (payload) => {
  if (!payload) {
    return;
  }

  if (state.call || state.pendingIncomingCall) {
    socket.emit("reject_call", { chatId: payload.chatId, reason: "busy" }, () => {});
    return;
  }

  showIncomingCallPrompt(payload);
});

socket.on("call_answer", async (payload) => {
  if (!payload || !state.call || state.call.chatId !== payload.chatId) {
    return;
  }

  try {
    await state.call.peer.setRemoteDescription(new RTCSessionDescription(payload.answer));
    if (typeof payload.micEnabled === "boolean") {
      state.call.remoteMicEnabled = payload.micEnabled;
    }
    if (typeof payload.cameraEnabled === "boolean") {
      state.call.remoteCameraEnabled = payload.cameraEnabled;
    }
    renderRemoteMediaIndicators();
    setCallConnected(true);
    clearOutgoingRingTimer();
    setCallStatus("Connected");
  } catch {
    showToast("Call connection failed.");
    cleanupCall(false);
  }
});

socket.on("call_reconnect_offer", async (payload) => {
  if (!payload || !state.call || state.call.chatId !== payload.chatId) {
    return;
  }

  try {
    setCallStatus("Reconnecting...");
    setCallQuality("reconnecting");
    await state.call.peer.setRemoteDescription(new RTCSessionDescription(payload.offer));
    const answer = await state.call.peer.createAnswer();
    await state.call.peer.setLocalDescription(answer);

    socket.emit("call_reconnect_answer", { chatId: payload.chatId, answer }, (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Could not send reconnect answer.");
      }
    });
  } catch {
    showToast("Reconnect failed.");
  }
});

socket.on("call_reconnect_answer", async (payload) => {
  if (!payload || !state.call || state.call.chatId !== payload.chatId) {
    return;
  }

  try {
    setCallStatus("Reconnecting...");
    setCallQuality("reconnecting");
    await state.call.peer.setRemoteDescription(new RTCSessionDescription(payload.answer));
  } catch {
    showToast("Reconnect failed.");
  }
});

socket.on("call_renegotiate_offer", async (payload) => {
  if (!payload || !state.call || state.call.chatId !== payload.chatId) {
    return;
  }

  try {
    await state.call.peer.setRemoteDescription(new RTCSessionDescription(payload.offer));
    const answer = await state.call.peer.createAnswer();
    await state.call.peer.setLocalDescription(answer);

    socket.emit("call_renegotiate_answer", { chatId: payload.chatId, answer }, (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Could not sync call media.");
      }
    });
  } catch {
    showToast("Could not sync call media.");
  }
});

socket.on("call_renegotiate_answer", async (payload) => {
  if (!payload || !state.call || state.call.chatId !== payload.chatId) {
    return;
  }

  try {
    await state.call.peer.setRemoteDescription(new RTCSessionDescription(payload.answer));
    syncRemoteVideoElements();
    renderRemoteMediaIndicators();
    renderRemoteVideoPlaceholder();
  } catch {
    showToast("Could not apply call media update.");
  }
});

socket.on("call_ice_candidate", async (payload) => {
  if (!payload || !state.call || state.call.chatId !== payload.chatId) {
    return;
  }

  try {
    await state.call.peer.addIceCandidate(payload.candidate);
  } catch {
    showToast("Network issue during call.");
  }
});

socket.on("call_rejected", (payload) => {
  if (!payload) {
    return;
  }

  if (state.call && state.call.chatId === payload.chatId) {
    showToast(callEndMessageForReason(payload.reason, "Call was declined."));
    cleanupCall(false);
    return;
  }

  if (state.pendingIncomingCall && state.pendingIncomingCall.chatId === payload.chatId) {
    clearIncomingRingTimer();
    state.pendingIncomingCall = null;
    elements.incomingCallPrompt.classList.add("hidden");
    syncCallFocusState();
    showToast(callEndMessageForReason(payload.reason, "Call was declined."));
  }
});

socket.on("call_ended", (payload) => {
  if (!payload) {
    return;
  }

  if (state.call && state.call.chatId === payload.chatId) {
    showToast(callEndMessageForReason(payload.reason, "Call ended."));
    cleanupCall(false);
    return;
  }

  if (state.pendingIncomingCall && state.pendingIncomingCall.chatId === payload.chatId) {
    clearIncomingRingTimer();
    state.pendingIncomingCall = null;
    elements.incomingCallPrompt.classList.add("hidden");
    syncCallFocusState();
    showToast(callEndMessageForReason(payload.reason, "Call ended."));
  }
});

socket.on("call_media_state", (payload) => {
  if (!payload) {
    return;
  }

  const chatId = String(payload.chatId ?? "");

  if (state.call && state.call.chatId === chatId) {
    if (typeof payload.micEnabled === "boolean") {
      state.call.remoteMicEnabled = payload.micEnabled;
      if (!payload.micEnabled) {
        state.call.remoteSpeaking = false;
        state.call.remoteSpeakingUntil = 0;
      }
    }
    if (typeof payload.cameraEnabled === "boolean") {
      state.call.remoteCameraEnabled = payload.cameraEnabled;
    }
    if (typeof payload.screenSharing === "boolean") {
      state.call.remoteScreenSharing = payload.screenSharing;
    }
    syncRemoteVideoElements();
    renderRemoteMediaIndicators();
    updateVoiceActivityUi();
    return;
  }

  if (state.pendingIncomingCall && state.pendingIncomingCall.chatId === chatId) {
    if (typeof payload.micEnabled === "boolean") {
      state.pendingIncomingCall.remoteMicEnabled = payload.micEnabled;
    }
    if (typeof payload.cameraEnabled === "boolean") {
      state.pendingIncomingCall.remoteCameraEnabled = payload.cameraEnabled;
    }
    if (typeof payload.screenSharing === "boolean") {
      state.pendingIncomingCall.screenSharing = payload.screenSharing;
    }
  }
});

socket.on("call_annotation", (payload) => {
  applyRemoteCallAnnotation(payload);
});

socket.on("call_chat_message", (payload) => {
  if (!payload || !state.call || String(payload.chatId ?? "") !== state.call.chatId) {
    return;
  }

  if (!Array.isArray(state.call.callMessages)) {
    state.call.callMessages = [];
  }

  state.call.callMessages.push({
    id: String(payload.message?.id ?? crypto.randomUUID()),
    senderKey: String(payload.message?.senderKey ?? "system"),
    senderName: String(payload.message?.senderName ?? "Unknown"),
    text: String(payload.message?.text ?? ""),
    sentAt: String(payload.message?.sentAt ?? new Date().toISOString())
  });

  if (state.call.callMessages.length > CALL_CHAT_MESSAGE_LIMIT * 2) {
    state.call.callMessages = state.call.callMessages.slice(-CALL_CHAT_MESSAGE_LIMIT * 2);
  }

  renderCallChat();
});

socket.on("disconnect", () => {
  state.isResumingSession = false;
  state.settingsSessionsLoading = false;
  state.typingByChatId = {};
  if (state.voiceNoteRecorder) {
    stopVoiceNoteRecording(false);
  }
  stopLocalTyping();
  renderTypingIndicator();
  renderSettingsSessions();
  cleanupCall(false);
  showToast("Disconnected from server.");
});

socket.on("connect_error", () => {
  showToast("Connection error.");
});

tryResumeSession();
setAuthMode("login");
updateSelfStrip();
setComposerEnabled(false);
renderProfilePanel();
updateHeaderActions();
updateCallControlButtons();
syncCallFocusState();
updateNotificationToggleButton();
updateEnterToSendToggleButton();
renderSettingsSessions();
renderReplyPreview();
renderTypingIndicator();
renderCallQualityBadge();
renderCallChat();
syncAttachMenuButtonState();
renderGifResults();
updateJumpToUnreadButton();
window.setInterval(pruneTypingState, 1000);
