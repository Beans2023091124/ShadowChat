const socket = io();
const SESSION_STORAGE_KEY = "shadow_chat_session_id";

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
  activeChat: null,
  profileUserKey: null,
  pendingAttachments: [],
  unreadByChatId: {},
  sidebarOpen: false,
  pendingIncomingCall: null,
  call: null,
  settingsAvatarDataUrl: null,
  isResumingSession: false
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
  authConfirmRow: document.getElementById("authConfirmRow"),
  authConfirmInput: document.getElementById("authConfirmInput"),
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
  groupPanel: document.getElementById("groupPanel"),
  groupForm: document.getElementById("groupForm"),
  groupNameInput: document.getElementById("groupNameInput"),
  groupMembersSelect: document.getElementById("groupMembersSelect"),

  selfAvatar: document.getElementById("selfAvatar"),
  selfName: document.getElementById("selfName"),
  settingsButton: document.getElementById("settingsButton"),

  chatTitle: document.getElementById("chatTitle"),
  chatSubtitle: document.getElementById("chatSubtitle"),
  voiceCallButton: document.getElementById("voiceCallButton"),
  videoCallButton: document.getElementById("videoCallButton"),
  openTempChatButton: document.getElementById("openTempChatButton"),
  closeTempChatButton: document.getElementById("closeTempChatButton"),
  messages: document.getElementById("messages"),
  attachmentPreview: document.getElementById("attachmentPreview"),
  composerForm: document.getElementById("composerForm"),
  attachButton: document.getElementById("attachButton"),
  fileInput: document.getElementById("fileInput"),
  messageInput: document.getElementById("messageInput"),
  sendButton: document.getElementById("sendButton"),

  profileEmpty: document.getElementById("profileEmpty"),
  profileContent: document.getElementById("profileContent"),
  profileAvatar: document.getElementById("profileAvatar"),
  profileName: document.getElementById("profileName"),
  profileState: document.getElementById("profileState"),
  profileMeta: document.getElementById("profileMeta"),

  settingsModal: document.getElementById("settingsModal"),
  closeSettingsButton: document.getElementById("closeSettingsButton"),
  settingsAvatarPreview: document.getElementById("settingsAvatarPreview"),
  settingsAvatarInput: document.getElementById("settingsAvatarInput"),
  settingsDisplayNameInput: document.getElementById("settingsDisplayNameInput"),
  saveProfileButton: document.getElementById("saveProfileButton"),
  settingsCurrentPasswordInput: document.getElementById("settingsCurrentPasswordInput"),
  settingsNewPasswordInput: document.getElementById("settingsNewPasswordInput"),
  changePasswordButton: document.getElementById("changePasswordButton"),
  settingsDeletePasswordInput: document.getElementById("settingsDeletePasswordInput"),
  deleteAccountButton: document.getElementById("deleteAccountButton"),
  enableNotificationsButton: document.getElementById("enableNotificationsButton"),
  logoutButton: document.getElementById("logoutButton"),

  incomingCallPrompt: document.getElementById("incomingCallPrompt"),
  incomingCallText: document.getElementById("incomingCallText"),
  incomingCallRings: document.getElementById("incomingCallRings"),
  acceptCallButton: document.getElementById("acceptCallButton"),
  declineCallButton: document.getElementById("declineCallButton"),

  callOverlay: document.getElementById("callOverlay"),
  callTitle: document.getElementById("callTitle"),
  callStatus: document.getElementById("callStatus"),
  remoteVideo: document.getElementById("remoteVideo"),
  localVideo: document.getElementById("localVideo"),
  toggleMicButton: document.getElementById("toggleMicButton"),
  toggleCameraButton: document.getElementById("toggleCameraButton"),
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
const RING_INTERVAL_MS = 2000;

let toastTimeout;
let outgoingRingTimer;
let incomingRingTimer;

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

function completeAuth(response) {
  state.isResumingSession = false;
  state.currentUser = response.user;
  setSessionId(response.sessionId ?? state.sessionId);
  elements.authOverlay.classList.add("hidden");

  elements.authUsernameInput.value = "";
  elements.authPasswordInput.value = "";
  elements.authConfirmInput.value = "";

  updateSelfStrip();
  renderRelationshipState();
  renderChatList();
  renderActiveChat();
}

function clearClientSessionState() {
  state.isResumingSession = false;
  state.currentUser = null;
  state.activeChat = null;
  state.profileUserKey = null;
  state.relationships = {
    friends: [],
    incomingRequests: [],
    outgoingRequests: []
  };
  state.chatSummaries = [];
  state.unreadByChatId = {};

  elements.authOverlay.classList.remove("hidden");
  updateSelfStrip();
  renderRelationshipState();
  renderChatList();
  renderActiveChat();
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
  socket.emit("resume_session", { sessionId }, (response) => {
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

function editChatMessage(chatId, message) {
  const nextText = window.prompt("Edit message", String(message.text ?? ""));

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

function deleteChatMessage(chatId, message) {
  const confirmed = window.confirm("Delete this message?");

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

function showMessageActions(chatId, message, x, y) {
  showContextMenu(x, y, [
    {
      label: "Edit message",
      onSelect: () => editChatMessage(chatId, message)
    },
    {
      label: "Delete message",
      danger: true,
      onSelect: () => deleteChatMessage(chatId, message)
    }
  ]);
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
}

function setComposerEnabled(enabled) {
  elements.messageInput.disabled = !enabled;
  elements.sendButton.disabled = !enabled;
  elements.attachButton.disabled = !enabled;
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

function openSettingsModal() {
  if (!state.currentUser) {
    return;
  }

  state.settingsAvatarDataUrl = state.currentUser.avatarDataUrl ?? null;
  elements.settingsDisplayNameInput.value = state.currentUser.name ?? "";
  elements.settingsAvatarInput.value = "";
  elements.settingsCurrentPasswordInput.value = "";
  elements.settingsNewPasswordInput.value = "";
  elements.settingsDeletePasswordInput.value = "";
  renderSettingsAvatarPreview();
  elements.settingsModal.classList.remove("hidden");
}

function updateHeaderActions() {
  const directTarget = currentDirectTarget();
  const isDirect = Boolean(directTarget);
  const isTempChat = state.activeChat?.type === "temp";
  const canCreateGroup = Boolean(state.currentUser) && state.relationships.friends.length > 0;

  elements.voiceCallButton.disabled = !isDirect;
  elements.videoCallButton.disabled = !isDirect;
  elements.toggleGroupPanelButton.disabled = !canCreateGroup;
  elements.openTempChatButton.disabled = !isDirect || isTempChat;

  elements.closeTempChatButton.classList.toggle("hidden", !isTempChat);

  if (isTempChat) {
    const alreadyVoted = Array.isArray(state.activeChat.closeVotes)
      ? state.activeChat.closeVotes.includes(state.currentUser.key)
      : false;

    elements.closeTempChatButton.disabled = alreadyVoted;
    elements.closeTempChatButton.textContent = alreadyVoted ? "Waiting" : "Close Temp";
  }
}

function renderProfilePanel() {
  const directTarget = currentDirectTarget();
  const fallback = resolveUserByKey(state.profileUserKey);
  const user = directTarget ?? fallback;

  if (!user) {
    elements.profileEmpty.classList.remove("hidden");
    elements.profileContent.classList.add("hidden");
    return;
  }

  elements.profileEmpty.classList.add("hidden");
  elements.profileContent.classList.remove("hidden");

  setAvatar(elements.profileAvatar, user);
  elements.profileName.textContent = user.name;
  elements.profileState.textContent = user.online ? "Online" : "Offline";

  if (state.activeChat?.type === "temp") {
    elements.profileMeta.textContent = "Temp chat enabled";
  } else if (state.activeChat?.type === "group") {
    elements.profileMeta.textContent = "Group member";
  } else {
    elements.profileMeta.textContent = "Friend";
  }
}

function renderGroupMemberOptions() {
  const selected = new Set(
    [...elements.groupMembersSelect.selectedOptions].map((option) => option.value)
  );

  elements.groupMembersSelect.innerHTML = "";

  for (const friend of state.relationships.friends) {
    const option = document.createElement("option");
    option.value = friend.key;
    option.textContent = friend.name;
    option.selected = selected.has(friend.key);
    elements.groupMembersSelect.append(option);
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
  const withUserKey = typeof target === "object" && target ? target.key : null;
  const withUser = typeof target === "string" ? target : target?.name;

  socket.emit("open_chat", { withUserKey, withUser, isTemp }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not open chat.");
      return;
    }

    state.activeChat = response.chat;
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
    renderGroupMemberOptions();
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
    status.className = "friend-status";
    status.textContent = friend.online ? "Online" : "Offline";

    body.append(name, status);
    item.append(avatar, body);

    elements.friendsList.append(item);
  }

  renderGroupMemberOptions();
}

function renderRelationshipState() {
  renderIncomingRequests();
  renderOutgoingRequests();
  renderFriends();
}

function loadChat(chatId, onLoaded) {
  socket.emit("load_chat", { chatId }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not load chat.");

      if (typeof onLoaded === "function") {
        onLoaded(false);
      }
      return;
    }

    state.activeChat = response.chat;
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
    }

    const right = document.createElement("div");
    right.className = "chat-right";

    const time = document.createElement("span");
    time.className = "chat-time";
    time.textContent = formatSummaryTime(summary.lastMessageAt ?? summary.updatedAt);

    right.append(time);

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

    item.addEventListener("click", () => {
      loadChat(summary.id);
    });

    elements.chatList.append(item);
  }
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
      link.textContent = `Download ${name}`;
      wrapper.append(link);
      continue;
    }

    const fileLink = document.createElement("a");
    fileLink.className = "attachment-link";
    fileLink.href = dataUrl;
    fileLink.download = name;
    fileLink.textContent = name;
    wrapper.append(fileLink);
  }

  return wrapper;
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

  const text = document.createElement("p");
  text.className = "system-text";
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

  if (!chat || chat.messages.length === 0) {
    const placeholder = document.createElement("div");
    placeholder.className = "placeholder";
    placeholder.textContent = chat
      ? "No messages yet."
      : "Select a friend or existing chat to start messaging.";
    elements.messages.append(placeholder);
    return;
  }

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

  for (const message of chat.messages) {
    if (message.senderKey === "system" || message.kind) {
      elements.messages.append(renderSystemMessage(chat, message, latestCloseRequestId));
      continue;
    }

    const card = document.createElement("article");
    card.className = `message ${
      message.senderKey === state.currentUser.key ? "self" : "other"
    }`;

    const meta = document.createElement("div");
    meta.className = "message-meta";
    const sentTime = formatMessageTime(message.sentAt);
    const editedSuffix = message.deleted ? "" : message.editedAt ? " (edited)" : "";
    meta.textContent = `${message.senderName} • ${sentTime}${editedSuffix}`;

    card.append(meta);

    if (message.deleted) {
      const deleted = document.createElement("p");
      deleted.className = "message-deleted";
      deleted.textContent = "Message deleted";
      card.append(deleted);
    } else if (message.text) {
      const text = document.createElement("div");
      text.className = "message-text";
      text.textContent = message.text;
      card.append(text);
    }

    if (!message.deleted) {
      const attachments = renderMessageAttachments(message);
      if (attachments) {
        card.append(attachments);
      }
    }

    if (message.senderKey === state.currentUser.key && !message.deleted) {
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

  elements.messages.scrollTop = elements.messages.scrollHeight;
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

    if (String(attachment.type ?? "").startsWith("image/")) {
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
    remove.addEventListener("click", () => {
      state.pendingAttachments = state.pendingAttachments.filter(
        (entry) => entry.id !== attachment.id
      );
      renderAttachmentPreview();
    });

    item.append(name, remove);
    elements.attachmentPreview.append(item);
  }
}

function clearPendingAttachments() {
  state.pendingAttachments = [];
  elements.fileInput.value = "";
  renderAttachmentPreview();
}

function renderActiveChat() {
  const chat = state.activeChat;

  if (!chat) {
    elements.chatTitle.textContent = "Select a chat";
    elements.chatSubtitle.textContent = "Tap a friend to start chatting.";
    setComposerEnabled(false);
    clearPendingAttachments();
    renderMessages(null);
    updateHeaderActions();
    renderProfilePanel();
    return;
  }

  if (chat.type === "group") {
    elements.chatTitle.textContent = chat.name || "Group chat";
    elements.chatSubtitle.textContent = `Group chat with ${chat.participants.length} members`;
  } else {
    const target = currentDirectTarget();
    if (chat.type === "temp") {
      elements.chatTitle.textContent = target ? `${target.name} (Temp)` : "Temp chat";
    } else {
      elements.chatTitle.textContent = target ? target.name : "Direct chat";
    }
    elements.chatSubtitle.textContent =
      chat.type === "temp"
        ? "Temp chat: both users must agree to close."
        : "Direct messages";

    if (target) {
      setProfileUser(target.key);
    }
  }

  setComposerEnabled(true);
  renderMessages(chat);
  updateHeaderActions();
  renderProfilePanel();
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();

    reader.onload = () => resolve(String(reader.result ?? ""));
    reader.onerror = () => reject(new Error("Could not read file."));
    reader.readAsDataURL(file);
  });
}

async function addSelectedFiles(fileList) {
  const files = [...fileList];

  if (files.length === 0) {
    return;
  }

  if (state.pendingAttachments.length + files.length > 4) {
    showToast("You can attach up to 4 files at once.");
    return;
  }

  for (const file of files) {
    if (file.size > 5 * 1024 * 1024) {
      showToast(`${file.name} is larger than 5 MB.`);
      continue;
    }

    try {
      const dataUrl = await readFileAsDataUrl(file);

      state.pendingAttachments.push({
        id: crypto.randomUUID(),
        name: file.name || "file",
        type: file.type || "application/octet-stream",
        size: file.size,
        dataUrl
      });
    } catch {
      showToast(`Could not attach ${file.name}.`);
    }
  }

  renderAttachmentPreview();
}

function startCallUi(mode, remoteName, statusText) {
  elements.callOverlay.classList.remove("hidden");
  elements.callOverlay.classList.toggle("voice-mode", mode === "voice");
  elements.callTitle.textContent = `${mode === "video" ? "Video" : "Voice"} call with ${remoteName}`;
  elements.callStatus.textContent = statusText;
  syncCallFocusState();
  updateCallControlButtons();
}

function setCallStatus(text) {
  elements.callStatus.textContent = text;
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

function updateCallControlButtons() {
  const hasCall = Boolean(state.call);
  const mode = state.call?.mode ?? "voice";
  const micEnabled = hasCall ? state.call?.micEnabled !== false : false;
  const cameraEnabled = hasCall ? state.call?.cameraEnabled !== false : false;

  elements.toggleMicButton.disabled = !hasCall;
  elements.toggleMicButton.textContent = micEnabled ? "Mic On" : "Mic Off";

  const isVideoCall = hasCall && mode === "video";
  elements.toggleCameraButton.classList.toggle("hidden", !isVideoCall);
  elements.toggleCameraButton.disabled = !isVideoCall;
  elements.toggleCameraButton.textContent = cameraEnabled ? "Cam On" : "Cam Off";
}

function setCallConnected(connected) {
  if (!state.call) {
    return;
  }

  state.call.connected = connected;
}

function startOutgoingRingCountdown() {
  clearOutgoingRingTimer();

  if (!state.call) {
    return;
  }

  state.call.ringCount = 1;
  setCallStatus(`Ringing (${state.call.ringCount}/${MAX_RING_COUNT})`);

  outgoingRingTimer = window.setInterval(() => {
    if (!state.call || state.call.connected) {
      clearOutgoingRingTimer();
      return;
    }

    state.call.ringCount += 1;

    if (state.call.ringCount > MAX_RING_COUNT) {
      const { chatId } = state.call;
      clearOutgoingRingTimer();
      socket.emit("end_call", { chatId }, () => {});
      showToast("No answer. Call ended.");
      cleanupCall(false);
      return;
    }

    setCallStatus(`Ringing (${state.call.ringCount}/${MAX_RING_COUNT})`);
  }, RING_INTERVAL_MS);
}

function updateIncomingRingLabel() {
  if (!state.pendingIncomingCall) {
    return;
  }

  const ringCount = Number(state.pendingIncomingCall.ringCount ?? 1);
  elements.incomingCallRings.textContent = `Ring ${ringCount} of ${MAX_RING_COUNT}`;
}

function startIncomingRingCountdown() {
  clearIncomingRingTimer();

  incomingRingTimer = window.setInterval(() => {
    if (!state.pendingIncomingCall) {
      clearIncomingRingTimer();
      return;
    }

    state.pendingIncomingCall.ringCount = Number(state.pendingIncomingCall.ringCount ?? 1) + 1;

    if (state.pendingIncomingCall.ringCount > MAX_RING_COUNT) {
      const fromName = state.pendingIncomingCall.fromName;
      declineIncomingCall("missed");
      showToast(`Missed call from ${fromName}.`);
      return;
    }

    updateIncomingRingLabel();
  }, RING_INTERVAL_MS);
}

function toggleMic() {
  if (!state.call?.localStream) {
    return;
  }

  state.call.micEnabled = !state.call.micEnabled;
  setTrackEnabled(state.call.localStream, "audio", state.call.micEnabled);
  updateCallControlButtons();
}

function toggleCamera() {
  if (!state.call?.localStream || state.call.mode !== "video") {
    return;
  }

  state.call.cameraEnabled = !state.call.cameraEnabled;
  setTrackEnabled(state.call.localStream, "video", state.call.cameraEnabled);
  updateCallControlButtons();
}

function cleanupCall(shouldEmitEndSignal = false) {
  clearOutgoingRingTimer();
  clearIncomingRingTimer();

  if (!state.call) {
    elements.callOverlay.classList.add("hidden");
    elements.incomingCallPrompt.classList.add("hidden");
    syncCallFocusState();
    updateCallControlButtons();
    return;
  }

  const { chatId, peer, localStream, remoteStream } = state.call;

  if (shouldEmitEndSignal) {
    socket.emit("end_call", { chatId }, () => {});
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

  elements.localVideo.srcObject = null;
  elements.remoteVideo.srcObject = null;

  state.call = null;
  state.pendingIncomingCall = null;

  elements.callOverlay.classList.add("hidden");
  elements.incomingCallPrompt.classList.add("hidden");
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
    const [stream] = event.streams;

    if (stream) {
      elements.remoteVideo.srcObject = stream;

      if (state.call) {
        state.call.remoteStream = stream;
      }
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

    if (connectionState === "failed" || connectionState === "disconnected") {
      showToast("Call ended.");
      cleanupCall(false);
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
    micEnabled: true,
    cameraEnabled: mode === "video",
    connected: false,
    ringCount: 0
  };

  elements.localVideo.srcObject = localStream;
  startCallUi(mode, target.name, "Calling...");
  startOutgoingRingCountdown();

  try {
    const offer = await peer.createOffer();
    await peer.setLocalDescription(offer);

    socket.emit("call_offer", { chatId, mode, offer }, (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Could not start call.");
        cleanupCall(false);
      }
    });
  } catch {
    showToast("Could not start call.");
    cleanupCall(false);
  }
}

function showIncomingCallPrompt(payload) {
  state.pendingIncomingCall = {
    ...payload,
    ringCount: 1
  };
  elements.incomingCallText.textContent = `${payload.fromName} is calling (${payload.mode}).`;
  updateIncomingRingLabel();
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
    micEnabled: true,
    cameraEnabled: incoming.mode === "video",
    connected: false,
    ringCount: 0
  };

  elements.localVideo.srcObject = localStream;
  startCallUi(incoming.mode, incoming.fromName, "Connecting...");

  try {
    await peer.setRemoteDescription(new RTCSessionDescription(incoming.offer));
    const answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);

    socket.emit(
      "call_answer",
      {
        chatId: incoming.chatId,
        answer
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

  socket.emit(eventName, { username, password }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Authentication failed.");
      return;
    }

    completeAuth(response);
  });
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
  elements.groupPanel.classList.toggle("hidden");

  if (!elements.groupPanel.classList.contains("hidden") && isMobileViewport()) {
    setSidebarOpen(true);
  }
});

elements.groupForm.addEventListener("submit", (event) => {
  event.preventDefault();

  const name = elements.groupNameInput.value.trim();
  const members = [...elements.groupMembersSelect.selectedOptions].map((option) => option.value);

  socket.emit("create_group", { name, members }, (response) => {
    if (!response?.ok) {
      showToast(response?.error ?? "Could not create group.");
      return;
    }

    elements.groupNameInput.value = "";
    for (const option of elements.groupMembersSelect.options) {
      option.selected = false;
    }

    elements.groupPanel.classList.add("hidden");

    state.activeChat = response.chat;
    state.unreadByChatId[response.chat.id] = 0;

    renderActiveChat();
    renderChatList();
  });
});

elements.composerForm.addEventListener("submit", (event) => {
  event.preventDefault();

  if (!state.activeChat) {
    return;
  }

  const text = elements.messageInput.value.trim();
  const outgoingAttachments = state.pendingAttachments.map((attachment) => ({
    name: attachment.name,
    type: attachment.type,
    size: attachment.size,
    dataUrl: attachment.dataUrl
  }));

  if (!text && outgoingAttachments.length === 0) {
    return;
  }

  socket.emit(
    "send_message",
    {
      chatId: state.activeChat.id,
      text,
      attachments: outgoingAttachments
    },
    (response) => {
      if (!response?.ok) {
        showToast(response?.error ?? "Failed to send message.");
        return;
      }

      elements.messageInput.value = "";
      clearPendingAttachments();
    }
  );
});

elements.attachButton.addEventListener("click", () => {
  elements.fileInput.click();
});

elements.fileInput.addEventListener("change", async () => {
  await addSelectedFiles(elements.fileInput.files);
  elements.fileInput.value = "";
});

elements.voiceCallButton.addEventListener("click", () => {
  startOutgoingCall("voice");
});

elements.videoCallButton.addEventListener("click", () => {
  startOutgoingCall("video");
});

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

elements.settingsButton.addEventListener("click", () => {
  openSettingsModal();
});

elements.closeSettingsButton.addEventListener("click", () => {
  elements.settingsModal.classList.add("hidden");
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
    showToast("Enter a display name.");
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
    return;
  }

  const permission = await Notification.requestPermission();
  showToast(permission === "granted" ? "Notifications enabled." : "Notifications blocked.");
});

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

elements.endCallButton.addEventListener("click", () => {
  cleanupCall(true);
});

window.addEventListener("resize", () => {
  if (!isMobileViewport()) {
    setSidebarOpen(false);
  }

  hideContextMenu();
});

window.addEventListener("blur", () => {
  hideContextMenu();
});

window.addEventListener("scroll", hideContextMenu, true);

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    hideContextMenu();
  }
});

document.addEventListener("pointerdown", (event) => {
  if (contextMenuElement.classList.contains("hidden")) {
    return;
  }

  if (contextMenuElement.contains(event.target)) {
    return;
  }

  hideContextMenu();
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

socket.on("chat_summaries", (payload) => {
  state.chatSummaries = Array.isArray(payload) ? payload : [];

  const activeStillExists = state.activeChat
    ? state.chatSummaries.some((summary) => summary.id === state.activeChat.id)
    : true;

  if (!activeStillExists) {
    state.activeChat = null;
    renderActiveChat();
  }

  const knownChatIds = new Set(state.chatSummaries.map((summary) => summary.id));
  for (const chatId of Object.keys(state.unreadByChatId)) {
    if (!knownChatIds.has(chatId)) {
      delete state.unreadByChatId[chatId];
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
  }
});

socket.on("chat_removed", (payload) => {
  const chatId = String(payload?.chatId ?? "");

  if (!chatId) {
    return;
  }

  delete state.unreadByChatId[chatId];

  if (state.activeChat?.id === chatId) {
    state.activeChat = null;
    renderActiveChat();
  }

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
  }
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
    setCallConnected(true);
    clearOutgoingRingTimer();
    setCallStatus("Connected");
  } catch {
    showToast("Call connection failed.");
    cleanupCall(false);
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
    const reason = String(payload.reason ?? "declined");
    const message =
      reason === "missed"
        ? "Call missed."
        : reason === "busy"
          ? "User is busy on another call."
          : "Call was declined.";
    showToast(message);
    cleanupCall(false);
  }
});

socket.on("call_ended", (payload) => {
  if (!payload) {
    return;
  }

  if (state.call && state.call.chatId === payload.chatId) {
    showToast("Call ended.");
    cleanupCall(false);
    return;
  }

  if (state.pendingIncomingCall && state.pendingIncomingCall.chatId === payload.chatId) {
    clearIncomingRingTimer();
    state.pendingIncomingCall = null;
    elements.incomingCallPrompt.classList.add("hidden");
    syncCallFocusState();
    showToast("Call ended.");
  }
});

socket.on("disconnect", () => {
  state.isResumingSession = false;
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
