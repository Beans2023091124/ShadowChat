# Shadow Chat

Discord-inspired real-time messaging app with a dark grey/black theme and `#a8a8a8` as the primary accent.

## Features

- Username-based accounts (in-memory)
- Friend request workflow (send, accept, decline)
- Friends list with online/offline presence
- Friends-only DM chats
- Friends-only temp chats
- Group chats with selected friends
- Temp chat deletion only after both participants agree to close

## Run Locally

1. Install dependencies:
   ```bash
   npm install
   ```
2. Start the server:
   ```bash
   npm run dev
   ```
3. Open `http://localhost:3000` in multiple tabs/windows using different usernames.

## Temp Chat Behavior

- Temp chats are direct friend chats.
- Either participant can press **Agree To Close**.
- The temp chat is deleted only after both participants agree.
- Sending a new message clears prior close votes.

## Notes

- Data is currently stored in memory and resets on server restart.
- Next step for production-style behavior is adding persistent storage and authentication.
"# Shadow-Chat" 
