# 🎵 Telegram VC Music Bot
### Pyrogram + Custom WebRTC (aiortc) — No PyTgCalls

## Architecture

```
User: /play <song>
        ↓
  Bot (Pyrogram, bot_token)     ← Commands handle karta hai
        ↓
  yt-dlp                        ← Audio stream URL nikalta hai
        ↓
  FFmpeg → PCM → Opus           ← AudioPipeline encode karta hai
        ↓
  Assistant (Pyrogram, account) ← phone.joinGroupCall MTProto call
        ↓
  aiortc (WebRTC)               ← ICE/DTLS/SRTP — Telegram media server
        ↓
  🔊 Members sunते hain!
```

## Bot vs Assistant — Zaroori Kyon Hai?

| | Bot | Assistant |
|---|---|---|
| Kya hai? | BotFather se bana | Real Telegram account |
| VC join kar sakta? | ❌ Nahi | ✅ Haan |
| Commands handle karta? | ✅ Haan | ❌ Nahi |

**Dono zaroori hain!**

## Setup

### 1. Dependencies install karo
```bash
pip install -r requirements.txt
```

> **Note:** `aiortc` ke liye system packages chahiye:
> ```bash
> # Ubuntu/Debian
> sudo apt install libavdevice-dev libavfilter-dev libopus-dev libvpx-dev pkg-config
> # macOS
> brew install ffmpeg opus libvpx pkg-config
> ```

### 2. Credentials lao

**API ID aur API Hash:**
- https://my.telegram.org → "API Development Tools"
- App banao → `API_ID` aur `API_HASH` milega

**Bot Token:**
- @BotFather → `/newbot` → token milega

### 3. .env banao
```bash
cp .env.example .env
# Values bharo
```

### 4. Pehli baar chalao
```bash
python main.py
```
Assistant ka **phone number** maangega → OTP daalo → `assistant.session` ban jayegi.
Dobara chalane par sirf file use hogi.

### 5. Group setup
1. **Bot** ko group mein add karo
2. **Assistant** account ko group mein add karo
3. Assistant ko **admin** banao (VC join karne ke liye)
4. Group mein **Voice Chat shuru karo**
5. `/join` karo → phir `/play <song>` karo!

## Commands

| Command | Description |
|---------|-------------|
| `/join` | Voice Chat mein aao |
| `/play <url ya naam>` | YouTube se music bajao |
| `/skip` | Agla song |
| `/stop` | Band karo, VC chhodo |
| `/pause` | Pause karo |
| `/resume` | Resume karo |
| `/queue` | Queue dekho |

## Troubleshooting

**`PARTICIPANT_JOIN_MISSING`**
→ Assistant group mein nahi hai — add karo

**`GROUPCALL_FORBIDDEN`**
→ Group mein Voice Chat band hai — shuru karo pehle

**`USER_BANNED_IN_CHANNEL`**
→ Assistant banned hai us group mein

**ICE connection fail**
→ STUN server check karo — default `stun:stun.l.google.com:19302` theek hai
→ Firewall UDP block kar sakta hai

**`assistant.session` baar baar delete**
→ File permissions check karo — same folder mein hona chahiye
