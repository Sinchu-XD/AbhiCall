import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ASSISTANT_SESSION = os.getenv("ASSISTANT_SESSION", "")
COOKIES_PATH = os.getenv("COOKIES_PATH", "")

STUN_SERVER    = os.getenv("STUN_SERVER",    "stun:stun.l.google.com:19302")
TURN_SERVER    = os.getenv("TURN_SERVER",    "")
TURN_USERNAME  = os.getenv("TURN_USERNAME",  "")
TURN_PASSWORD  = os.getenv("TURN_PASSWORD",  "")
