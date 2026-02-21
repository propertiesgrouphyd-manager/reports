import os
import json

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

CHAT_MAP = json.loads(os.getenv("TELEGRAM_CHAT_MAP", "{}"))

def get_chat_id(name: str):
    if name not in CHAT_MAP:
        raise RuntimeError(f"Chat ID not configured: {name}")
    return CHAT_MAP[name]
