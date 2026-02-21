import os
import json

# ================= TELEGRAM =================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

CHAT_MAP = json.loads(os.getenv("TELEGRAM_CHAT_MAP", "{}"))

def get_chat_id(name: str):
    if name not in CHAT_MAP:
        raise RuntimeError(f"Chat ID not configured: {name}")
    return CHAT_MAP[name]


# ================= PROPERTIES =================

PROPERTIES_RAW = json.loads(os.getenv("OYO_PROPERTIES", "{}"))

PROPERTIES = {int(k): v for k, v in PROPERTIES_RAW.items()}

if not PROPERTIES:
    raise RuntimeError("OYO_PROPERTIES secret missing or empty")
