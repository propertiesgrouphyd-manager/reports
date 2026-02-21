import asyncio
import aiohttp
from datetime import datetime, timedelta
import traceback
import random
import pytz
from telegram_config import BOT_TOKEN, get_chat_id, PROPERTIES
IST = pytz.timezone("Asia/Kolkata")

now = datetime.now(IST)

# ==========================================================
# MULTI PROPERTY ROOM + PRICE DETAILS (RANGE) TELEGRAM BOT
# ==========================================================

# ------------------- GLOBAL SETTINGS -------------------
PROP_PARALLEL_LIMIT = 4
API_TIMEOUT = 25
prop_semaphore = asyncio.Semaphore(PROP_PARALLEL_LIMIT)

# ------------------- PROPERTIES -------------------
from telegram_config import BOT_TOKEN, get_chat_id

TELEGRAM_BOT_TOKEN = BOT_TOKEN
TELEGRAM_CHAT_ID = get_chat_id("rpp")


TELEGRAM_SEND_LOCK = asyncio.Lock()

# ------------------- UTILS -------------------
def fmt_date(d): return d.strftime("%Y-%m-%d")
def fmt_human(d): return d.strftime("%d/%m/%Y")

def safe_int(x):
    try:
        return int(x)
    except:
        return 999999

def split_message(msg: str, limit=3900):
    msg = str(msg or "")
    if len(msg) <= limit:
        return [msg]
    parts = []
    while len(msg) > limit:
        cut = msg.rfind("\n", 0, limit)
        if cut == -1 or cut < 500:
            cut = limit
        parts.append(msg[:cut].strip())
        msg = msg[cut:].strip()
    if msg:
        parts.append(msg)
    return parts

def wrap_rooms(prefix, rooms, per_line=12):
    if not rooms:
        return f"{prefix} -"
    chunks = []
    for i in range(0, len(rooms), per_line):
        chunks.append(", ".join(rooms[i:i+per_line]))
    lines = []
    for idx, c in enumerate(chunks):
        if idx == 0:
            lines.append(f"{prefix} {c}")
        else:
            lines.append(f"{' ' * len(prefix)} {c}")
    return "\n".join(lines)

# ------------------- TELEGRAM SENDER -------------------
async def send_telegram_message(text, retries=15, session=None):
    def extract_property_code(msg: str):
        msg = str(msg or "")
        marker = "Room and Price Details :"
        idx = msg.find(marker)
        if idx != -1:
            rest = msg[idx + len(marker):].strip()
            prop = rest.split()[0].strip()
            return prop
        marker2 = "CONSOLIDATED :"
        idx2 = msg.find(marker2)
        if idx2 != -1:
            rest = msg[idx2 + len(marker2):].strip()
            prop = rest.split()[0].strip()
            return prop
        return None

    bot_token = TELEGRAM_BOT_TOKEN
    chat_id = TELEGRAM_CHAT_ID

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"


    async def _post(sess, msg_part):
        payload = {"chat_id": chat_id, "text": msg_part, "parse_mode": "HTML"}
        async with sess.post(url, json=payload, timeout=API_TIMEOUT) as resp:
            if resp.status == 429:
                retry_after = 5
                try:
                    data = await resp.json()
                    retry_after = int(data.get("parameters", {}).get("retry_after", 5))
                except Exception:
                    retry_after = 5
                print(f"⚠️ TELEGRAM 429 → sleeping {retry_after}s (chat_id={chat_id})")
                await asyncio.sleep(retry_after + 1)
                return False

            if resp.status != 200:
                err = ""
                try: err = await resp.text()
                except: pass
                raise RuntimeError(f"Telegram HTTP {resp.status} {err}")

            data = await resp.json()
            if data.get("ok") is True:
                return True
            raise RuntimeError(f"Telegram ok:false → {data.get('description','Unknown error')}")

    parts = split_message(text)

    async with TELEGRAM_SEND_LOCK:
        if session is None:
            async with aiohttp.ClientSession() as s:
                for part in parts:
                    last_err = None
                    for attempt in range(1, retries + 1):
                        try:
                            ok = await _post(s, part)
                            if ok:
                                await asyncio.sleep(0.3)
                                break
                        except Exception as e:
                            last_err = e
                            wait = min(60, 2 * attempt)
                            print(f"⚠️ Telegram retry {attempt}/{retries} → {wait}s :: {e}")
                            await asyncio.sleep(wait)
                    else:
                        raise RuntimeError(f"Telegram send failed after retries: {last_err}")
            return

        for part in parts:
            last_err = None
            for attempt in range(1, retries + 1):
                try:
                    ok = await _post(session, part)
                    if ok:
                        await asyncio.sleep(0.3)
                        break
                except Exception as e:
                    last_err = e
                    wait = min(60, 2 * attempt)
                    print(f"⚠️ Telegram retry {attempt}/{retries} → {wait}s :: {e}")
                    await asyncio.sleep(wait)
            else:
                raise RuntimeError(f"Telegram send failed after retries: {last_err}")

# ------------------- AUTH HELPERS -------------------
def build_auth(P: dict, qid: int):
    cookies = {"uif": P["UIF"], "uuid": P["UUID"]}
    headers = {
        "accept": "application/json",
        "x-qid": str(qid),
        "x-source-client": "merchant",
        "user-agent": "Mozilla/5.0",
    }
    return headers, cookies

# ------------------- OYO APIs -------------------
async def fetch_rooms_for_day(session, qid: int, day, P: dict, retries: int = 5):
    """
    ✅ Auth added
    ✅ Retry added for 5xx (property 15 fix)
    """
    url = f"https://www.oyoos.com/crs_api/hotels/{qid}/rooms/blocked_rooms"
    start_date = day
    end_date = day + timedelta(days=1)

    params = {
        "start_date": fmt_date(start_date),
        "end_date": fmt_date(end_date),
        "block_start_date": fmt_date(start_date),
        "block_end_date": fmt_date(start_date),
        "blocked_rooms": "true",
        "pinned_rooms": "false",
        "pending_approvals": "false",
        "qid": str(qid),
        "locale": "en",
    }

    headers, cookies = build_auth(P, qid)

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, params=params, headers=headers, cookies=cookies, timeout=API_TIMEOUT) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("rooms", [])

                txt = ""
                try:
                    txt = await r.text()
                except:
                    txt = ""

                # retry only for server errors
                if r.status in (500, 502, 503, 504):
                    last_err = RuntimeError(f"blocked_rooms API {r.status}: {txt[:250]}")
                    wait = min(20, attempt * 3) + random.random()
                    print(f"⚠️ blocked_rooms {qid} {fmt_date(day)} → {r.status} retry {attempt}/{retries} after {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue

                raise RuntimeError(f"blocked_rooms API {r.status}: {txt[:250]}")
        except Exception as e:
            last_err = e
            wait = min(20, attempt * 3) + random.random()
            print(f"⚠️ blocked_rooms exception retry {attempt}/{retries} after {wait:.1f}s :: {e}")
            await asyncio.sleep(wait)

    raise RuntimeError(f"blocked_rooms API FAILED after retries: {last_err}")

def extract_room_number(room_obj: dict):
    return str(room_obj.get("number", "") or "").strip()

def extract_floor(room_obj: dict):
    try:
        f = room_obj.get("floor", None)
        if f is None:
            return None
        return int(f)
    except:
        return None

def is_booked(room_obj: dict):
    return room_obj.get("booking_id") is not None

async def fetch_property_pricing(session, P, date_str):
    url = f"https://www.oyoos.com/hms_ms/api/v2/smart_owner_pricing/{P['QID']}/"
    params = {
        "qid": P["QID"],
        "start_date": date_str,
        "end_date": date_str,
        "smart_price_enable": "false",
    }

    headers, cookies = build_auth(P, int(P["QID"]))

    async with session.get(url, params=params, headers=headers, cookies=cookies, timeout=API_TIMEOUT) as r:
        if r.status != 200:
            txt = ""
            try: txt = await r.text()
            except: pass
            raise RuntimeError(f"PRICING API FAILED {r.status} :: {txt[:200]}")
        return await r.json()

async def fetch_property_details(session, P, qid: int):
    """
    ✅ FIXED: Added authentication
    This is why property details were missing earlier.
    """
    url = "https://www.oyoos.com/hms_ms/api/v1/location/property-details"
    params = {"qid": str(qid)}

    headers, cookies = build_auth(P, qid)

    async with session.get(url, params=params, headers=headers, cookies=cookies, timeout=API_TIMEOUT) as r:
        if r.status != 200:
            t = ""
            try: t = await r.text()
            except: pass
            return None
        try:
            return await r.json()
        except:
            return None

# ------------------- CORE COMPUTATION -------------------
async def compute_property_range_availability(prop_key: int, checkin, checkout):
    P = PROPERTIES[prop_key]
    prop_code = P["name"]
    qid = int(P["QID"])

    all_rooms = set()
    booked_rooms = set()

    floor_all = {}
    floor_booked = {}

    async with aiohttp.ClientSession() as session:
        curr = checkin
        last_night = checkout - timedelta(days=1)

        while curr <= last_night:
            rooms = await fetch_rooms_for_day(session, qid, curr, P)

            for room in rooms:
                num = extract_room_number(room)
                if not num:
                    continue

                fl = extract_floor(room)

                all_rooms.add(num)
                if fl is not None:
                    floor_all.setdefault(fl, set()).add(num)

                if is_booked(room):
                    booked_rooms.add(num)
                    if fl is not None:
                        floor_booked.setdefault(fl, set()).add(num)

            curr += timedelta(days=1)

    total = len(all_rooms)
    booked = len(booked_rooms)
    available = total - booked

    floors = sorted(set(list(floor_all.keys()) + list(floor_booked.keys())))
    total_floors = len(floors)

    floor_summary = []
    for fl in floors:
        all_on_floor = floor_all.get(fl, set())
        booked_on_floor = floor_booked.get(fl, set())
        available_on_floor = sorted(list(all_on_floor - booked_on_floor), key=safe_int)
        booked_on_floor_sorted = sorted(list(booked_on_floor), key=safe_int)

        floor_summary.append({
            "floor": fl,
            "avl": available_on_floor,
            "bkd": booked_on_floor_sorted
        })

    return {
        "prop": prop_code,
        "qid": qid,
        "total": total,
        "booked": booked,
        "available": available,
        "total_floors": total_floors,
        "floor_summary": floor_summary
    }

def build_price_section(pricing_json, stay_nights, from_date_str):
    p1 = p2 = p3 = "-"
    categories = pricing_json.get("room_categories_info", []) or []
    selected = None
    for cat in categories:
        if str(cat.get("room_category_name", "")).strip().lower() == "classic":
            selected = cat
            break
    if selected is None and categories:
        selected = categories[0]

    if selected:
        for d in (selected.get("datewise_details") or []):
            if str(d.get("date", "")).strip() == from_date_str:
                cp = d.get("current_prices", {}) or {}
                p1 = cp.get("1", "-")
                p2 = cp.get("2", "-")
                p3 = cp.get("3", "-")
                break

    def mult(val):
        try:
            if val in (None, "", "-"):
                return "-"
            return int(float(val) * stay_nights)
        except:
            return val

    return mult(p1), mult(p2), mult(p3)

def build_property_details_section(details_json):
    """
    ✅ PERFECT LEFT ALIGNED Property Details (<pre> Telegram friendly)

    Format:
    Name:
      value

    Alternate Name:
      value

    Address:
      value

    Google Map:
      link
    """
    if not details_json or not isinstance(details_json, dict):
        return (
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Property Details\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🏨 Name:\n"
            "  -\n\n"
            "🏷 Alternate Name:\n"
            "  -\n\n"
            "📍 Address:\n"
            "  -\n\n"
            "🗺 Google Map:\n"
            "  -"
        )

    name = str(details_json.get("name", "-") or "-").strip()
    alt = str(details_json.get("alternate_name", "-") or "-").strip()
    plot = str(details_json.get("plot_number", "") or "").strip()
    street = str(details_json.get("street", "") or "").strip()
    city = str(details_json.get("city", "") or "").strip()
    pin = str(details_json.get("pincode", "") or "").strip()
    map_link = str(details_json.get("map_link", "-") or "-").strip()

    # ✅ Build address single string
    addr_parts = []
    if plot:
        addr_parts.append(plot)
    if street:
        addr_parts.append(street)
    if city or pin:
        if city and pin:
            addr_parts.append(f"{city} - {pin}")
        elif city:
            addr_parts.append(city)
        elif pin:
            addr_parts.append(pin)

    address = ", ".join([x for x in addr_parts if x]) if addr_parts else "-"

    # ✅ Wrap helper (left aligned, PRE friendly)
    def wrap_text(text: str, width: int = 64):
        text = str(text or "").strip()
        if not text:
            return ["-"]

        # link wrap (cut)
        if text.startswith("http://") or text.startswith("https://"):
            return [text[i:i+width] for i in range(0, len(text), width)]

        # normal wrap (word based)
        words = text.split()
        lines = []
        cur = ""
        for w in words:
            if len(cur) + len(w) + 1 <= width:
                cur = (cur + " " + w).strip()
            else:
                if cur:
                    lines.append(cur)
                cur = w
        if cur:
            lines.append(cur)
        return lines if lines else ["-"]

    # ✅ Block builder: label in one line, value next line(s), blank line after
    def block(label: str, value: str):
        lines = [label]
        for ln in wrap_text(value, 64):
            lines.append(f"  {ln}")
        lines.append("")  # ✅ one line space after each field
        return "\n".join(lines)

    out = []
    out.append("━━━━━━━━━━━━━━━━━━━━━━")
    out.append("Property Details")
    out.append("━━━━━━━━━━━━━━━━━━━━━━")

    out.append(block("🏨 Name:", name))
    out.append(block("🏷 Alternate Name:", alt))
    out.append(block("📍 Address:", address))

    # ✅ Google Map (no extra blank line at end)
    out.append("🗺 Google Map:")
    for ln in wrap_text(map_link, 64):
        out.append(f"  {ln}")

    return "\n".join(out).strip()



def build_property_message(result, checkin, checkout, stay_nights, p1, p2, p3, prop_details_text):
    prop = result["prop"]

    floor_lines = []
    floor_lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    floor_lines.append("Floor-wise Availability")
    floor_lines.append("━━━━━━━━━━━━━━━━━━━━━━\n")

    for fl_data in result["floor_summary"]:
        fl = fl_data["floor"]
        avl = fl_data["avl"]
        bkd = fl_data["bkd"]

        floor_lines.append(f"Floor {fl}  | 🟢 Avl: {len(avl):02d}  🔴 Bkd: {len(bkd):02d}")
        floor_lines.append(wrap_rooms("🟢", avl, per_line=12))
        floor_lines.append(wrap_rooms("🔴", bkd, per_line=12))
        floor_lines.append("")

    floor_text = "\n".join(floor_lines).strip()

    return f"""
<pre>
Room and Price Details : {prop}

✅ Stay Dates         : {fmt_human(checkin)} to {fmt_human(checkout)}

🌙 Stay Nights        : {stay_nights}

🏨 Total Rooms        : {result["total"]}
🟢 Available Rooms    : {result["available"]}
🔴 Booked Rooms       : {result["booked"]}

🏢 Floors             : {result["total_floors"]}

{floor_text}

━━━━━━━━━━━━━━━━━━━━━━
Pricing (Range Total)
━━━━━━━━━━━━━━━━━━━━━━
👤 1 Guest            : ₹{p1}
👥 2 Guests           : ₹{p2}
👥👤 3 Guests          : ₹{p3}

{prop_details_text}
</pre>
""".strip()

def build_consolidated_message(all_results, checkin, checkout):
    total_all = sum(r["total"] for r in all_results)
    booked_all = sum(r["booked"] for r in all_results)
    available_all = sum(r["available"] for r in all_results)

    lines = []
    lines.append("<pre>")
    lines.append("CONSOLIDATED : ALL")
    lines.append("")
    lines.append(f"🗓 Range : {fmt_human(checkin)} → {fmt_human(checkout)} (Checkout)")
    lines.append("")
    lines.append(f"🏨 Total Rooms        : {total_all}")
    lines.append(f"🟢 Available Rooms     : {available_all}")
    lines.append(f"🔴 Booked Rooms        : {booked_all}")
    lines.append("")
    lines.append("📌 Property-wise Summary:")
    lines.append("")

    all_results_sorted = sorted(all_results, key=lambda x: x["booked"], reverse=True)
    for r in all_results_sorted:
        prop = r["prop"]
        t = r["total"]
        b = r["booked"]
        a = r["available"]
        occ = round((b / t) * 100) if t else 0
        lines.append(f"- {prop} | Total:{t} | Avl:{a} | Bkd:{b} | Occ:{occ}%")

    lines.append("</pre>")
    return "\n".join(lines).strip()

# ------------------- WORKER -------------------
async def process_property(prop_key: int, checkin, checkout):
    async with prop_semaphore:
        P = PROPERTIES[prop_key]
        qid = int(P["QID"])

        # availability
        result = await compute_property_range_availability(prop_key, checkin, checkout)

        # pricing + details
        async with aiohttp.ClientSession() as session:
            pricing_json = await fetch_property_pricing(session, P, fmt_date(checkin))
            details_json = await fetch_property_details(session, P, qid)

        return result, pricing_json, details_json

# ------------------- MAIN -------------------
async def main():
    print("========================================")
    print(" OYO RANGE AVAILABILITY TELEGRAM BOT")
    print("========================================")

    # AUTO SELECT TODAY DATE (ONE DAY ONLY)
    now = datetime.now(IST)
    today = now.date()
    checkin = today
    checkout = today + timedelta(days=1)


    stay_nights = (checkout - checkin).days
    if stay_nights <= 0:
        print("❌ Invalid stay nights")
        return

    selected = list(PROPERTIES.keys())

    print("==============================================")
    print(" MULTI PROPERTY ROOM + PRICE DETAILS")
    print("==============================================")
    print(f"Range: {checkin} -> {checkout} (checkout)")
    print(f"Stay Nights: {stay_nights}")
    print(f"Properties: {len(selected)}")
    print("==============================================")

    tasks = [process_property(p, checkin, checkout) for p in selected]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    ok_results = []
    failures = []

    for p, res in zip(selected, results):
        if isinstance(res, Exception):
            failures.append((p, res))
            continue
        ok_results.append((p, res))

    if failures:
        print("\n❌ FAILURES:")
        for p, e in failures:
            prop_name = PROPERTIES[p]["name"]
            print(f"- {p} ({prop_name}): {e}")

    prop_messages = []
    for prop_key, (availability_result, pricing_json, details_json) in ok_results:
        p1, p2, p3 = build_price_section(pricing_json, stay_nights, fmt_date(checkin))
        details_text = build_property_details_section(details_json)

        msg = build_property_message(
            availability_result,
            checkin,
            checkout,
            stay_nights,
            p1, p2, p3,
            details_text
        )
        prop_messages.append(msg)

    consolidated_msg = build_consolidated_message(
        [x[1][0] for x in ok_results],
        checkin,
        checkout
    )

    async with aiohttp.ClientSession() as tg_session:
        for m in prop_messages:
            await send_telegram_message(m, session=tg_session)
            await asyncio.sleep(1.2)

        await send_telegram_message(consolidated_msg, session=tg_session)

    print("✅ TELEGRAM SENT DONE.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("SCRIPT CRASHED")
        print(e)
        traceback.print_exc()
