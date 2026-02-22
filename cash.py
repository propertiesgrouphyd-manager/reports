# ==============================
# ULTRA FAST ASYNC MULTI PROPERTY AUTOMATION
# DATE-WISE COLLECTION BASED ON PAYMENT CREATED_AT
# FINAL EXCEL: ONLY PAID BOOKINGS (NO PER DAY STAY CALC)
# ==============================

import os
import json
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import traceback
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, Reference
from io import BytesIO
import pytz
IST = pytz.timezone("Asia/Kolkata")
now = datetime.now(IST)

MAX_FULL_RUN_RETRIES = 5
FULL_RUN_RETRY_DELAY = 10

PROP_PARALLEL_LIMIT = 3
DETAIL_PARALLEL_LIMIT = 10

prop_semaphore = asyncio.Semaphore(PROP_PARALLEL_LIMIT)

DETAIL_TIMEOUT = 25
ROOMS_TIMEOUT = 25
BATCH_TIMEOUT = 35


# ================= TELEGRAM =================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("❌ TELEGRAM_BOT_TOKEN missing")


CHAT_MAP = json.loads(os.getenv("TELEGRAM_CHAT_MAP", "{}"))

def get_chat_id(name: str):
    if name not in CHAT_MAP:
        raise RuntimeError(f"❌ Chat ID not configured: {name}")
    return int(CHAT_MAP[name])


# choose chat key from secret map
TELEGRAM_CHAT_ID = get_chat_id("6am")   # change if needed


# ================= PROPERTIES =================

PROPERTIES_RAW = json.loads(os.getenv("OYO_PROPERTIES", "{}"))

PROPERTIES = {int(k): v for k, v in PROPERTIES_RAW.items()}

if not PROPERTIES:
    raise RuntimeError("❌ OYO_PROPERTIES secret missing or empty")

# ================= TELEGRAM =================
async def send_telegram_excel_buffer(buffer, filename, caption=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"

    data = aiohttp.FormData()
    data.add_field("chat_id", str(TELEGRAM_CHAT_ID))
    if caption:
        data.add_field("caption", caption)

    data.add_field(
        "document",
        buffer,
        filename=filename,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data, timeout=120) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"Telegram send failed: {text}")

# ================= BEAUTIFY EXCEL =================
def beautify(ws):
    blue = PatternFill("solid", fgColor="1F4E78")
    light1 = PatternFill("solid", fgColor="DDEBF7")
    light2 = PatternFill("solid", fgColor="F2F2F2")
    yellow = PatternFill("solid", fgColor="FFF4CC")

    bold_white = Font(color="FFFFFF", bold=True, size=12)
    bold_black = Font(color="000000", bold=True, size=12)

    thin = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    max_row = ws.max_row
    max_col = ws.max_column
    ws.freeze_panes = "A2"

    for col in range(1, max_col + 1):
        c = ws.cell(row=1, column=col)
        c.fill = blue
        c.font = bold_white
        c.alignment = Alignment(horizontal="center", vertical="center")
        c.border = thin

    for r in range(2, max_row + 1):
        fill = light1 if r % 2 == 0 else light2
        for c in range(1, max_col + 1):
            cell = ws.cell(row=r, column=c)
            if cell.value is None:
                continue

            if cell.fill is not None and cell.fill.patternType is not None:
                cell.border = thin
                continue

            cell.fill = fill
            cell.border = thin

    for col in ws.columns:
        max_length = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = max_length + 5

    for r in range(2, max_row + 1):
        text = str(ws.cell(row=r, column=1).value or "")
        if text.strip() == "":
            continue
        if "Booking" in text or "Amount" in text or "Total" in text or "OYO" in text:
            ws.cell(row=r, column=1).fill = yellow
            ws.cell(row=r, column=1).font = bold_black

def get_hour_color(hour):
    """
    Sunlight theme colors across 24 hours
    Night → Sunrise → Day → Sunset → Night
    """
    palette = [
        "0B3D91", "0F52BA", "1C6ED5", "2E86DE",  # 12–4 AM (deep blue → dawn)
        "5DADE2", "85C1E9", "AED6F1", "F9E79F",  # 4–8 AM (sunrise)
        "F7DC6F", "F4D03F", "F1C40F", "F39C12",  # 8–12 PM (bright day)
        "EB984E", "E67E22", "DC7633", "D35400",  # 12–4 PM (warm sun)
        "CD6155", "C0392B", "A93226", "922B21",  # 4–8 PM (sunset)
        "7B241C", "641E16", "512E5F", "2C3E50"   # 8–12 AM (night)
    ]
    return palette[hour % 24]

# ================= BOOKING SOURCE =================
def get_booking_source(b):
    source = str(b.get("source", "") or "").strip()
    ota = str(b.get("ota_source", "") or "").strip()
    sub = str(b.get("sub_source", "") or "").strip()
    corp = bool(b.get("is_corporate", False))
    booking_identifier = str(b.get("booking_identifier", "") or "").strip()

    if booking_identifier == "TA":
        return "TA"
    if source == "Walk In":
        return "Walk-in"
    if corp or sub == "corporate":
        return "CB"
    if "Booking.com" in ota:
        return "BDC"
    if "GoMMT" in ota:
        return "MMT"
    if "Agoda" in ota:
        return "Agoda"
    if source == "Travel Agent" or sub == "TPO":
        return "TA"
    if source in ["Android App","IOS App","Web Booking","Mobile Web Booking","Website Booking","Direct"]:
        return "OYO"
    return "OBA"


# ================= FETCH DETAILS =================
async def fetch_booking_details(session, P, booking_no):
    url = "https://www.oyoos.com/hms_ms/api/v1/visibility/booking_details_with_entities"
    params = {
        "qid": P["QID"],
        "booking_id": booking_no,
        "role": 0,
        "platform": "OYOOS",
        "country_code": 1
    }
    cookies = {"uif": P["UIF"], "uuid": P["UUID"]}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "x-qid": str(P["QID"]),
        "x-source-client": "merchant"
    }

    for attempt in range(1, 4):
        try:
            async with session.get(
                url,
                params=params,
                headers=headers,
                cookies=cookies,
                timeout=DETAIL_TIMEOUT
            ) as r:
                if r.status != 200:
                    raise RuntimeError("DETAIL API FAILED")

                data = await r.json()
                booking = next(
                    iter(data.get("entities", {}).get("bookings", {}).values()),
                    {}
                )
                payments = booking.get("payments", [])

                payment_events = []

                for p in payments:
                    mode = p.get("mode", "")
                    amt = float(p.get("amount", 0) or 0)

                    if amt <= 0:
                        continue

                    created_at = str(p.get("created_at") or "").strip()
                    if not created_at:
                        continue

                    try:
                        dt = datetime.fromisoformat(created_at.replace("Z", ""))
                        dt = dt.astimezone(IST)
                    except Exception:
                        continue

                    pay_date = dt.strftime("%Y-%m-%d")
                    pay_time = dt.strftime("%H:%M")
                    pay_hour = dt.hour   # ⭐ important

                    # classification
                    if mode == "Cash at Hotel":
                        bucket = "cash"
                    elif mode == "UPI QR":
                        bucket = "qr"
                    elif mode == "oyo_wizard_discount":
                        bucket = "discount"
                    else:
                        bucket = "online"

                    payment_events.append({
                        "date": pay_date,
                        "time": pay_time,
                        "hour": pay_hour,
                        "mode": bucket,
                        "amt": amt
                    })

                return payment_events

        except Exception:
            await asyncio.sleep(2 + attempt)

    raise RuntimeError("DETAIL FETCH FAILED")

# ================= BATCH FETCH =================
async def fetch_bookings_batch(session, offset, f, t, P):
    url = "https://www.oyoos.com/hms_ms/api/v1/get_booking_with_ids"
    params = {
        "qid": P["QID"],
        "checkin_from": f,
        "checkin_till": t,
        "batch_count": 100,
        "batch_offset": offset,
        "visibility_required": "true",
        "additionalParams": "payment_hold_transaction,guest,stay_details",
        "decimal_price": "true",
        "ascending": "true",
        "sort_on": "checkin_date"
    }
    cookies = {"uif": P["UIF"], "uuid": P["UUID"]}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "x-qid": str(P["QID"]),
        "x-source-client": "merchant"
    }

    async with session.get(url, params=params, cookies=cookies, headers=headers, timeout=BATCH_TIMEOUT) as r:
        if r.status != 200:
            raise RuntimeError("BATCH API FAILED")
        return await r.json()

# ================= PROPERTY DETAILS API =================
async def fetch_property_details(session, P):
    url = "https://www.oyoos.com/hms_ms/api/v1/location/property-details"
    params = {"qid": P["QID"]}
    cookies = {"uif": P["UIF"], "uuid": P["UUID"]}
    headers = {"accept": "application/json", "x-qid": str(P["QID"]), "x-source-client": "merchant"}

    for attempt in range(1, 4):
        try:
            async with session.get(url, params=params, cookies=cookies, headers=headers, timeout=20) as r:
                if r.status != 200:
                    raise RuntimeError(f"PROPERTY DETAILS API FAILED ({r.status})")
                data = await r.json()
                return {
                    "name": str(data.get("name", "") or "").strip(),
                    "alternate_name": str(data.get("alternate_name", "") or "").strip(),
                    "plot_number": str(data.get("plot_number", "") or "").strip(),
                    "street": str(data.get("street", "") or "").strip(),
                    "pincode": str(data.get("pincode", "") or "").strip(),
                    "city": str(data.get("city", "") or "").strip(),
                    "country": str(data.get("country", "") or "").strip(),
                    "map_link": str(data.get("map_link", "") or "").strip(),
                }
        except Exception:
            await asyncio.sleep(2 + attempt)

    return {"name":"","alternate_name":"","plot_number":"","street":"","pincode":"","city":"","country":"","map_link":""}

# ================= FETCH TOTAL ROOMS =================
async def fetch_total_rooms(session, P):
    url = "https://www.oyoos.com/hms_ms/api/v1/hotels/roomsNew"
    params = {"qid": P["QID"]}
    cookies = {"uif": P["UIF"], "uuid": P["UUID"]}
    headers = {"accept": "application/json", "x-qid": str(P["QID"]), "x-source-client": "merchant"}

    for attempt in range(1, 4):
        try:
            async with session.get(url, params=params, cookies=cookies, headers=headers, timeout=ROOMS_TIMEOUT) as r:
                if r.status != 200:
                    raise RuntimeError("ROOM API FAILED")
                data = await r.json()
                rooms = data.get("rooms", {})
                return len(rooms)
        except Exception:
            await asyncio.sleep(2 + attempt)

    return 0

# ================= PROCESS PROPERTY =================
async def process_property(P, TF, TT, HF, HT):
    print(f"PROCESSING → {P['name']}")

    tf_dt = datetime.strptime(TF, "%Y-%m-%d").date()
    tt_dt = datetime.strptime(TT, "%Y-%m-%d").date()

    async with aiohttp.ClientSession() as session:
        total_rooms = await fetch_total_rooms(session, P)
        prop_details = await fetch_property_details(session, P)

        detail_semaphore = asyncio.Semaphore(DETAIL_PARALLEL_LIMIT)
        detail_cache = {}

        async def limited_detail_call(booking_no):
            async with detail_semaphore:
                if booking_no in detail_cache:
                    return detail_cache[booking_no]
                res = await fetch_booking_details(session, P, booking_no)
                detail_cache[booking_no] = res
                return res

        daily_collect = {}
        booking_date_mode_map = {}

        # ⭐ hourly cash storage
        hourly_cash = {h: 0.0 for h in range(24)}

        offset = 0
        while True:
            data = await fetch_bookings_batch(session, offset, HF, HT, P)
            if not data or not data.get("bookingIds"):
                break

            bookings = data.get("entities", {}).get("bookings", {})
            if not bookings:
                break

            tasks = []
            mapping = []

            for b in bookings.values():
                status = (b.get("status") or "").strip()
                if status not in ["Checked In", "Checked Out"]:
                    continue

                booking_no = b.get("booking_no")
                if not booking_no:
                    continue

                tasks.append(limited_detail_call(booking_no))
                mapping.append(b)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res, b in zip(results, mapping):
                if isinstance(res, Exception):
                    continue

                payment_events = res or []
                booking_no = b.get("booking_no")
                source = get_booking_source(b)

                for ev in payment_events:
                    d = ev.get("date")
                    if not d:
                        continue

                    try:
                        d_dt = datetime.strptime(d, "%Y-%m-%d").date()
                    except Exception:
                        continue

                    if not (tf_dt <= d_dt <= tt_dt):
                        continue

                    if d not in daily_collect:
                        daily_collect[d] = {
                            "cash": 0.0,
                            "qr": 0.0,
                            "online": 0.0,
                            "discount": 0.0
                        }

                    if (d, booking_no) not in booking_date_mode_map:
                        booking_date_mode_map[(d, booking_no)] = {
                            "cash": 0.0,
                            "qr": 0.0,
                            "online": 0.0,
                            "discount": 0.0,
                            "times": set(),
                            "b": b,
                            "source": source
                        }

                    mode = ev.get("mode")
                    amt = float(ev.get("amt", 0) or 0)
                    phour = ev.get("hour")
                    ptime = ev.get("time")

                    if ptime:
                        booking_date_mode_map[(d, booking_no)]["times"].add(ptime)

                    if mode == "cash":
                        daily_collect[d]["cash"] += amt
                        booking_date_mode_map[(d, booking_no)]["cash"] += amt

                        # ⭐ hourly aggregation
                        if phour is not None:
                            hourly_cash[phour] += amt

                    elif mode == "qr":
                        daily_collect[d]["qr"] += amt
                        booking_date_mode_map[(d, booking_no)]["qr"] += amt
                    elif mode == "discount":
                        daily_collect[d]["discount"] += amt
                        booking_date_mode_map[(d, booking_no)]["discount"] += amt
                    else:
                        daily_collect[d]["online"] += amt
                        booking_date_mode_map[(d, booking_no)]["online"] += amt

            if len(data.get("bookingIds", [])) < 100:
                break
            offset += 100

        # dataframe (unchanged)
        all_rows = []

        for (d, booking_no), vals in booking_date_mode_map.items():
            b = vals["b"]

            cash = vals["cash"]
            qr = vals["qr"]
            online = vals["online"]
            discount = vals["discount"]

            total_paid = cash + qr + online + discount
            times_str = ", ".join(sorted(vals["times"]))

            all_rows.append({
                "Date": d,
                "Booking Id": booking_no,
                "Guest Name": b.get("guest_name"),
                "Status": b.get("status"),
                "Booking Source": vals["source"],
                "Check In": b.get("checkin"),
                "Check Out": b.get("checkout"),
                "Cash": round(cash, 2),
                "QR": round(qr, 2),
                "Online": round(online, 2),
                "Discount": round(discount, 2),
                "Total Paid": round(total_paid, 2),
                "Time": times_str
            })

        df = pd.DataFrame(all_rows)

        if df.empty:
            df = pd.DataFrame(columns=[
                "Date", "Booking Id", "Guest Name", "Status",
                "Booking Source", "Check In", "Check Out",
                "Cash", "QR", "Online", "Discount",
                "Total Paid", "Time"
            ])

        df = df.sort_values(["Date", "Booking Id"], ascending=True)

        return (P["name"], df, total_rooms, prop_details, daily_collect, hourly_cash)


# ================= RETRY =================
async def run_property_with_retry(P, TF, TT, HF, HT, retries=3):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return await process_property(P, TF, TT, HF, HT)
        except Exception as e:
            last_error = e
            print(f"RETRY {attempt}/{retries} → {P['name']} :: {e}")
            await asyncio.sleep(2 + attempt * 2)
    raise RuntimeError(f"PROPERTY FAILED → {P['name']}") from last_error

async def run_property_limited(P, TF, TT, HF, HT):
    async with prop_semaphore:
        return await run_property_with_retry(P, TF, TT, HF, HT)

# ================= MAIN =================
async def main():
    print("========================================")
    print(" HOURLY CASH REPORT (PREMIUM)")
    print("========================================")

    global now
    now = datetime.now(IST)

    target_date = (now - timedelta(days=1)).date()
    TF = target_date.strftime("%Y-%m-%d")
    TT = TF

    HF = (target_date - timedelta(days=120)).strftime("%Y-%m-%d")
    HT = TF

    print("TARGET DATE :", TF)

    pending = {k: v for k, v in PROPERTIES.items()}
    success_results = {}

    # ================= FETCH DATA =================
    for run_attempt in range(1, MAX_FULL_RUN_RETRIES + 1):
        if not pending:
            break

        print(f"RUN ATTEMPT {run_attempt}")

        tasks = [run_property_limited(P, TF, TT, HF, HT) for P in pending.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        new_pending = {}

        for key, (P, result) in zip(list(pending.keys()), zip(pending.values(), results)):
            if isinstance(result, Exception):
                print(f"FAILED → {P['name']}")
                new_pending[key] = P
                continue

            success_results[key] = result
            print(f"SUCCESS → {P['name']}")

        pending = new_pending

        if pending:
            await asyncio.sleep(FULL_RUN_RETRY_DELAY)

    valid_results = [success_results[k] for k in PROPERTIES.keys() if k in success_results]

    if len(valid_results) != len(PROPERTIES):
        missing = [PROPERTIES[k]["name"] for k in PROPERTIES if k not in success_results]
        raise RuntimeError(f"DATA INCOMPLETE: Missing properties: {missing}")

    # ================= EXCEL =================

    wb = Workbook()
    wb.remove(wb.active)

    consolidated_hourly_cash = {h: 0.0 for h in range(24)}

    # ================= COLOR PALETTE =================
    def get_hour_color(hour):
        palette = [
            "0B3D91", "0F52BA", "1C6ED5", "2E86DE",
            "5DADE2", "85C1E9", "AED6F1", "F9E79F",
            "F7DC6F", "F4D03F", "F1C40F", "F39C12",
            "EB984E", "E67E22", "DC7633", "D35400",
            "CD6155", "C0392B", "A93226", "922B21",
            "7B241C", "641E16", "512E5F", "2C3E50"
        ]
        return palette[hour % 24]

    # ================= HOUR LABEL =================
    def hour_label(h):
        start = datetime(2000, 1, 1, h, 0)
        end = start + timedelta(hours=1)

        def fmt(dt):
            return dt.strftime("%I%p").lstrip("0")

        return f"{fmt(start)} - {fmt(end)}"

    # ================= SHEET BUILDER =================
    def create_sheet(ws, hourly_cash):

        ws.append(["Date", "Time (Hourly)", "Cash"])

        header_fill = PatternFill("solid", fgColor="1F4E78")
        header_font = Font(bold=True, color="FFFFFF", size=13)

        for col in range(1, 4):
            c = ws.cell(row=1, column=col)
            c.fill = header_fill
            c.font = header_font
            c.alignment = Alignment(horizontal="center", vertical="center")

        total = 0

        # 24 HOURS
        for h in range(24):

            cash = round(hourly_cash.get(h, 0), 2)
            total += cash

            ws.append([
                target_date.strftime("%Y-%m-%d"),
                hour_label(h),
                cash
            ])

            row = ws.max_row

            color = get_hour_color(h)
            fill = PatternFill("solid", fgColor=color)

            for col in range(1, 4):
                cell = ws.cell(row=row, column=col)
                cell.fill = fill
                cell.font = Font(bold=True, color="FFFFFF")
                cell.alignment = Alignment(horizontal="center")

        # TOTAL ROW
        ws.append(["", "TOTAL", round(total, 2)])
        total_row = ws.max_row

        for col in range(1, 4):
            c = ws.cell(row=total_row, column=col)
            c.fill = PatternFill("solid", fgColor="000000")
            c.font = Font(bold=True, color="FFFFFF", size=12)
            c.alignment = Alignment(horizontal="center")

        # WIDTH
        ws.column_dimensions["A"].width = 15
        ws.column_dimensions["B"].width = 18
        ws.column_dimensions["C"].width = 12

        # ================= BAR CHART =================
        chart = BarChart()
        chart.title = "Hourly Cash Collection"
        chart.height = 12
        chart.width = 24

        data = Reference(ws, min_col=3, min_row=1, max_row=25)
        cats = Reference(ws, min_col=2, min_row=2, max_row=25)

        chart.add_data(data, titles_from_data=True)
        chart.set_categories(cats)

        ws.add_chart(chart, "E2")

    # ================= PROPERTY SHEETS =================
    for name, df, total_rooms, prop_details, daily_collect, hourly_cash in valid_results:

        print(f"Creating sheet → {name}")

        ws = wb.create_sheet(name[:31])
        create_sheet(ws, hourly_cash)

        for h in range(24):
            consolidated_hourly_cash[h] += hourly_cash.get(h, 0)

    # ================= CONSOLIDATED =================
    print("Creating sheet → CONSOLIDATED")

    ws = wb.create_sheet("CONSOLIDATED")
    create_sheet(ws, consolidated_hourly_cash)

    # ================= SAVE + SEND =================
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    await send_telegram_excel_buffer(
        buffer,
        filename=f"Cash_Collection_{TF}.xlsx",
        caption="📊 Yesterday Hourly Cash Report (All Properties)"
    )

    print("✅ EXCEL SENT SUCCESSFULLY")

# ================= RUN =================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("SCRIPT CRASHED")
        print(e)
        traceback.print_exc()
        print("SCRIPT CRASHED", e, flush=True)
