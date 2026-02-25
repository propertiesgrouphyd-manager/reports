# ==============================
# ULTRA FAST ASYNC MULTI PROPERTY AUTOMATION
# HOURLY COLLECTION REPORT
# CASH + QR + ONLINE + TOTAL
# WITH RANKING + CHARTS
# ==============================

import os
import json
import asyncio
import aiohttp
from datetime import datetime, timedelta
import traceback
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.chart import BarChart, Reference
from openpyxl.chart.series import DataPoint
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
BATCH_TIMEOUT = 35


# ================= TELEGRAM =================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_MAP = json.loads(os.getenv("TELEGRAM_CHAT_MAP", "{}"))

def get_chat_id(name: str):
    return int(CHAT_MAP[name])

TELEGRAM_CHAT_ID = get_chat_id("6am")


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
                raise RuntimeError(await resp.text())


# ================= PROPERTIES =================

PROPERTIES_RAW = json.loads(os.getenv("OYO_PROPERTIES", "{}"))
PROPERTIES = {int(k): v for k, v in PROPERTIES_RAW.items()}


# ================= COLOR =================

def get_hour_color(hour):

    palette = [
        "EEF3FB","E8F0FA","E3EDFA","DEEAFA",
        "D9F2FF","DFF7FF","E6FBFF","FFF9DB",
        "FFF4CC","FFEFB3","FFE699","FFDD80",
        "FFE0CC","FFD6B3","FFCC99","FFC280",
        "FFD9D9","FFD1D1","FFC9C9","FFC1C1",
        "F3E5F5","EDE7F6","E8EAF6","E3F2FD"
    ]

    return palette[hour % 24]


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

    async with session.get(
        url,
        params=params,
        headers=headers,
        cookies=cookies,
        timeout=DETAIL_TIMEOUT
    ) as r:

        if r.status != 200:
            return []

        data = await r.json()

        booking = next(
            iter(data.get("entities", {}).get("bookings", {}).values()),
            {}
        )

        payments = booking.get("payments", [])

        events = []

        for p in payments:

            amt = float(p.get("amount", 0) or 0)
            if amt <= 0:
                continue

            created_at = str(p.get("created_at") or "").strip()
            if not created_at:
                continue

            try:
                dt = datetime.fromisoformat(created_at.replace("Z", ""))
                dt = dt.astimezone(IST)
            except:
                continue

            mode = p.get("mode", "")

            if mode == "Cash at Hotel":
                bucket = "cash"
            elif mode == "UPI QR":
                bucket = "qr"
            elif mode == "oyo_wizard_discount":
                continue
            else:
                bucket = "online"

            events.append({
                "date": dt.strftime("%Y-%m-%d"),
                "hour": dt.hour,
                "mode": bucket,
                "amt": amt
            })

        return events


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

    async with session.get(
        url,
        params=params,
        cookies=cookies,
        headers=headers,
        timeout=BATCH_TIMEOUT
    ) as r:

        if r.status != 200:
            raise RuntimeError("BATCH API FAILED")

        return await r.json()


# ================= PROCESS PROPERTY =================

async def process_property(P, TF, TT, HF, HT):

    print(f"PROCESSING → {P['name']}")

    tf_dt = datetime.strptime(TF, "%Y-%m-%d").date()
    tt_dt = datetime.strptime(TT, "%Y-%m-%d").date()

    async with aiohttp.ClientSession() as session:

        detail_semaphore = asyncio.Semaphore(DETAIL_PARALLEL_LIMIT)

        async def limited_detail_call(booking_no):
            async with detail_semaphore:
                return await fetch_booking_details(session, P, booking_no)

        hourly = {
            h: {"cash":0.0,"qr":0.0,"online":0.0,"total":0.0}
            for h in range(24)
        }

        offset = 0

        while True:

            data = await fetch_bookings_batch(session, offset, HF, HT, P)

            if not data or not data.get("bookingIds"):
                break

            bookings = data.get("entities", {}).get("bookings", {})

            tasks = []

            for b in bookings.values():

                status = (b.get("status") or "").strip()
                if status not in ["Checked In", "Checked Out"]:
                    continue

                booking_no = b.get("booking_no")
                if not booking_no:
                    continue

                tasks.append(limited_detail_call(booking_no))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:

                if isinstance(res, Exception):
                    continue

                for ev in res or []:

                    try:
                        d_dt = datetime.strptime(ev["date"], "%Y-%m-%d").date()
                    except:
                        continue

                    if not (tf_dt <= d_dt <= tt_dt):
                        continue

                    h = ev["hour"]
                    amt = float(ev["amt"])

                    mode = ev["mode"]

                    hourly[h][mode] += amt
                    hourly[h]["total"] += amt

            if len(data.get("bookingIds", [])) < 100:
                break

            offset += 100

        return (P["name"], hourly)


# ================= MAIN =================

async def main():

    global now
    now = datetime.now(IST)

    target_date = (now - timedelta(days=1)).date()

    TF = target_date.strftime("%Y-%m-%d")
    TT = TF

    HF = (target_date - timedelta(days=30)).strftime("%Y-%m-%d")
    HT = TF

    display_date = target_date.strftime("%d-%m-%Y")

    tasks = [
        process_property(P, TF, TT, HF, HT)
        for P in PROPERTIES.values()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    valid_results = [r for r in results if not isinstance(r, Exception)]

    wb = Workbook()
    wb.remove(wb.active)

    consolidated = {
        h: {"cash":0.0,"qr":0.0,"online":0.0,"total":0.0}
        for h in range(24)
    }

    property_totals = []

    def hour_label(h):
        s = datetime(2000,1,1,h)
        e = s + timedelta(hours=1)
        return f"{s.strftime('%I%p').lstrip('0')} - {e.strftime('%I%p').lstrip('0')}"

    def create_sheet(ws, data):

        ws.append(["Date","Time","Cash","QR","Online","Total"])

        for h in range(24):

            row = data[h]

            ws.append([
                display_date,
                hour_label(h),
                round(row["cash"],2),
                round(row["qr"],2),
                round(row["online"],2),
                round(row["total"],2)
            ])

    # PROPERTY SHEETS
    for name, hourly in valid_results:

        ws = wb.create_sheet(name[:31])
        create_sheet(ws, hourly)

        total_sum = 0

        for h in range(24):
            for k in consolidated[h]:
                consolidated[h][k] += hourly[h][k]

            total_sum += hourly[h]["total"]

        property_totals.append({
            "name": name,
            "total": total_sum
        })

    # CONSOLIDATED
    ws = wb.create_sheet("CONSOLIDATED")
    create_sheet(ws, consolidated)

    # PROPERTY RANKING
    ws = wb.create_sheet("PROPERTY RANKING")

    ws.append(["Rank","Property","Total Collection"])

    property_totals.sort(key=lambda x: x["total"], reverse=True)

    rank = 1
    for p in property_totals:
        ws.append([rank, p["name"], round(p["total"],2)])
        rank += 1

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    await send_telegram_excel_buffer(
        buffer,
        filename=f"Collection_{display_date}.xlsx",
        caption="Hourly Collection Report"
    )


if __name__ == "__main__":
    asyncio.run(main())
