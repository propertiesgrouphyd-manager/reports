# ==============================
# ULTRA FAST ASYNC MULTI PROPERTY AUTOMATION
# DATE-WISE COLLECTION REPORT (CASH + QR + ONLINE + TOTAL)
# MONTH START → TODAY
# WITH PROPERTY RANKING
# FINAL FIXED VERSION
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

MAX_FULL_RUN_RETRIES = 5
FULL_RUN_RETRY_DELAY = 10

PROP_PARALLEL_LIMIT = 3
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

def get_row_color(index):

    palette = [
        "EEF3FB","E8F0FA","E3EDFA","DEEAFA",
        "D9F2FF","DFF7FF","E6FBFF","FFF9DB",
        "FFF4CC","FFEFB3","FFE699","FFDD80",
        "FFE0CC","FFD6B3","FFCC99","FFC280",
        "FFD9D9","FFD1D1","FFC9C9","FFC1C1",
        "F3E5F5","EDE7F6","E8EAF6","E3F2FD"
    ]

    return palette[index % len(palette)]


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

            created_at = str(p.get("created_at") or "")
            if not created_at:
                continue

            try:
                dt = datetime.fromisoformat(created_at.replace("Z", ""))
                dt = dt.astimezone(IST)
            except:
                continue

            mode_raw = p.get("mode", "")

            if mode_raw == "Cash at Hotel":
                bucket = "cash"
            elif mode_raw == "UPI QR":
                bucket = "qr"
            else:
                bucket = "online"

            events.append({
                "date": dt.date(),
                "mode": bucket,
                "amt": amt
            })

        return events


# ================= FETCH BOOKINGS =================

async def fetch_bookings_batch(session, offset, f, t, P):

    url = "https://www.oyoos.com/hms_ms/api/v1/get_booking_with_ids"

    params = {
        "qid": P["QID"],
        "checkin_from": f,
        "checkin_till": t,
        "batch_count": 100,
        "batch_offset": offset
    }

    cookies = {"uif": P["UIF"], "uuid": P["UUID"]}

    headers = {
        "accept": "application/json",
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
            return {}

        return await r.json()


# ================= PROCESS PROPERTY =================

async def process_property(P, TF, TT, date_list):

    date_map = {
        d: {"cash":0,"qr":0,"online":0,"total":0}
        for d in date_list
    }

    async with aiohttp.ClientSession() as session:

        offset = 0

        while True:

            data = await fetch_bookings_batch(session, offset, TF, TT, P)

            if not data or not data.get("bookingIds"):
                break

            bookings = data.get("entities", {}).get("bookings", {})

            for b in bookings.values():

                if b.get("status") not in ["Checked In","Checked Out"]:
                    continue

                booking_no = b.get("booking_no")
                if not booking_no:
                    continue

                details = await fetch_booking_details(session, P, booking_no)

                for ev in details:

                    d = ev["date"]
                    if d not in date_map:
                        continue

                    mode = ev["mode"]
                    amt = float(ev["amt"])

                    date_map[d][mode] += amt
                    date_map[d]["total"] += amt

            if len(data.get("bookingIds", [])) < 100:
                break

            offset += 100

    return (P["name"], date_map)


# ================= MAIN =================

async def main():

    today = datetime.now(IST).date()
    month_start = today.replace(day=1)

    TF = month_start.strftime("%Y-%m-%d")
    TT = today.strftime("%Y-%m-%d")

    display_month = today.strftime("%B %Y")

    # date list
    date_list = []
    d = month_start
    while d <= today:
        date_list.append(d)
        d += timedelta(days=1)

    # ================= FETCH =================

    tasks = [
        process_property(P, TF, TT, date_list)
        for P in PROPERTIES.values()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    valid_results = []
    for r in results:
        if isinstance(r, Exception):
            continue
        valid_results.append(r)

    # ================= EXCEL =================

    wb = Workbook()
    wb.remove(wb.active)

    consolidated = {
        d: {"cash":0,"qr":0,"online":0,"total":0}
        for d in date_list
    }

    property_totals = []

    thin = Border(
        left=Side(style="thin", color="DDDDDD"),
        right=Side(style="thin", color="DDDDDD"),
        top=Side(style="thin", color="DDDDDD"),
        bottom=Side(style="thin", color="DDDDDD"),
    )

    def create_sheet(ws, data_map):

        headers = ["Date","Cash","QR","Online","Total"]
        ws.append(headers)

        header_fill = PatternFill("solid", fgColor="1F4E78")

        for col in range(1,6):
            c = ws.cell(row=1,column=col)
            c.fill = header_fill
            c.font = Font(bold=True,color="FFFFFF")
            c.alignment = Alignment(horizontal="center")

        widths = [14,12,12,12,14]
        for i,w in enumerate(widths,start=1):
            ws.column_dimensions[chr(64+i)].width = w

        totals = {"cash":0,"qr":0,"online":0,"total":0}

        for idx, d in enumerate(date_list):

            row = data_map[d]

            ws.append([
                d.strftime("%d-%m-%Y"),
                round(row["cash"],2),
                round(row["qr"],2),
                round(row["online"],2),
                round(row["total"],2)
            ])

            totals["cash"] += row["cash"]
            totals["qr"] += row["qr"]
            totals["online"] += row["online"]
            totals["total"] += row["total"]

            r = ws.max_row
            fill = PatternFill("solid", fgColor=get_row_color(idx))

            for c in range(1,6):
                cell = ws.cell(row=r,column=c)
                cell.fill = fill
                cell.border = thin
                cell.alignment = Alignment(horizontal="center")

        ws.append([
            "TOTAL",
            round(totals["cash"],2),
            round(totals["qr"],2),
            round(totals["online"],2),
            round(totals["total"],2)
        ])

        total_row = ws.max_row

        for c in range(1,6):
            cell = ws.cell(row=total_row,column=c)
            cell.fill = PatternFill("solid", fgColor="000000")
            cell.font = Font(bold=True,color="FFFFFF")
            cell.alignment = Alignment(horizontal="center")

    # ================= PROPERTY SHEETS =================

    for name, data in valid_results:

        ws = wb.create_sheet(name[:31])
        create_sheet(ws, data)

        totals = {"cash":0,"qr":0,"online":0,"total":0}

        for d in date_list:

            for k in consolidated[d]:
                consolidated[d][k] += data[d][k]

            totals["cash"] += data[d]["cash"]
            totals["qr"] += data[d]["qr"]
            totals["online"] += data[d]["online"]
            totals["total"] += data[d]["total"]

        property_totals.append({
            "name": name,
            **totals
        })

    # ================= CONSOLIDATED =================

    ws = wb.create_sheet("CONSOLIDATED")
    create_sheet(ws, consolidated)

    # ================= PROPERTY RANKING =================

    ws = wb.create_sheet("PROPERTY RANKING")

    headers = ["Rank","Property","Cash","QR","Online","Total"]
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="1F4E78")

    for col in range(1,7):
        c = ws.cell(row=1,column=col)
        c.fill = header_fill
        c.font = Font(bold=True,color="FFFFFF")
        c.alignment = Alignment(horizontal="center")

    widths = [8,25,12,12,12,14]
    for i,w in enumerate(widths,start=1):
        ws.column_dimensions[chr(64+i)].width = w

    property_totals.sort(key=lambda x: x["total"], reverse=True)

    rank = 1
    for p in property_totals:

        ws.append([
            rank,
            p["name"],
            round(p["cash"],2),
            round(p["qr"],2),
            round(p["online"],2),
            round(p["total"],2)
        ])

        r = ws.max_row

        for c in range(1,7):
            cell = ws.cell(row=r,column=c)
            cell.border = thin
            cell.alignment = Alignment(horizontal="center")

        rank += 1

    # ================= SAVE =================

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    await send_telegram_excel_buffer(
        buffer,
        filename=f"Collection_Datewise_{display_month}.xlsx",
        caption="📊 Date-wise Collection Report"
    )


# ================= RUN =================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
