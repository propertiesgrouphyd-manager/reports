# ==============================
# ULTRA FAST ASYNC MULTI PROPERTY AUTOMATION
# DATE-WISE COLLECTION REPORT (CASH + QR + ONLINE + TOTAL)
# MONTH START → TODAY
# WITH PROPERTY RANKING + BAR CHARTS
# SAME ENGINE AS WORKING CASH SCRIPT
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
                    raise RuntimeError("DETAIL FAIL")

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
                    except Exception:
                        continue

                    mode_raw = p.get("mode", "")

                    if mode_raw == "Cash at Hotel":
                        bucket = "cash"
                    elif mode_raw == "UPI QR":
                        bucket = "qr"
                    else:
                        bucket = "online"

                    events.append({
                        "date": dt.strftime("%Y-%m-%d"),
                        "mode": bucket,
                        "amt": amt
                    })

                return events

        except Exception:
            await asyncio.sleep(2 + attempt)

    return []


# ================= FETCH BOOKINGS =================

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
            raise RuntimeError("BATCH FAIL")

        return await r.json()


# ================= PROCESS PROPERTY =================

async def process_property(P, TF, TT, HF, HT, date_list):

    print(f"PROCESSING → {P['name']}")

    tf_dt = datetime.strptime(TF, "%Y-%m-%d").date()
    tt_dt = datetime.strptime(TT, "%Y-%m-%d").date()

    async with aiohttp.ClientSession() as session:

        detail_semaphore = asyncio.Semaphore(DETAIL_PARALLEL_LIMIT)
        detail_cache = {}

        async def limited_detail_call(booking_no):
            async with detail_semaphore:
                if booking_no in detail_cache:
                    return detail_cache[booking_no]
                res = await fetch_booking_details(session, P, booking_no)
                detail_cache[booking_no] = res
                return res

        date_map = {
            d: {"cash":0.0,"qr":0.0,"online":0.0,"total":0.0}
            for d in date_list
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

                    if d_dt not in date_map:
                        continue

                    amt = float(ev["amt"])
                    mode = ev["mode"]

                    if mode == "cash":
                        date_map[d_dt]["cash"] += amt
                    elif mode == "qr":
                        date_map[d_dt]["qr"] += amt
                    else:
                        date_map[d_dt]["online"] += amt

                    date_map[d_dt]["total"] += amt

            if len(data.get("bookingIds", [])) < 100:
                break

            offset += 100

        return (P["name"], date_map)


# ================= MAIN =================

async def main():
    print("========================================")
    print(" OYO MONTHLY TELEGRAM AUTOMATION")
    print("========================================")

    global now
    now = datetime.now(IST)


    # ================= BUSINESS DATE CUTOVER (12 PM RULE) =================
    # ================= ALWAYS YESTERDAY =================
    target_date = (now - timedelta(days=1)).date()


    # ================= PREVIOUS MONTH (BASED ON TARGET_DATE) =================
    # ================= CURRENT MONTH TO YESTERDAY =================
    TF = target_date.replace(day=1).strftime("%Y-%m-%d")
    TT = target_date.strftime("%Y-%m-%d")

    # NEW FEATURE: total days in range
    target_days = (datetime.strptime(TT, "%Y-%m-%d") - datetime.strptime(TF, "%Y-%m-%d")).days + 1


    # ================= HISTORY RANGE (120 DAYS BEFORE → TARGET_DATE) =================
    HF = (target_date - timedelta(days=120)).strftime("%Y-%m-%d")
    HT = target_date.strftime("%Y-%m-%d")
    
    MONTH_LABEL = datetime.strptime(TF, "%Y-%m-%d").strftime("%B %Y")

    date_list = []
    d = month_start
    while d <= today:
        date_list.append(d)
        d += timedelta(days=1)

    tasks = [
        process_property(P, TF, TT, HF, HT, date_list)
        for P in PROPERTIES.values()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    valid_results = [r for r in results if not isinstance(r, Exception)]

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

        # ===== CHARTS =====

        start_row = ws.max_row + 3

        titles = ["Cash","QR","Online","Total"]

        for i,col in enumerate(range(2,6)):

            chart = BarChart()
            chart.height = 12
            chart.width = 26
            chart.title = titles[i]
            chart.legend = None

            data = Reference(ws, min_col=col, min_row=1, max_row=len(date_list)+1)
            cats = Reference(ws, min_col=1, min_row=2, max_row=len(date_list)+1)

            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)

            series = chart.series[0]
            pts = []

            for idx in range(len(date_list)):
                dp = DataPoint(idx=idx)
                dp.graphicalProperties.solidFill = get_row_color(idx)
                pts.append(dp)

            series.dPt = pts

            ws.add_chart(chart, f"A{start_row + i*22}")

    # ===== PROPERTY SHEETS =====

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

    # ===== CONSOLIDATED =====

    ws = wb.create_sheet("CONSOLIDATED")
    create_sheet(ws, consolidated)

    # ===== PROPERTY RANKING =====

    ws = wb.create_sheet("PROPERTY RANKING")

    headers = ["Rank","Property","Cash","QR","Online","Total"]
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="1F4E78")

    for col in range(1,7):
        c = ws.cell(row=1,column=col)
        c.fill = header_fill
        c.font = Font(bold=True,color="FFFFFF")
        c.alignment = Alignment(horizontal="center")

    widths = [8,28,12,12,12,14]
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

        rank += 1

    # ===== SAVE =====

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    await send_telegram_excel_buffer(
        buffer,
        filename=f"Collection_Datewise_{MONTH_LABEL}.xlsx",
        caption="📊 Date-wise Collection Report"
    )


# ================= RUN =================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
