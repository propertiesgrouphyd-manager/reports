# ==============================
# ULTRA FAST ASYNC MULTI PROPERTY AUTOMATION
# DATE-WISE CASH COLLECTION REPORT
# MONTH START → TODAY
# WITH PROPERTY RANKING
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

    return palette[index % len(palette))


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
            raise RuntimeError("DETAIL API FAILED")

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

            if p.get("mode") != "Cash at Hotel":
                continue

            events.append({
                "date": dt.date(),
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
            raise RuntimeError("BATCH API FAILED")

        return await r.json()


# ================= PROCESS PROPERTY =================

async def process_property(P, TF, TT, HF, HT, date_list):

    date_map = {d: 0.0 for d in date_list}

    async with aiohttp.ClientSession() as session:

        offset = 0

        while True:

            data = await fetch_bookings_batch(session, offset, HF, HT, P)

            if not data or not data.get("bookingIds"):
                break

            bookings = data.get("entities", {}).get("bookings", {})

            for b in bookings.values():

                if b.get("status") not in ["Checked In", "Checked Out"]:
                    continue

                booking_no = b.get("booking_no")
                if not booking_no:
                    continue

                details = await fetch_booking_details(session, P, booking_no)

                for ev in details:

                    d = ev["date"]
                    if d not in date_map:
                        continue

                    date_map[d] += float(ev["amt"])

            if len(data.get("bookingIds", [])) < 100:
                break

            offset += 100

    return (P["name"], date_map)


# ================= RETRY =================

async def run_property_with_retry(P, TF, TT, HF, HT, date_list, retries=3):

    for _ in range(retries):
        try:
            return await process_property(P, TF, TT, HF, HT, date_list)
        except:
            await asyncio.sleep(2)

    raise RuntimeError("PROPERTY FAILED")


async def run_property_limited(P, TF, TT, HF, HT, date_list):
    async with prop_semaphore:
        return await run_property_with_retry(P, TF, TT, HF, HT, date_list)


# ================= MAIN =================

async def main():

    today = datetime.now(IST).date()
    month_start = today.replace(day=1)

    TF = month_start.strftime("%Y-%m-%d")
    TT = today.strftime("%Y-%m-%d")

    HF = TF
    HT = TT

    display_month = today.strftime("%B %Y")

    date_list = []
    d = month_start
    while d <= today:
        date_list.append(d)
        d += timedelta(days=1)

    pending = {k:v for k,v in PROPERTIES.items()}
    success = {}

    for _ in range(MAX_FULL_RUN_RETRIES):

        if not pending:
            break

        tasks = [
            run_property_limited(P, TF, TT, HF, HT, date_list)
            for P in pending.values()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        new_pending = {}

        for key,(P,res) in zip(list(pending.keys()), zip(pending.values(), results)):
            if isinstance(res, Exception):
                new_pending[key] = P
            else:
                success[key] = res

        pending = new_pending

    results = [success[k] for k in PROPERTIES if k in success]

    wb = Workbook()
    wb.remove(wb.active)

    consolidated = {d: 0.0 for d in date_list}
    property_totals = []


    # ================= SHEET BUILDER =================

    def create_sheet(ws, data_map):

        thin = Border(
            left=Side(style="thin", color="DDDDDD"),
            right=Side(style="thin", color="DDDDDD"),
            top=Side(style="thin", color="DDDDDD"),
            bottom=Side(style="thin", color="DDDDDD"),
        )

        ws.append(["Date","Cash"])

        header_fill = PatternFill("solid", fgColor="1F4E78")

        for col in range(1,3):
            c = ws.cell(row=1,column=col)
            c.fill = header_fill
            c.font = Font(bold=True,color="FFFFFF")
            c.alignment = Alignment(horizontal="center")

        ws.column_dimensions["A"].width = 14
        ws.column_dimensions["B"].width = 14

        total = 0

        for idx, d in enumerate(date_list):

            cash = round(data_map[d],2)
            total += cash

            ws.append([d.strftime("%d-%m-%Y"), cash])

            r = ws.max_row
            fill = PatternFill("solid", fgColor=get_row_color(idx))

            for c in range(1,3):
                cell = ws.cell(row=r,column=c)
                cell.fill = fill
                cell.border = thin
                cell.alignment = Alignment(horizontal="center")

        ws.append(["TOTAL", round(total,2)])

        total_row = ws.max_row

        for c in range(1,3):
            cell = ws.cell(row=total_row,column=c)
            cell.fill = PatternFill("solid", fgColor="000000")
            cell.font = Font(bold=True,color="FFFFFF")
            cell.alignment = Alignment(horizontal="center")

        # ===== CHART =====

        chart = BarChart()
        chart.height = 12
        chart.width = 26
        chart.title = "Cash Collection"
        chart.legend = None

        data = Reference(ws, min_col=2, min_row=1, max_row=len(date_list)+1)
        cats = Reference(ws, min_col=1, min_row=2, max_row=len(date_list)+1)

        chart.add_data(data, titles_from_data=True)
        chart.set_categories(cats)

        series = chart.series[0]
        pts = []

        for hr in range(len(date_list)):
            dp = DataPoint(idx=hr)
            dp.graphicalProperties.solidFill = get_row_color(hr)
            pts.append(dp)

        series.dPt = pts

        ws.add_chart(chart, f"A{ws.max_row+3}")

        footer = ws.max_row + 22
        ws.cell(row=footer, column=1).value = "📊 Excel bar chart auto-generated"
        ws.cell(row=footer, column=1).font = Font(bold=True)


    # ===== PROPERTY SHEETS =====

    for name, data in results:

        ws = wb.create_sheet(name[:31])
        create_sheet(ws, data)

        total_cash = 0

        for d in date_list:
            consolidated[d] += data[d]
            total_cash += data[d]

        property_totals.append({
            "name": name,
            "cash": total_cash
        })


    # ===== CONSOLIDATED =====

    ws = wb.create_sheet("CONSOLIDATED")
    create_sheet(ws, consolidated)


    # ===== PROPERTY RANKING =====

    ws = wb.create_sheet("PROPERTY RANKING")

    ws.append(["Rank","Property","Cash"])

    header_fill = PatternFill("solid", fgColor="1F4E78")

    for col in range(1,4):
        c = ws.cell(row=1,column=col)
        c.fill = header_fill
        c.font = Font(bold=True,color="FFFFFF")
        c.alignment = Alignment(horizontal="center")

    property_totals.sort(key=lambda x: x["cash"], reverse=True)

    rank = 1
    for p in property_totals:
        ws.append([rank, p["name"], round(p["cash"],2)])
        rank += 1


    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    await send_telegram_excel_buffer(
        buffer,
        filename=f"Cash_Datewise_{display_month}.xlsx",
        caption="📊 Date-wise Cash Collection Report"
    )


# ================= RUN =================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
