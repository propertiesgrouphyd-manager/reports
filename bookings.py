# ==============================
# ULTRA FAST ASYNC MULTI PROPERTY AUTOMATION
# HOURLY BOOKING MODE REPORT
# EXACT SAME FORMAT AS CASH VERSION
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
prop_semaphore = asyncio.Semaphore(PROP_PARALLEL_LIMIT)

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


# ================= COLOR THEME =================

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


# ================= BOOKING SOURCE =================

def get_booking_source(b):

    source = str(b.get("source", "") or "")
    ota = str(b.get("ota_source", "") or "")
    sub = str(b.get("sub_source", "") or "")
    corp = bool(b.get("is_corporate", False))

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

    if source in ["Android App","IOS App","Web Booking","Mobile Web Booking","Website Booking","Direct"]:
        return "OYO"

    if source == "Travel Agent":
        return "TA"

    return "OBA"


# ================= BATCH FETCH =================

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
            raise RuntimeError("BATCH FAIL")

        return await r.json()


# ================= PROCESS PROPERTY =================

async def process_property(P, TF, TT, HF, HT):

    tf_dt = datetime.strptime(TF, "%Y-%m-%d").date()
    tt_dt = datetime.strptime(TT, "%Y-%m-%d").date()

    hourly = {
        h: {"OYO":0,"Walk-in":0,"MMT":0,"BDC":0,"Agoda":0,"CB":0,"TA":0,"OBA":0}
        for h in range(24)
    }

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

                checkin_time = b.get("checkin_time")
                if not checkin_time:
                    continue

                try:
                    dt = datetime.fromisoformat(checkin_time.replace("Z", ""))
                    dt = dt.astimezone(IST)
                except:
                    continue

                d_dt = dt.date()

                if not (tf_dt <= d_dt <= tt_dt):
                    continue

                hour = dt.hour
                src = get_booking_source(b)

                if src not in hourly[hour]:
                    src = "OBA"

                hourly[hour][src] += 1

            if len(data.get("bookingIds", [])) < 100:
                break

            offset += 100

    return (P["name"], hourly)


# ================= RETRY =================

async def run_property_with_retry(P, TF, TT, HF, HT, retries=3):

    for _ in range(retries):
        try:
            return await process_property(P, TF, TT, HF, HT)
        except:
            await asyncio.sleep(2)

    raise RuntimeError("PROPERTY FAILED")


async def run_property_limited(P, TF, TT, HF, HT):
    async with prop_semaphore:
        return await run_property_with_retry(P, TF, TT, HF, HT)


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

    pending = {k:v for k,v in PROPERTIES.items()}
    success = {}

    for _ in range(MAX_FULL_RUN_RETRIES):

        if not pending:
            break

        tasks = [run_property_limited(P, TF, TT, HF, HT) for P in pending.values()]
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

    consolidated = {
        h: {"OYO":0,"Walk-in":0,"MMT":0,"BDC":0,"Agoda":0,"CB":0,"TA":0,"OBA":0}
        for h in range(24)
    }


    def hour_label(h):
        s = datetime(2000,1,1,h,0)
        e = s + timedelta(hours=1)
        return f"{s.strftime('%I%p').lstrip('0')} - {e.strftime('%I%p').lstrip('0')}"


    def create_sheet(ws, hourly_data):

        thin = Border(
            left=Side(style="thin", color="DDDDDD"),
            right=Side(style="thin", color="DDDDDD"),
            top=Side(style="thin", color="DDDDDD"),
            bottom=Side(style="thin", color="DDDDDD"),
        )

        headers = [
            "Date","Time (Hourly)",
            "OYO","Walk-in","MMT","BDC","Agoda","CB","TA","OBA","Total"
        ]

        ws.append(headers)

        header_fill = PatternFill("solid", fgColor="1F4E78")

        for col in range(1,len(headers)+1):
            c = ws.cell(row=1,column=col)
            c.fill = header_fill
            c.font = Font(bold=True,color="FFFFFF")
            c.alignment = Alignment(horizontal="center", vertical="center")

        # widths
        widths = [14,18,10,10,10,10,10,10,10,10,12]
        for i,w in enumerate(widths, start=1):
            ws.column_dimensions[chr(64+i)].width = w

        totals = [0]*9

        for h in range(24):

            row = hourly_data[h]
            values = [
                row["OYO"],row["Walk-in"],row["MMT"],row["BDC"],
                row["Agoda"],row["CB"],row["TA"],row["OBA"]
            ]

            total = sum(values)

            ws.append([
                display_date,
                hour_label(h),
                *values,
                total
            ])

            r = ws.max_row
            fill = PatternFill("solid", fgColor=get_hour_color(h))

            for c in range(1,len(headers)+1):
                cell = ws.cell(row=r,column=c)
                cell.fill = fill
                cell.border = thin
                cell.alignment = Alignment(horizontal="center")

        # TOTAL ROW
        ws.append(["","","","","","","","","","",""])

        total_row = ws.max_row

        for col in range(1,len(headers)+1):
            c = ws.cell(row=total_row,column=col)
            c.fill = PatternFill("solid", fgColor="000000")
            c.font = Font(bold=True,color="FFFFFF")
            c.alignment = Alignment(horizontal="center")

        ws.cell(row=total_row,column=2).value = "TOTAL"


        # ===== CHARTS =====

        start_chart_row = ws.max_row + 2

        for i,col in enumerate(range(3,11)):

            chart = BarChart()
            chart.height = 10
            chart.width = 24
            chart.title = headers[col-1]
            chart.style = 10
            chart.legend = None

            data = Reference(ws, min_col=col, min_row=1, max_row=25)
            cats = Reference(ws, min_col=2, min_row=2, max_row=25)

            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)

            series = chart.series[0]
            points = []

            for hr in range(24):
                dp = DataPoint(idx=hr)
                dp.graphicalProperties.solidFill = get_hour_color(hr)
                points.append(dp)

            series.dPt = points

            ws.add_chart(chart, f"A{start_chart_row + i*15}")

        footer_row = start_chart_row + 8*15
        ws.cell(row=footer_row, column=1).value = "📊 Excel bar chart auto-generated"
        ws.cell(row=footer_row, column=1).font = Font(bold=True)


    # property sheets
    for name, hourly in results:

        ws = wb.create_sheet(name[:31])
        create_sheet(ws, hourly)

        for h in range(24):
            for k in consolidated[h]:
                consolidated[h][k] += hourly[h][k]


    # consolidated
    ws = wb.create_sheet("CONSOLIDATED")
    create_sheet(ws, consolidated)


    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    await send_telegram_excel_buffer(
        buffer,
        filename=f"Booking_Mode_{display_date}.xlsx",
        caption="📊 Hourly Booking Mode Report"
    )


# ================= RUN =================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
