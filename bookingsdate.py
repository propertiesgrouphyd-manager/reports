# ==============================
# ULTRA FAST ASYNC MULTI PROPERTY AUTOMATION
# HOURLY BOOKING MODE REPORT (ULTRA FAST)
# WITH CHARTS + PROPERTY RANKING
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

PROP_PARALLEL_LIMIT = 4
prop_semaphore = asyncio.Semaphore(PROP_PARALLEL_LIMIT)

BATCH_TIMEOUT = 40


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

    if source in [
        "Android App","IOS App","Web Booking",
        "Mobile Web Booking","Website Booking","Direct"
    ]:
        return "OYO"

    return "OBA"


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
        url, params=params, cookies=cookies,
        headers=headers, timeout=BATCH_TIMEOUT
    ) as r:

        if r.status != 200:
            raise RuntimeError("BATCH FAIL")

        return await r.json()




def parse_oyo_time(val):

    if not val:
        return None

    s = str(val).replace("Z", "+00:00").strip()

    try:
        dt = datetime.fromisoformat(s)
    except:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except:
            return None

    # ===== ENSURE TZ =====
    if dt.tzinfo is None:
        dt = IST.localize(dt)

    return dt

# ================= PROCESS PROPERTY =================

async def process_property(P, TF, TT, HF, HT):

    print(f"PROCESSING → {P['name']}")

    tf_dt = datetime.strptime(TF, "%Y-%m-%d").date()
    tt_dt = datetime.strptime(TT, "%Y-%m-%d").date()

    hourly_map = {}

    for h in range(24):
        hourly_map[h] = {
            "OYO":0,"Walk-in":0,"MMT":0,"BDC":0,
            "Agoda":0,"CB":0,"TA":0,"OBA":0
        }

    async with aiohttp.ClientSession() as session:

        offset = 0

        while True:

            data = await fetch_bookings_batch(session, offset, HF, HT, P)

            if not data or not data.get("bookingIds"):
                break

            bookings = data.get("entities", {}).get("bookings", {})

            for b in bookings.values():

                status = (b.get("status") or "").strip()

                if status not in ["Checked In", "Checked Out"]:
                    continue

                # ===== TIME SOURCE (FIXED) =====
                time_raw = (
                    b.get("checkin_time")
                    or b.get("created_at")
                    or b.get("inserted_at")
                )

                dt = parse_oyo_time(time_raw)

                if not dt:
                    continue

                dt = dt.astimezone(IST)

                event_date = dt.date()

                if not (tf_dt <= event_date <= tt_dt):
                    continue

                hour = dt.hour

                src = get_booking_source(b)

                if src not in hourly_map[hour]:
                    src = "OBA"

                rooms = int(
                    b.get("no_of_rooms")
                    or b.get("oyo_rooms")
                    or 1
                )

                hourly_map[hour][src] += rooms

            if len(data.get("bookingIds", [])) < 100:
                break

            offset += 100

    return (P["name"], hourly_map)


# ================= RETRY =================

async def run_property_with_retry(P, TF, TT, HF, HT, retries=3):

    for attempt in range(retries):
        try:
            return await process_property(P, TF, TT, HF, HT)
        except Exception as e:
            print(f"RETRY {attempt+1} → {P['name']} :: {e}")
            await asyncio.sleep(2)

    raise RuntimeError("PROPERTY FAILED")


async def run_property_limited(P, TF, TT, HF, HT):
    async with prop_semaphore:
        return await run_property_with_retry(P, TF, TT, HF, HT)


# ================= UTIL =================

def hour_label(h):

    start = datetime(2000,1,1,h,0)
    end = start + timedelta(hours=1)

    return f"{start.strftime('%I%p').lstrip('0')} - {end.strftime('%I%p').lstrip('0')}"


def autofit_columns(ws):

    for column_cells in ws.columns:

        max_length = 0
        col_letter = column_cells[0].column_letter

        for cell in column_cells:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass

        adjusted = (max_length * 1.4) + 4

        if col_letter == "A":
            adjusted = max(adjusted, 16)

        if col_letter == "B":
            adjusted = max(adjusted, 22)

        ws.column_dimensions[col_letter].width = min(adjusted, 45)


# ================= MAIN =================

async def main():

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

    consolidated = {h:{
        "OYO":0,"Walk-in":0,"MMT":0,"BDC":0,
        "Agoda":0,"CB":0,"TA":0,"OBA":0
    } for h in range(24)}

    property_totals = []

    thin = Border(
        left=Side(style="thin", color="DDDDDD"),
        right=Side(style="thin", color="DDDDDD"),
        top=Side(style="thin", color="DDDDDD"),
        bottom=Side(style="thin", color="DDDDDD"),
    )

    # ================= SHEET BUILDER =================

    def create_sheet(ws, hourly_map):

        headers = [
            "Date","Time (Hourly)",
            "OYO","Walk-in","MMT","BDC",
            "Agoda","CB","TA","OBA","Total"
        ]

        ws.append(headers)

        header_fill = PatternFill("solid", fgColor="1F4E78")

        for col in range(1,len(headers)+1):
            c = ws.cell(row=1,column=col)
            c.fill = header_fill
            c.font = Font(bold=True,color="FFFFFF")
            c.alignment = Alignment(horizontal="center")

        totals = [0]*8

        for h in range(24):

            row = hourly_map[h]

            values = [
                row["OYO"],row["Walk-in"],row["MMT"],row["BDC"],
                row["Agoda"],row["CB"],row["TA"],row["OBA"]
            ]

            total = sum(values)

            for i,v in enumerate(values):
                totals[i]+=v

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

        ws.append(["", "TOTAL", *totals, sum(totals)])

        total_row = ws.max_row

        for col in range(1,len(headers)+1):
            cell = ws.cell(row=total_row,column=col)
            cell.fill = PatternFill("solid", fgColor="000000")
            cell.font = Font(bold=True,color="FFFFFF")
            cell.alignment = Alignment(horizontal="center")

        # ===== CHARTS =====
        start_chart_row = ws.max_row + 3
        chart_gap = 22

        for i,col in enumerate(range(3,12)):

            chart = BarChart()
            chart.height = 12
            chart.width = 26
            chart.title = headers[col-1]
            chart.legend = None

            data = Reference(ws, min_col=col, min_row=1, max_row=25)
            cats = Reference(ws, min_col=2, min_row=2, max_row=25)

            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)

            series = chart.series[0]
            pts = []

            for idx in range(24):
                dp = DataPoint(idx=idx)
                dp.graphicalProperties.solidFill = get_hour_color(idx)
                pts.append(dp)

            series.dPt = pts

            ws.add_chart(chart, f"A{start_chart_row + i*chart_gap}")

        autofit_columns(ws)


    # ================= PROPERTY SHEETS =================

    for name,hourly_map in results:

        ws = wb.create_sheet(name[:31])
        create_sheet(ws,hourly_map)

        total_prop = 0

        for h in range(24):

            for k in consolidated[h]:
                consolidated[h][k]+=hourly_map[h][k]

            total_prop += sum(hourly_map[h].values())

        property_totals.append({
            "name":name,
            "total":total_prop
        })


    # ================= CONSOLIDATED =================

    ws = wb.create_sheet("CONSOLIDATED")
    create_sheet(ws,consolidated)


    # ================= PROPERTY RANKING =================

    ws = wb.create_sheet("PROPERTY RANKING")

    headers = ["Rank","Property","Total Bookings","Badge"]
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="1F4E78")

    for col in range(1,5):
        c = ws.cell(row=1,column=col)
        c.fill = header_fill
        c.font = Font(bold=True,color="FFFFFF")
        c.alignment = Alignment(horizontal="center")

    property_totals.sort(key=lambda x:x["total"], reverse=True)

    def get_medal(rank):
        if rank == 1: return "🥇 Gold"
        if rank == 2: return "🥈 Silver"
        if rank == 3: return "🥉 Bronze"
        return ""

    rank = 1

    for idx,p in enumerate(property_totals):

        medal = get_medal(rank)

        ws.append([rank,p["name"],p["total"],medal])

        r = ws.max_row
        fill = PatternFill("solid", fgColor=get_hour_color(idx))

        for c in range(1,5):
            cell = ws.cell(row=r,column=c)
            cell.fill = fill
            cell.border = thin
            cell.alignment = Alignment(horizontal="center")

        rank+=1

    autofit_columns(ws)


    # ================= SAVE =================

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    await send_telegram_excel_buffer(
        buffer,
        filename=f"Hourly_Booking_Mode_{display_date}.xlsx",
        caption="📊 Hourly Booking Mode Report"
    )


# ================= RUN =================

if __name__ == "__main__":

    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
