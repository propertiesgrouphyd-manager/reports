# ==============================
# ULTRA FAST ASYNC MULTI PROPERTY AUTOMATION
# HOURLY COLLECTION REPORT (CASH + QR + ONLINE + TOTAL)
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

            mode_raw = p.get("mode", "")

            if mode_raw == "Cash at Hotel":
                bucket = "cash"
            elif mode_raw == "UPI QR":
                bucket = "qr"
            elif mode_raw == "oyo_wizard_discount":
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

async def process_property(P, TF, TT, HF, HT):

    tf_dt = datetime.strptime(TF, "%Y-%m-%d").date()
    tt_dt = datetime.strptime(TT, "%Y-%m-%d").date()

    async with aiohttp.ClientSession() as session:

        hourly = {
            h: {"cash":0,"qr":0,"online":0,"total":0}
            for h in range(24)
        }

        offset = 0

        while True:

            data = await fetch_bookings_batch(session, offset, HF, HT, P)

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

                    try:
                        d_dt = datetime.strptime(ev["date"], "%Y-%m-%d").date()
                    except:
                        continue

                    if not (tf_dt <= d_dt <= tt_dt):
                        continue

                    hour = ev["hour"]
                    amt = float(ev["amt"])
                    mode = ev["mode"]

                    hourly[hour][mode] += amt
                    hourly[hour]["total"] += amt

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
        h: {"cash":0,"qr":0,"online":0,"total":0}
        for h in range(24)
    }

    property_totals = []


    def hour_label(h):
        s = datetime(2000,1,1,h,0)
        e = s + timedelta(hours=1)
        return f"{s.strftime('%I%p').lstrip('0')} - {e.strftime('%I%p').lstrip('0')}"


    def create_sheet(ws, hourly):

        thin = Border(
            left=Side(style="thin", color="DDDDDD"),
            right=Side(style="thin", color="DDDDDD"),
            top=Side(style="thin", color="DDDDDD"),
            bottom=Side(style="thin", color="DDDDDD"),
        )

        headers = ["Date","Time (Hourly)","Cash","QR","Online","Total"]
        ws.append(headers)

        header_fill = PatternFill("solid", fgColor="1F4E78")

        for col in range(1,7):
            c = ws.cell(row=1,column=col)
            c.fill = header_fill
            c.font = Font(bold=True,color="FFFFFF")
            c.alignment = Alignment(horizontal="center")

        widths = [14,18,12,12,12,14]
        for i,w in enumerate(widths,start=1):
            ws.column_dimensions[chr(64+i)].width = w


        totals = {"cash":0,"qr":0,"online":0,"total":0}

        for h in range(24):

            data = hourly[h]

            cash = round(data["cash"],2)
            qr = round(data["qr"],2)
            online = round(data["online"],2)
            total = round(data["total"],2)

            totals["cash"] += cash
            totals["qr"] += qr
            totals["online"] += online
            totals["total"] += total

            ws.append([display_date,hour_label(h),cash,qr,online,total])

            r = ws.max_row
            fill = PatternFill("solid", fgColor=get_hour_color(h))

            for c in range(1,7):
                cell = ws.cell(row=r,column=c)
                cell.fill = fill
                cell.border = thin
                cell.alignment = Alignment(horizontal="center")


        ws.append(["","TOTAL",
                   totals["cash"],
                   totals["qr"],
                   totals["online"],
                   totals["total"]])

        total_row = ws.max_row

        for c in range(1,7):
            cell = ws.cell(row=total_row,column=c)
            cell.fill = PatternFill("solid", fgColor="000000")
            cell.font = Font(bold=True,color="FFFFFF")
            cell.alignment = Alignment(horizontal="center")


        def add_chart(title,col,start):

            chart = BarChart()
            chart.title = title
            chart.height = 12
            chart.width = 26
            chart.legend = None

            data = Reference(ws,min_col=col,min_row=1,max_row=25)
            cats = Reference(ws,min_col=2,min_row=2,max_row=25)

            chart.add_data(data,titles_from_data=True)
            chart.set_categories(cats)

            series = chart.series[0]
            pts = []

            for hr in range(24):
                dp = DataPoint(idx=hr)
                dp.graphicalProperties.solidFill = get_hour_color(hr)
                pts.append(dp)

            series.dPt = pts

            ws.add_chart(chart,f"A{start}")

            return start+22   # enough gap


        chart_row = ws.max_row + 2

        chart_row = add_chart("Cash Collection",3,chart_row)
        chart_row = add_chart("QR Collection",4,chart_row)
        chart_row = add_chart("Online Collection",5,chart_row)
        chart_row = add_chart("Total Collection",6,chart_row)

        footer = chart_row + 2
        ws.cell(row=footer,column=1).value = "📊 Excel bar chart auto-generated"
        ws.cell(row=footer,column=1).font = Font(bold=True)


    # ===== PROPERTY SHEETS =====
    for name, hourly in results:

        ws = wb.create_sheet(name[:31])
        create_sheet(ws, hourly)

        cash_total = qr_total = online_total = total_total = 0

        for h in range(24):

            consolidated[h]["cash"] += hourly[h]["cash"]
            consolidated[h]["qr"] += hourly[h]["qr"]
            consolidated[h]["online"] += hourly[h]["online"]
            consolidated[h]["total"] += hourly[h]["total"]

            cash_total += hourly[h]["cash"]
            qr_total += hourly[h]["qr"]
            online_total += hourly[h]["online"]
            total_total += hourly[h]["total"]

        property_totals.append({
            "name": name,
            "cash": cash_total,
            "qr": qr_total,
            "online": online_total,
            "total": total_total
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


    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    await send_telegram_excel_buffer(
        buffer,
        filename=f"Collection_{display_date}.xlsx",
        caption="📊 Hourly Collection Report"
    )


# ================= RUN =================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
