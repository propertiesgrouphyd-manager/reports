
# ==============================
# ULTRA FAST ASYNC MULTI PROPERTY AUTOMATION
# DATE-WISE COLLECTION BASED ON PAYMENT CREATED_AT
# FINAL EXCEL: ONLY PAID BOOKINGS (NO PER DAY STAY CALC)
# ==============================

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import traceback
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


from telegram_config import BOT_TOKEN, get_chat_id

TELEGRAM_BOT_TOKEN = BOT_TOKEN
TELEGRAM_CHAT_ID = get_chat_id("collection")


async def send_telegram_message(text, session):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }

    try:
        async with session.post(url, json=payload, timeout=30) as resp:
            response_text = await resp.text()

            if resp.status != 200:
                print("❌ TELEGRAM ERROR:", resp.status, response_text)
                return

            print("✅ Telegram sent")

    except Exception as e:
        print("❌ TELEGRAM EXCEPTION:", e)

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

                    # 🔥 Skip zero payments
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

                    # ================= CLASSIFICATION =================
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

        detail_semaphore = asyncio.Semaphore(DETAIL_PARALLEL_LIMIT)
        detail_cache = {}

        async def limited_detail_call(booking_no):
            async with detail_semaphore:
                if booking_no in detail_cache:
                    return detail_cache[booking_no]
                res = await fetch_booking_details(session, P, booking_no)
                detail_cache[booking_no] = res
                return res

        booking_date_mode_map = {}

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

                    if (d, booking_no) not in booking_date_mode_map:
                        booking_date_mode_map[(d, booking_no)] = {
                            "cash": 0.0,
                            "qr": 0.0,
                            "online": 0.0,
                            "discount": 0.0,
                            "b": b,
                            "source": source
                        }

                    mode = ev.get("mode")
                    amt = float(ev.get("amt", 0) or 0)

                    if mode == "cash":
                        booking_date_mode_map[(d, booking_no)]["cash"] += amt
                    elif mode == "qr":
                        booking_date_mode_map[(d, booking_no)]["qr"] += amt
                    elif mode == "discount":
                        booking_date_mode_map[(d, booking_no)]["discount"] += amt
                    else:
                        booking_date_mode_map[(d, booking_no)]["online"] += amt

            if len(data.get("bookingIds", [])) < 100:
                break
            offset += 100

        # ================= BUILD DATAFRAME =================
        all_rows = []

        for (d, booking_no), vals in booking_date_mode_map.items():
            b = vals["b"]

            cash = vals["cash"]
            qr = vals["qr"]
            online = vals["online"]
            discount = vals["discount"]

            total_paid = cash + qr + online + discount

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
                "Total Paid": round(total_paid, 2)
            })

        df = pd.DataFrame(all_rows)

        if df.empty:
            df = pd.DataFrame(columns=[
                "Date", "Booking Id", "Guest Name", "Status",
                "Booking Source", "Check In", "Check Out",
                "Cash", "QR", "Online", "Discount", "Total Paid"
            ])

        df = df.sort_values(["Date", "Booking Id"], ascending=True)

        return (P["name"], df)

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



def build_daily_collection_message(
    prop,
    report_date,
    total_guests,
    total_amount,
    cash,
    qr,
    online,
    discount
):
    return f"""
<pre>
DAILY COLLECTION REPORT : {prop}

🏢 Property Code     : {prop}
📅 Date              : {report_date}

🔹 Total Guests Paid : {total_guests}

🔹 Total Amount      : ₹{total_amount:,.2f}
🔹 Cash              : ₹{cash:,.2f}
🔹 QR                : ₹{qr:,.2f}
🔹 Online            : ₹{online:,.2f}
🔹 Discount          : ₹{discount:,.2f}

</pre>
""".strip()
# ================= MAIN =================
async def main():
    print("========================================")
    print(" OYO DAILY COLLECTION TELEGRAM AUTOMATION")
    print("========================================")

    global now
    now = datetime.now(IST)

    target_date = now.date()


    TF = target_date.strftime("%Y-%m-%d")
    TT = TF

    HF = (target_date - timedelta(days=60)).strftime("%Y-%m-%d")
    HT = target_date.strftime("%Y-%m-%d")

    print("BUSINESS DATE :", TF)

    pending = {k: v for k, v in PROPERTIES.items()}
    success_results = {}

    # ================= RETRY ENGINE =================
    for run_attempt in range(1, MAX_FULL_RUN_RETRIES + 1):
        if not pending:
            break

        print(f"\n🔁 PARTIAL RUN {run_attempt}/{MAX_FULL_RUN_RETRIES}")
        tasks = [run_property_limited(P, TF, TT, HF, HT) for P in pending.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        new_pending = {}
        for key, (P, result) in zip(list(pending.keys()), zip(pending.values(), results)):
            if isinstance(result, Exception):
                print(f"❌ FAILED → {P['name']} :: {result}")
                new_pending[key] = P
                continue

            success_results[key] = result
            print(f"✅ OK → {P['name']}")

        pending = new_pending

        if pending:
            if run_attempt == MAX_FULL_RUN_RETRIES:
                raise RuntimeError(
                    f"FINAL FAILURE: {[p['name'] for p in pending.values()]}"
                )
            await asyncio.sleep(FULL_RUN_RETRY_DELAY)

    valid_results = [success_results[k] for k in PROPERTIES.keys() if k in success_results]

    if len(valid_results) != len(PROPERTIES):
        raise RuntimeError("DATA INCOMPLETE")

    # ================= TELEGRAM SEND =================
    async with aiohttp.ClientSession() as tg_session:

        consolidated_cash = 0
        consolidated_qr = 0
        consolidated_online = 0
        consolidated_discount = 0
        consolidated_guests = 0

        for name, df in valid_results:

            total_guests = df["Booking Id"].nunique() if not df.empty else 0

            cash = round(df["Cash"].sum(), 2) if not df.empty else 0
            qr = round(df["QR"].sum(), 2) if not df.empty else 0
            online = round(df["Online"].sum(), 2) if not df.empty else 0
            discount = round(df["Discount"].sum(), 2) if not df.empty else 0

            total_amount = cash + qr + online + discount

            msg = build_daily_collection_message(
                prop=name,
                report_date=datetime.strptime(TT, "%Y-%m-%d").strftime("%d/%m/%Y"),
                total_guests=total_guests,
                total_amount=total_amount,
                cash=cash,
                qr=qr,
                online=online,
                discount=discount
            )

            await send_telegram_message(msg, tg_session)
            await asyncio.sleep(1.2)

            consolidated_cash += cash
            consolidated_qr += qr
            consolidated_online += online
            consolidated_discount += discount
            consolidated_guests += total_guests

        # ================= CONSOLIDATED REPORT =================
        consolidated_total = (
            consolidated_cash +
            consolidated_qr +
            consolidated_online +
            consolidated_discount
        )

        consolidated_msg = build_daily_collection_message(
            prop="ALL PROPERTIES",
            report_date=datetime.strptime(TT, "%Y-%m-%d").strftime("%d/%m/%Y"),
            total_guests=consolidated_guests,
            total_amount=consolidated_total,
            cash=consolidated_cash,
            qr=consolidated_qr,
            online=consolidated_online,
            discount=consolidated_discount
        )

        await send_telegram_message(consolidated_msg, tg_session)

    print("✅ ALL PROPERTY REPORTS SENT")
# ================= RUN =================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("SCRIPT CRASHED")
        print(e)
        traceback.print_exc()
        print("SCRIPT CRASHED", e, flush=True)
