"""
NSE Bhav Copy Fetcher
Runs daily via GitHub Actions
Fetches NSE Bhav Copy and writes to Google Sheets
"""

import os
import io
import json
import time
import zipfile
import requests
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ─── CONFIG ──────────────────────────────────────────────────
SHEET_ID       = os.environ.get("SHEET_ID")
CREDENTIALS    = os.environ.get("GOOGLE_CREDENTIALS")
RAW_SHEET_NAME = "BhavCopy_Raw"
LOG_SHEET_NAME = "Fetch_Log"
IST_OFFSET     = timedelta(hours=5, minutes=30)

# ─── NSE HEADERS (browser simulation) ────────────────────────
NSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

# ─── GOOGLE SHEETS SETUP ─────────────────────────────────────
def get_gspread_client():
    creds_dict = json.loads(CREDENTIALS)
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client

def get_or_create_sheet(spreadsheet, name, headers):
    try:
        sheet = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=name, rows=1000, cols=len(headers))
        sheet.append_row(headers)
        print(f"  Created sheet: {name}")
    return sheet

# ─── NSE SESSION ─────────────────────────────────────────────
def get_nse_session():
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    print("  Warming up NSE session...")
    try:
        session.get("https://www.nseindia.com/", timeout=15)
        time.sleep(2)
        session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=15)
        time.sleep(1)
    except Exception as e:
        print(f"  Warning: Session warmup issue: {e}")
    return session

# ─── BUILD NSE URL ────────────────────────────────────────────
def build_url(date):
    y = date.strftime("%Y")
    m = date.strftime("%m")
    d = date.strftime("%d")
    return (
        f"https://nsearchives.nseindia.com/content/cm/"
        f"BhavCopy_NSE_CM_0_0_0_{y}{m}{d}_F_0000.csv.zip"
    )

# ─── FETCH ONE DAY ───────────────────────────────────────────
def fetch_bhav(session, date):
    url = build_url(date)
    date_str = date.strftime("%Y-%m-%d")
    print(f"  Fetching: {url}")

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            print(f"  {date_str}: 404 — likely holiday or future date")
            return None, "HOLIDAY_OR_404"
        if resp.status_code != 200:
            print(f"  {date_str}: HTTP {resp.status_code}")
            return None, f"HTTP_{resp.status_code}"

        # Unzip
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_name = z.namelist()[0]
        csv_text = z.read(csv_name).decode("utf-8")
        lines    = csv_text.strip().split("\n")

        if len(lines) < 2:
            return None, "EMPTY_CSV"

        # Parse header
        header = [h.strip().upper() for h in lines[0].split(",")]
        def ci(name):
            return header.index(name) if name in header else -1

        idx = {
            "symbol":    ci("SYMBOL"),
            "series":    ci("SERIES"),
            "open":      ci("OPEN"),
            "high":      ci("HIGH"),
            "low":       ci("LOW"),
            "close":     ci("CLOSE"),
            "last":      ci("LAST"),
            "prevclose": ci("PREVCLOSE"),
            "qty":       ci("TOTTRDQTY"),
            "val":       ci("TOTTRDVAL"),
            "trades":    ci("TOTALTRADES"),
            "isin":      ci("ISIN"),
            "delqty":    ci("DELIV_QTY"),
            "delperc":   ci("DELIV_PERC"),
        }

        rows = []
        for line in lines[1:]:
            cols = [c.strip() for c in line.split(",")]
            if len(cols) < 5:
                continue
            series = cols[idx["series"]].strip() if idx["series"] >= 0 else ""
            if series != "EQ":
                continue

            def val(key, cast=str):
                try:
                    return cast(cols[idx[key]]) if idx[key] >= 0 and cols[idx[key]] else ""
                except:
                    return ""

            rows.append([
                date_str,
                val("symbol"),
                val("series"),
                val("open",      float),
                val("high",      float),
                val("low",       float),
                val("close",     float),
                val("last",      float),
                val("prevclose", float),
                val("qty",       int),
                val("val",       float),
                val("trades",    int),
                val("isin"),
                val("delqty",    int),
                val("delperc",   float),
            ])

        print(f"  {date_str}: {len(rows)} EQ stocks parsed")
        return rows, "SUCCESS"

    except Exception as e:
        print(f"  {date_str}: ERROR — {e}")
        return None, str(e)

# ─── CHECK IF DATE EXISTS IN SHEET ───────────────────────────
def date_exists(sheet, date_str):
    try:
        col_a = sheet.col_values(1)
        return date_str in col_a
    except:
        return False

# ─── MAIN ────────────────────────────────────────────────────
def main():
    ist_now  = datetime.utcnow() + IST_OFFSET
    print(f"\n{'='*55}")
    print(f"  NSE Bhav Copy Fetcher")
    print(f"  IST Time: {ist_now.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}\n")

    # Connect to Google Sheets
    print("[1/4] Connecting to Google Sheets...")
    client        = get_gspread_client()
    spreadsheet   = client.open_by_key(SHEET_ID)

    RAW_HEADERS = [
        "DATE","SYMBOL","SERIES","OPEN","HIGH","LOW","CLOSE","LAST",
        "PREVCLOSE","TOTTRDQTY","TOTTRDVAL","TOTALTRADES","ISIN",
        "DELIV_QTY","DELIV_PERC"
    ]
    LOG_HEADERS = ["TIMESTAMP","DATE_FETCHED","ROWS_ADDED","STATUS","NOTES"]

    raw_sheet = get_or_create_sheet(spreadsheet, RAW_SHEET_NAME, RAW_HEADERS)
    log_sheet = get_or_create_sheet(spreadsheet, LOG_SHEET_NAME, LOG_HEADERS)
    print("  Connected.")

    # Determine dates to fetch — last 10 trading days
    print("\n[2/4] Determining dates to fetch...")
    dates_to_fetch = []
    check_date = ist_now.date()
    days_back  = 0

    while len(dates_to_fetch) < 10 and days_back < 30:
        d = check_date - timedelta(days=days_back)
        days_back += 1
        if d.weekday() >= 5:  # skip weekends
            continue
        d_str = d.strftime("%Y-%m-%d")
        if date_exists(raw_sheet, d_str):
            print(f"  {d_str}: already fetched, skipping")
            continue
        dates_to_fetch.append(d)

    if not dates_to_fetch:
        print("  All recent dates already fetched!")
        log_sheet.append_row([
            ist_now.strftime("%Y-%m-%d %H:%M:%S"), "---", 0, "SKIPPED", "All dates already present"
        ])
        return

    print(f"  Will fetch: {[str(d) for d in dates_to_fetch]}")

    # Start NSE session
    print("\n[3/4] Starting NSE session...")
    session = get_nse_session()

    # Fetch each date
    print("\n[4/4] Fetching data...")
    total_rows = 0

    for date in sorted(dates_to_fetch):
        date_str = date.strftime("%Y-%m-%d")
        rows, status = fetch_bhav(session, date)

        ts = ist_now.strftime("%Y-%m-%d %H:%M:%S")

        if rows and len(rows) > 0:
            # Write to sheet in one batch
            print(f"  Writing {len(rows)} rows to Google Sheets...")
            raw_sheet.append_rows(rows, value_input_option="RAW")
            total_rows += len(rows)
            log_sheet.append_row([ts, date_str, len(rows), "SUCCESS", f"{len(rows)} EQ stocks"])
            print(f"  ✅ {date_str}: done")
        else:
            log_sheet.append_row([ts, date_str, 0, status, "No data written"])
            print(f"  ⚠️  {date_str}: {status}")

        time.sleep(2)  # polite delay between requests

    print(f"\n{'='*55}")
    print(f"  ✅ Complete! {total_rows} total rows written.")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()
