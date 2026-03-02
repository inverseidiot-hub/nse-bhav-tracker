"""
NSE Bhav Copy Fetcher — with auto column detection
Runs daily via GitHub Actions
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

SHEET_ID       = os.environ.get("SHEET_ID")
CREDENTIALS    = os.environ.get("GOOGLE_CREDENTIALS")
RAW_SHEET_NAME = "BhavCopy_Raw"
LOG_SHEET_NAME = "Fetch_Log"
IST_OFFSET     = timedelta(hours=5, minutes=30)

NSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

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

def get_nse_session():
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    print("  Warming up NSE session...")
    try:
        r = session.get("https://www.nseindia.com/", timeout=15)
        print(f"  NSE homepage: HTTP {r.status_code}")
        time.sleep(2)
    except Exception as e:
        print(f"  Warning: {e}")
    return session

def build_urls(date):
    y         = date.strftime("%Y")
    m         = date.strftime("%m")
    d         = date.strftime("%d")
    mon_upper = date.strftime("%b").upper()
    return [
        f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{y}{m}{d}_F_0000.csv.zip",
        f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{y}/{mon_upper}/cm{d}{mon_upper}{y}bhav.csv.zip",
    ]

def find_col(header, candidates):
    for c in candidates:
        if c.upper() in header:
            return header.index(c.upper())
    return -1

def fetch_bhav(session, date):
    date_str = date.strftime("%Y-%m-%d")
    urls = build_urls(date)

    for url in urls:
        print(f"\n  Trying URL: {url}")
        try:
            resp = session.get(url, timeout=30)
            print(f"  HTTP {resp.status_code}, {len(resp.content)} bytes")

            if resp.status_code != 200 or len(resp.content) < 200:
                continue

            try:
                z = zipfile.ZipFile(io.BytesIO(resp.content))
            except Exception as e:
                print(f"  Not valid ZIP: {e}")
                continue

            fname    = z.namelist()[0]
            csv_text = z.read(fname).decode("utf-8", errors="replace")
            lines    = [l for l in csv_text.strip().split("\n") if l.strip()]
            print(f"  File: {fname}, Lines: {len(lines)}")

            if len(lines) < 2:
                continue

            # Parse and print headers
            raw_header = lines[0]
            print(f"  Raw header: {raw_header[:200]}")
            header = [h.strip().upper().replace('"','').replace('\r','') for h in raw_header.split(",")]
            print(f"  Columns ({len(header)}): {header}")

            # Auto detect columns
            idx = {
                "symbol":    find_col(header, ["SYMBOL","SCRIP_CD"]),
                "series":    find_col(header, ["SERIES"]),
                "open":      find_col(header, ["OPEN","OPEN_PRICE"]),
                "high":      find_col(header, ["HIGH","HIGH_PRICE"]),
                "low":       find_col(header, ["LOW","LOW_PRICE"]),
                "close":     find_col(header, ["CLOSE","CLOSE_PRICE","LAST_PRICE"]),
                "last":      find_col(header, ["LAST","LTP","LAST_PRICE"]),
                "prevclose": find_col(header, ["PREVCLOSE","PREV_CLOSE","PREVIOUSCLOSE","PREV_CLOSING_PRICE"]),
                "qty":       find_col(header, ["TOTTRDQTY","TTL_TRD_QNTY","TOTAL_TRADED_QUANTITY","TRDQNTY"]),
                "val":       find_col(header, ["TOTTRDVAL","TTLTRDDVAL","TOTAL_TRADED_VALUE","TRDVAL"]),
                "trades":    find_col(header, ["TOTALTRADES","NO_OF_TRADES","NOOFTRADES"]),
                "isin":      find_col(header, ["ISIN","ISIN_CODE"]),
                "delqty":    find_col(header, ["DELIV_QTY","DELIVERABLE_QTY","DELVQTY","DELIVERY_QTY"]),
                "delperc":   find_col(header, ["DELIV_PERC","DELIVERABLE_PERC","DELVPERC","DELIVERY_PERC","% DELVRD TO TRADED QTY"]),
            }
            print(f"  Column idx: {idx}")

            # Show sample data rows
            print("  First 2 data rows:")
            for line in lines[1:3]:
                print(f"    {line[:150]}")

            # Build rows
            rows = []
            for line in lines[1:]:
                cols = [c.strip().replace('"','').replace('\r','') for c in line.split(",")]
                if len(cols) < 5:
                    continue

                if idx["series"] >= 0:
                    if idx["series"] >= len(cols):
                        continue
                    if cols[idx["series"]].strip().upper() != "EQ":
                        continue
                    series = "EQ"
                else:
                    series = "EQ"

                def val(key, cast=str):
                    try:
                        i = idx[key]
                        if i < 0 or i >= len(cols):
                            return ""
                        v = cols[i].strip()
                        if not v or v in ["-","NA","N/A"]:
                            return ""
                        return cast(v)
                    except:
                        return ""

                rows.append([
                    date_str,
                    val("symbol"),
                    series,
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

            print(f"  Parsed {len(rows)} EQ rows")
            if len(rows) > 0:
                return rows, "SUCCESS"

        except Exception as e:
            print(f"  Exception: {e}")
            continue

    return None, "ALL_URLS_FAILED"

def date_exists(sheet, date_str):
    try:
        col_a = sheet.col_values(1)
        return date_str in col_a
    except:
        return False

def main():
    ist_now = datetime.utcnow() + IST_OFFSET
    print(f"\n{'='*55}")
    print(f"  NSE Bhav Copy Fetcher")
    print(f"  IST: {ist_now.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}\n")

    print("[1/4] Connecting to Google Sheets...")
    client      = get_gspread_client()
    spreadsheet = client.open_by_key(SHEET_ID)

    RAW_HEADERS = ["DATE","SYMBOL","SERIES","OPEN","HIGH","LOW","CLOSE","LAST",
                   "PREVCLOSE","TOTTRDQTY","TOTTRDVAL","TOTALTRADES","ISIN","DELIV_QTY","DELIV_PERC"]
    LOG_HEADERS = ["TIMESTAMP","DATE_FETCHED","ROWS_ADDED","STATUS","NOTES"]

    raw_sheet = get_or_create_sheet(spreadsheet, RAW_SHEET_NAME, RAW_HEADERS)
    log_sheet = get_or_create_sheet(spreadsheet, LOG_SHEET_NAME, LOG_HEADERS)
    print("  Connected.")

    print("\n[2/4] Finding dates to fetch...")
    dates_to_fetch = []
    check_date = ist_now.date()
    days_back  = 0
    while len(dates_to_fetch) < 10 and days_back < 30:
        d = check_date - timedelta(days=days_back)
        days_back += 1
        if d.weekday() >= 5:
            continue
        d_str = d.strftime("%Y-%m-%d")
        if date_exists(raw_sheet, d_str):
            print(f"  {d_str}: already fetched")
            continue
        dates_to_fetch.append(d)

    if not dates_to_fetch:
        print("  Nothing to fetch!")
        return

    print(f"  To fetch: {[str(d) for d in dates_to_fetch]}")

    print("\n[3/4] NSE session...")
    session = get_nse_session()

    print("\n[4/4] Fetching...")
    total_rows = 0

    for date in sorted(dates_to_fetch):
        date_str = date.strftime("%Y-%m-%d")
        rows, status = fetch_bhav(session, date)
        ts = ist_now.strftime("%Y-%m-%d %H:%M:%S")

        if rows and len(rows) > 0:
            print(f"  Writing {len(rows)} rows...")
            raw_sheet.append_rows(rows, value_input_option="RAW")
            total_rows += len(rows)
            log_sheet.append_row([ts, date_str, len(rows), "SUCCESS", f"{len(rows)} EQ stocks"])
            print(f"  ✅ {date_str} done")
        else:
            log_sheet.append_row([ts, date_str, 0, status, "0 rows"])
            print(f"  ⚠️  {date_str}: {status}")

        time.sleep(2)

    print(f"\n  Total rows written: {total_rows}")

if __name__ == "__main__":
    main()
