import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
from playwright.sync_api import sync_playwright

CSV_PATH = "data/dxy_history.csv"
URL = "https://www.investing.com/currencies/us-dollar-index"

def parse_float_safe(s: str):
    if s is None:
        return None
    s = s.strip()
    # bỏ dấu phẩy ngàn nếu có
    s = s.replace(",", "")
    try:
        return float(s)
    except:
        return None

def scrape_dxy():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = context.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=90000)

        # chờ render
        page.wait_for_timeout(5000)

        price = None

        # 1) selector thường gặp của Investing (có thể thay đổi theo thời gian)
        candidate_selectors = [
            '[data-test="instrument-price-last"]',
            'span[data-test="instrument-price-last"]',
            'div[data-test="instrument-price-last"]',
            'span[class*="text-5xl"]',   # fallback UI class mới
        ]

        for sel in candidate_selectors:
            try:
                el = page.locator(sel).first
                if el.count() > 0:
                    txt = el.inner_text(timeout=3000)
                    val = parse_float_safe(txt)
                    if val is not None:
                        price = val
                        break
            except:
                pass

        # 2) fallback: regex từ HTML
        if price is None:
            html = page.content()

            patterns = [
                r'"last"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
                r'"last_price"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
                r'data-test="instrument-price-last"[^>]*>\s*(?P<v>\d+(?:\.\d+)?)\s*<',
            ]
            for pat in patterns:
                m = re.search(pat, html, flags=re.IGNORECASE)
                if m:
                    val = parse_float_safe(m.group("v"))
                    if val is not None:
                        price = val
                        break

        browser.close()

        if price is None:
            raise RuntimeError("Không parse được DXY từ Investing.")

        return round(price, 4)

def append_csv(new_price: float):
    os.makedirs("data", exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.strftime("%Y-%m-%d %H:%M:%S")

    new_row = {
        "datetime_utc": now_iso,
        "dxy_index": new_price,
        "source": "investing_playwright",
    }

    if os.path.exists(CSV_PATH):
        df = pd.read_csv(CSV_PATH)
    else:
        df = pd.DataFrame(columns=["datetime_utc", "dxy_index", "source", "dxy_change_pct"])

    # dedup 60 giây
    if len(df) > 0:
        try:
            last_dt = pd.to_datetime(df.iloc[-1]["datetime_utc"], utc=True)
            diff_sec = abs((now_utc - last_dt.to_pydatetime()).total_seconds())
            if diff_sec < 60:
                print("SKIP: duplicate within 60s")
                return
        except:
            pass

    # reference rate: dòng trước hoặc baseline 100
    ref = 100.0
    if len(df) > 0:
        try:
            prev = float(df.iloc[-1]["dxy_index"])
            if prev > 0:
                ref = prev
        except:
            pass

    change_pct = ((new_price - ref) / ref) * 100.0
    new_row["dxy_change_pct"] = round(change_pct, 6)

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.to_csv(CSV_PATH, index=False)
    print(f"OK append: {new_row}")

if __name__ == "__main__":
    price = scrape_dxy()
    append_csv(price)
