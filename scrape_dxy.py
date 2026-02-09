import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from playwright.sync_api import sync_playwright

CSV_PATH = "data/dxy_history.csv"
URLS = [
    "https://www.investing.com/currencies/us-dollar-index",
    "https://m.investing.com/currencies/us-dollar-index",
]

def parse_float_safe(s: str):
    if s is None:
        return None
    s = s.strip().replace(",", "")
    try:
        return float(s)
    except:
        return None

def now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def get_dxy_synthetic_from_fx():
    """
    Fallback khi Investing parse fail.
    """
    url = "https://open.er-api.com/v6/latest/USD"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    rates = data.get("rates", {})

    for c in ["EUR", "JPY", "GBP", "CAD", "SEK", "CHF"]:
        if c not in rates or rates[c] in (None, 0):
            raise RuntimeError(f"Missing USD/{c}")

    usd_eur = float(rates["EUR"])
    usd_jpy = float(rates["JPY"])
    usd_gbp = float(rates["GBP"])
    usd_cad = float(rates["CAD"])
    usd_sek = float(rates["SEK"])
    usd_chf = float(rates["CHF"])

    eur_usd = 1.0 / usd_eur
    gbp_usd = 1.0 / usd_gbp

    dxy = (
        50.14348112
        * (eur_usd ** -0.576)
        * (usd_jpy ** 0.136)
        * (gbp_usd ** -0.119)
        * (usd_cad ** 0.091)
        * (usd_sek ** 0.042)
        * (usd_chf ** 0.036)
    )
    return round(dxy, 4)

def scrape_investing_once(url: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            viewport={"width": 1366, "height": 768},
            java_script_enabled=True,
            ignore_https_errors=True,
        )
        page = context.new_page()

        # giảm tài nguyên nặng
        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "font", "media"]
            else route.continue_(),
        )

        page.goto(url, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(7000)

        # selectors best-effort
        selectors = [
            '[data-test="instrument-price-last"]',
            'span[data-test="instrument-price-last"]',
            'div[data-test="instrument-price-last"]',
            'span[class*="text-5xl"]',
            'span[class*="pid-8827-last"]',  # có thể xuất hiện theo instrument class
        ]

        price = None
        for sel in selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0:
                    txt = loc.inner_text(timeout=2500)
                    val = parse_float_safe(txt)
                    if val is not None:
                        price = val
                        break
            except:
                pass

        # regex fallback từ HTML
        if price is None:
            html = page.content()
            patterns = [
                r'"last"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
                r'"last_price"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
                r'data-test="instrument-price-last"[^>]*>\s*(?P<v>\d+(?:\.\d+)?)\s*<',
                r'"instrumentId"\s*:\s*8827.*?"last"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
            ]
            for pat in patterns:
                m = re.search(pat, html, flags=re.IGNORECASE | re.DOTALL)
                if m:
                    val = parse_float_safe(m.group("v"))
                    if val is not None:
                        price = val
                        break

        browser.close()

        if price is None:
            raise RuntimeError("Không parse được DXY từ Investing")
        return round(price, 4)

def scrape_investing_with_retry(max_rounds=3):
    last_err = ""
    for i in range(1, max_rounds + 1):
        for u in URLS:
            try:
                print(f"[TRY] round={i} url={u}")
                return scrape_investing_once(u), "investing_playwright"
            except Exception as e:
                last_err = f"{u}: {e}"
                print(f"[FAIL] {last_err}")
        time.sleep(i * 5)
    raise RuntimeError(last_err)

def append_csv(price: float, source: str):
    os.makedirs("data", exist_ok=True)

    row = {
        "datetime_utc": now_utc_str(),
        "dxy_index": price,
        "source": source,
        "dxy_change_pct": None,
    }

    if os.path.exists(CSV_PATH):
        df = pd.read_csv(CSV_PATH)
    else:
        df = pd.DataFrame(columns=["datetime_utc", "dxy_index", "source", "dxy_change_pct"])

    # dedup 60 giây
    if len(df) > 0:
        try:
            last_dt = pd.to_datetime(df.iloc[-1]["datetime_utc"], utc=True)
            now_dt = datetime.now(timezone.utc)
            if abs((now_dt - last_dt.to_pydatetime()).total_seconds()) < 60:
                print("SKIP duplicate < 60s")
                return
        except:
            pass

    ref = 100.0
    if len(df) > 0:
        try:
            prev = float(df.iloc[-1]["dxy_index"])
            if prev > 0:
                ref = prev
        except:
            pass

    row["dxy_change_pct"] = round(((price - ref) / ref) * 100.0, 6)
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(CSV_PATH, index=False)
    print("APPENDED:", row)

if __name__ == "__main__":
    try:
        price, source = scrape_investing_with_retry(max_rounds=3)
    except Exception as e:
        print("[WARN] Investing failed, fallback synthetic:", e)
        price = get_dxy_synthetic_from_fx()
        source = "synthetic_fx_fallback"

    append_csv(price, source)
