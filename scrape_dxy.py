import os
import re
import json
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

CSV_PATH = "data/dxy_history.csv"
DEBUG_DIR = "debug"

URLS = [
    "https://www.investing.com/currencies/us-dollar-index",
    "https://m.investing.com/currencies/us-dollar-index",
]

USER_AGENTS = [
    # desktop
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # mobile
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]

def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)

def parse_float_safe(s: str) -> Optional[float]:
    if s is None:
        return None
    s = s.strip().replace(",", "")
    # lấy số đầu tiên dạng 97.40
    m = re.search(r"\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except:
        return None

def now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ---------- fallback synthetic ----------
def get_dxy_synthetic_from_fx() -> float:
    """
    DXY synthetic:
    50.14348112 * (EURUSD^-0.576) * (USDJPY^0.136) * (GBPUSD^-0.119)
                * (USDCAD^0.091) * (USDSEK^0.042) * (USDCHF^0.036)
    """
    url = "https://open.er-api.com/v6/latest/USD"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    rates = data.get("rates", {})

    needed = ["EUR", "JPY", "GBP", "CAD", "SEK", "CHF"]
    for c in needed:
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

# ---------- Investing scrape ----------
def try_extract_from_html(html: str) -> Optional[float]:
    patterns = [
        r'data-test="instrument-price-last"[^>]*>\s*(?P<v>\d+(?:\.\d+)?)\s*<',
        r'"last_price"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
        r'"last"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
        r'"pid"\s*:\s*8827.*?"last"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
        r'"instrumentId"\s*:\s*8827.*?"last"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
    ]
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE | re.DOTALL)
        if m:
            val = parse_float_safe(m.group("v"))
            if val is not None:
                return round(val, 4)
    return None

def save_debug(page, tag: str):
    try:
        html = page.content()
        with open(os.path.join(DEBUG_DIR, f"last_page_{tag}.html"), "w", encoding="utf-8") as f:
            f.write(html)
        page.screenshot(path=os.path.join(DEBUG_DIR, f"last_screenshot_{tag}.png"), full_page=True)
    except:
        pass

def scrape_investing_once(url: str, ua: str, viewport: dict) -> float:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-gpu",
            ],
        )

        context = browser.new_context(
            user_agent=ua,
            locale="en-US",
            viewport=viewport,
            java_script_enabled=True,
            ignore_https_errors=True,
            timezone_id="UTC",
        )

        # stealth nhẹ
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
            window.chrome = { runtime: {} };
        """)

        page = context.new_page()

        # block resource nặng, nhưng giữ XHR/fetch/script/css/doc
        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "media", "font"]
            else route.continue_(),
        )

        # capture JSON/XHR có khả năng chứa giá
        captured_prices = []

        def on_response(resp):
            try:
                ctype = (resp.headers or {}).get("content-type", "")
                u = resp.url.lower()
                if "json" in ctype or any(k in u for k in ["quotes", "chart", "stream", "api", "instrument"]):
                    txt = resp.text()
                    # thử parse json trước
                    try:
                        obj = json.loads(txt)
                        txt2 = json.dumps(obj)
                    except:
                        txt2 = txt
                    v = try_extract_from_html(txt2)
                    if v is not None:
                        captured_prices.append(v)
            except:
                pass

        page.on("response", on_response)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
        except Exception as e:
            save_debug(page, "goto_fail")
            browser.close()
            raise

        # đợi network thêm chút
        page.wait_for_timeout(7000)

        # 1) ưu tiên giá bắt từ response json/xhr
        if captured_prices:
            browser.close()
            return round(captured_prices[-1], 4)

        # 2) parse DOM selectors
        selectors = [
            '[data-test="instrument-price-last"]',
            'span[data-test="instrument-price-last"]',
            'div[data-test="instrument-price-last"]',
            'span[class*="instrument-price_last"]',
            'span[class*="text-5xl"]',
            'span[class*="pid-8827-last"]',
        ]
        for sel in selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0:
                    txt = loc.inner_text(timeout=3000)
                    val = parse_float_safe(txt)
                    if val is not None:
                        browser.close()
                        return round(val, 4)
            except:
                pass

        # 3) parse HTML raw
        html = page.content()
        v = try_extract_from_html(html)
        if v is not None:
            browser.close()
            return v

        # fail -> lưu debug
        save_debug(page, "parse_fail")
        browser.close()
        raise RuntimeError("Không parse được DXY từ Investing trong lần thử này")

def scrape_investing_with_retry(max_rounds=4) -> Tuple[float, str]:
    last_err = ""
    # luân phiên cấu hình desktop/mobile
    profiles = [
        (USER_AGENTS[0], {"width": 1366, "height": 768}),
        (USER_AGENTS[1], {"width": 412, "height": 915}),
    ]

    for r in range(1, max_rounds + 1):
        for url in URLS:
            for ua, vp in profiles:
                try:
                    print(f"[TRY] round={r} url={url} vp={vp['width']}x{vp['height']}")
                    val = scrape_investing_once(url, ua, vp)
                    return val, "investing_playwright"
                except Exception as e:
                    last_err = f"{url} | {vp} -> {e}"
                    print("[FAIL]", last_err)
        sleep_s = 4 * r
        print(f"[WAIT] {sleep_s}s")
        time.sleep(sleep_s)

    raise RuntimeError(last_err)

# ---------- CSV ----------
def append_csv(price: float, source: str):
    ensure_dirs()
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

    # dedup 60s
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
    ensure_dirs()
    try:
        price, source = scrape_investing_with_retry(max_rounds=4)
    except Exception as e:
        print("[WARN] Investing failed -> fallback synthetic:", e)
        price = get_dxy_synthetic_from_fx()
        source = "synthetic_fx_fallback"

    append_csv(price, source)
