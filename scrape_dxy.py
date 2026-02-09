import os
import re
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd
import requests
from playwright.sync_api import sync_playwright

CSV_PATH = "data/dxy_history.csv"
DEBUG_DIR = "debug"

# CNBC ICE U.S. Dollar Index page
URLS = ["https://www.cnbc.com/quotes/.DXY"]

USER_AGENTS = [
    # desktop
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # mobile
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]


# -------------------------
# Helpers
# -------------------------
def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)


def parse_float_safe(s: str) -> Optional[float]:
    if s is None:
        return None
    s = s.strip().replace(",", "")
    m = re.search(r"\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def save_debug(page, tag: str):
    try:
        html = page.content()
        with open(os.path.join(DEBUG_DIR, f"last_page_{tag}.html"), "w", encoding="utf-8") as f:
            f.write(html)
        page.screenshot(path=os.path.join(DEBUG_DIR, f"last_screenshot_{tag}.png"), full_page=True)
    except Exception:
        pass


# -------------------------
# Fallback synthetic DXY
# -------------------------
def get_dxy_synthetic_from_fx() -> float:
    """
    Synthetic DXY:
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


# -------------------------
# Strict extractors (avoid wrong 99.99)
# -------------------------
def extract_dxy_from_json_text_strict(text: str) -> Optional[float]:
    """
    Only accept values near .DXY context.
    """
    patterns = [
        r'"symbol"\s*:\s*"\.DXY".{0,600}?"last(?:Price)?"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
        r'"last(?:Price)?"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?.{0,600}?"symbol"\s*:\s*"\.DXY"',
        r'"ticker"\s*:\s*"\.DXY".{0,600}?"last(?:Price)?"\s*:\s*"?(?P<v>\d+(?:\.\d+)?)"?',
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            v = parse_float_safe(m.group("v"))
            if v is not None:
                return round(v, 4)
    return None


def extract_dxy_from_html_strict(html: str) -> Optional[float]:
    """
    Parse near known DXY labels in HTML.
    """
    ctx_patterns = [
        r'ICE U\.S\. Dollar Index.{0,900}(?P<v>\d{2,3}\.\d{2,4})',
        r'\.DXY:Exchange.{0,900}(?P<v>\d{2,3}\.\d{2,4})',
        r'Last.{0,160}(?P<v>\d{2,3}\.\d{2,4})',
    ]
    for pat in ctx_patterns:
        m = re.search(pat, html, flags=re.IGNORECASE | re.DOTALL)
        if m:
            v = parse_float_safe(m.group("v"))
            if v is not None:
                return round(v, 4)
    return None


# -------------------------
# CNBC scrape
# -------------------------
def scrape_cnbc_once(url: str, user_agent: str, viewport: dict) -> float:
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
            user_agent=user_agent,
            locale="en-US",
            viewport=viewport,
            java_script_enabled=True,
            ignore_https_errors=True,
            timezone_id="UTC",
        )

        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
            window.chrome = { runtime: {} };
        """)

        page = context.new_page()

        # block heavy resources
        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "media", "font"]
            else route.continue_(),
        )

        captured_prices = []

        def on_response(resp):
            try:
                ctype = (resp.headers or {}).get("content-type", "")
                u = resp.url.lower()
                if ("json" in ctype) or any(k in u for k in ["quote", "quotes", "chart", "graphql", "api"]):
                    txt = resp.text()
                    v = extract_dxy_from_json_text_strict(txt)
                    if v is not None:
                        captured_prices.append(v)
            except Exception:
                pass

        page.on("response", on_response)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
        except Exception:
            save_debug(page, "cnbc_goto_fail")
            browser.close()
            raise

        page.wait_for_timeout(7000)

        # 1) strict DOM selectors around quote strip "Last"
        strict_selectors = [
            '[data-testid="QuoteStrip-lastPrice"]',
            '[data-testid*="QuoteStrip-lastPrice"]',
            '[class*="QuoteStrip-lastPrice"]',
            'main [class*="QuoteStrip"] [class*="lastPrice"]',
        ]

        for sel in strict_selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0:
                    txt = loc.inner_text(timeout=3000)
                    v = parse_float_safe(txt)
                    if v is not None and 90 <= v <= 110:
                        browser.close()
                        return round(v, 4)
            except Exception:
                pass

        # 2) captured JSON/XHR with strict .DXY context
        for v in reversed(captured_prices):
            if v is not None and 90 <= v <= 110:
                browser.close()
                return round(v, 4)

        # 3) strict HTML fallback
        html = page.content()
        v = extract_dxy_from_html_strict(html)
        if v is not None and 90 <= v <= 110:
            browser.close()
            return round(v, 4)

        save_debug(page, "cnbc_parse_fail")
        browser.close()
        raise RuntimeError("Không parse được Last price .DXY từ CNBC")


def scrape_cnbc_with_retry(max_rounds=4) -> Tuple[float, str]:
    last_err = ""
    profiles = [
        (USER_AGENTS[0], {"width": 1366, "height": 768}),
        (USER_AGENTS[1], {"width": 412, "height": 915}),
    ]

    for r in range(1, max_rounds + 1):
        for url in URLS:
            for ua, vp in profiles:
                try:
                    print(f"[TRY] round={r} url={url} vp={vp['width']}x{vp['height']}")
                    val = scrape_cnbc_once(url, ua, vp)
                    print(f"[OK] cnbc price={val}")
                    return val, "cnbc_playwright"
                except Exception as e:
                    last_err = f"{url} | {vp} -> {e}"
                    print(f"[FAIL] {last_err}")

        sleep_s = 4 * r
        print(f"[WAIT] {sleep_s}s")
        time.sleep(sleep_s)

    raise RuntimeError(last_err)


# -------------------------
# CSV append (safe + outlier guard)
# -------------------------
def append_csv(price: float, source: str):
    ensure_dirs()

    columns = ["datetime_utc", "dxy_index", "source", "dxy_change_pct"]
    row = {
        "datetime_utc": now_utc_str(),
        "dxy_index": float(price),
        "source": source,
        "dxy_change_pct": None,
    }

    # read safely
    if os.path.exists(CSV_PATH):
        try:
            if os.path.getsize(CSV_PATH) == 0:
                df = pd.DataFrame(columns=columns)
            else:
                df = pd.read_csv(CSV_PATH)
        except Exception as e:
            print(f"[WARN] CSV lỗi/rỗng -> reset dataframe. reason={e}")
            df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(columns=columns)

    # normalize columns
    for c in columns:
        if c not in df.columns:
            df[c] = None
    df = df[columns]

    # dedup 60s
    if len(df) > 0 and pd.notna(df.iloc[-1]["datetime_utc"]):
        try:
            last_dt = pd.to_datetime(df.iloc[-1]["datetime_utc"], utc=True)
            now_dt = datetime.now(timezone.utc)
            if abs((now_dt - last_dt.to_pydatetime()).total_seconds()) < 60:
                print("SKIP duplicate < 60s")
                return
        except Exception as e:
            print(f"[WARN] parse last datetime fail: {e}")

    # outlier guard (30m jump too large)
    if len(df) > 0:
        try:
            prev = float(df.iloc[-1]["dxy_index"])
            if abs(row["dxy_index"] - prev) > 3.0:
                print(f"[WARN] Outlier detected prev={prev}, new={row['dxy_index']} -> skip")
                return
        except Exception:
            pass

    # reference for change pct
    ref = 97.324
    if len(df) > 0:
        try:
            prev = float(df.iloc[-1]["dxy_index"])
            if prev > 0:
                ref = prev
        except Exception:
            pass

    row["dxy_change_pct"] = round(((row["dxy_index"] - ref) / ref) * 100.0, 6)

    # append + sort
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    try:
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        df = df.sort_values("datetime_utc").reset_index(drop=True)
        df["datetime_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    # atomic write
    tmp = CSV_PATH + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, CSV_PATH)

    print("APPENDED:", row)


# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    ensure_dirs()

    if not os.path.exists(CSV_PATH):
        pd.DataFrame(
            columns=["datetime_utc", "dxy_index", "source", "dxy_change_pct"]
        ).to_csv(CSV_PATH, index=False)

    try:
        price, source = scrape_cnbc_with_retry(max_rounds=4)
    except Exception as e:
        print("[WARN] CNBC failed -> fallback synthetic:", e)
        price = get_dxy_synthetic_from_fx()
        source = "synthetic_fx_fallback"

    append_csv(price, source)
