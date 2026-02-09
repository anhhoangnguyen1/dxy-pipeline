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

URLS = ["https://www.cnbc.com/quotes/.DXY"]

USER_AGENTS = [
    # desktop
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # mobile
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]

# sanity range for DXY
DXY_MIN = 90.0
DXY_MAX = 110.0

# maximum jump allowed between 30-min samples
MAX_DELTA_PER_RUN = 1.0


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


def in_dxy_range(v: Optional[float]) -> bool:
    return v is not None and DXY_MIN <= v <= DXY_MAX


def save_debug(page, tag: str):
    try:
        html = page.content()
        with open(os.path.join(DEBUG_DIR, f"last_page_{tag}.html"), "w", encoding="utf-8") as f:
            f.write(html)
        page.screenshot(path=os.path.join(DEBUG_DIR, f"last_screenshot_{tag}.png"), full_page=True)
    except Exception:
        pass


def extract_last_price_near_last_label(text: str) -> Optional[float]:
    """
    Parse đúng giá ngay sau nhãn:
      'Last | 4:23 AM EST' ... '97.329'
    """
    patterns = [
        r'Last\s*\|\s*[^0-9]{0,20}\d{1,2}:\d{2}\s*[AP]M\s*EST[^0-9]{0,140}(?P<v>\d{2,3}\.\d{2,4})',
        r'Last[^0-9]{0,140}(?P<v>\d{2,3}\.\d{2,4})',
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            v = parse_float_safe(m.group("v"))
            if in_dxy_range(v):
                return round(v, 4)
    return None


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
# CNBC scrape
# -------------------------
def scrape_cnbc_once(url: str, user_agent: str, viewport: dict) -> Tuple[float, str]:
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

        captured = []  # list[(value, trace)]

        def on_response(resp):
            try:
                ctype = (resp.headers or {}).get("content-type", "")
                u = resp.url.lower()
                if ("json" in ctype) or any(k in u for k in ["quote", "quotes", "chart", "graphql", "api"]):
                    txt = resp.text()
                    # strict .DXY context + price key
                    m = re.search(
                        r'(\.DXY|ICE U\.S\. Dollar Index).{0,1200}?(lastPrice|last|price).{0,80}?(\d{2,3}\.\d{2,4})',
                        txt,
                        flags=re.IGNORECASE | re.DOTALL,
                    )
                    if m:
                        v = parse_float_safe(m.group(3))
                        if in_dxy_range(v):
                            captured.append((round(v, 4), f"xhr:{u[:140]}"))
            except Exception:
                pass

        page.on("response", on_response)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
        except Exception:
            save_debug(page, "cnbc_goto_fail")
            browser.close()
            raise

        page.wait_for_timeout(8000)

        # 1) Parse from full visible text near "Last | ..."
        try:
            body_text = page.locator("body").inner_text(timeout=5000)
            v = extract_last_price_near_last_label(body_text)
            if in_dxy_range(v):
                browser.close()
                return round(v, 4), "dom:text-near-last-label"
        except Exception:
            pass

        # 2) Parse from HTML near "Last | ..."
        html = page.content()
        v = extract_last_price_near_last_label(html)
        if in_dxy_range(v):
            browser.close()
            return round(v, 4), "html:near-last-label"

        # 3) Parse strict from XHR/JSON candidates
        for val, tr in reversed(captured):
            if in_dxy_range(val):
                browser.close()
                return round(val, 4), tr

        save_debug(page, "cnbc_parse_fail")
        browser.close()
        raise RuntimeError("Cannot parse strict CNBC Last price")


def scrape_cnbc_with_retry(max_rounds=4) -> Tuple[float, str, str]:
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
                    val, trace = scrape_cnbc_once(url, ua, vp)
                    print(f"[OK] cnbc price={val} trace={trace}")
                    return val, "cnbc_playwright", trace
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
def append_csv(price: float, source: str, parse_trace: str):
    ensure_dirs()

    columns = ["datetime_utc", "dxy_index", "source", "parse_trace", "dxy_change_pct"]
    row = {
        "datetime_utc": now_utc_str(),
        "dxy_index": float(price),
        "source": source,
        "parse_trace": parse_trace,
        "dxy_change_pct": None,
    }

    # safe read
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

    # outlier guard
    if len(df) > 0:
        try:
            prev = float(df.iloc[-1]["dxy_index"])
            if abs(row["dxy_index"] - prev) > MAX_DELTA_PER_RUN:
                print(
                    f"[WARN] Outlier detected prev={prev}, new={row['dxy_index']} "
                    f"(>{MAX_DELTA_PER_RUN}) -> skip"
                )
                return
        except Exception:
            pass

    # reference for change pct
    ref = 100.0
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
            columns=["datetime_utc", "dxy_index", "source", "parse_trace", "dxy_change_pct"]
        ).to_csv(CSV_PATH, index=False)

    try:
        price, source, trace = scrape_cnbc_with_retry(max_rounds=4)
    except Exception as e:
        print("[WARN] CNBC failed -> fallback synthetic:", e)
        price = get_dxy_synthetic_from_fx()
        source = "synthetic_fx_fallback"
        trace = "fallback:synthetic_fx"

    print(f"[INFO] final_price={price} source={source} trace={trace}")
    append_csv(price, source, trace)
