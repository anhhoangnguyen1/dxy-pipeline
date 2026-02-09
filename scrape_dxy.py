import os
import re
import time
from datetime import datetime, timezone
from typing import Optional, Tuple, List

import pandas as pd
import requests
from playwright.sync_api import sync_playwright

CSV_PATH = "data/dxy_history.csv"
DEBUG_DIR = "debug"
URL = "https://www.cnbc.com/quotes/.DXY"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]

# hard sanity range for DXY
DXY_MIN = 90.0
DXY_MAX = 110.0

# max jump accepted vs previous sample (30m cadence)
MAX_DELTA_VS_PREV = 0.8

# dedup seconds
DEDUP_SECONDS = 60


def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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


def in_hard_range(v: Optional[float]) -> bool:
    return v is not None and DXY_MIN <= v <= DXY_MAX


def save_debug(page, tag: str):
    try:
        html = page.content()
        with open(os.path.join(DEBUG_DIR, f"last_page_{tag}.html"), "w", encoding="utf-8") as f:
            f.write(html)
        page.screenshot(path=os.path.join(DEBUG_DIR, f"last_screenshot_{tag}.png"), full_page=True)
    except Exception:
        pass


def load_df_safe() -> pd.DataFrame:
    columns = ["datetime_utc", "dxy_index", "source", "parse_trace", "dxy_change_pct"]
    if os.path.exists(CSV_PATH):
        try:
            if os.path.getsize(CSV_PATH) == 0:
                df = pd.DataFrame(columns=columns)
            else:
                df = pd.read_csv(CSV_PATH)
        except Exception:
            df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(columns=columns)

    for c in columns:
        if c not in df.columns:
            df[c] = None
    return df[columns]


def get_prev_dxy(df: pd.DataFrame) -> Optional[float]:
    if len(df) == 0:
        return None
    try:
        v = float(df.iloc[-1]["dxy_index"])
        if v > 0:
            return v
    except Exception:
        return None
    return None


def is_plausible_vs_prev(v: float, prev: Optional[float]) -> bool:
    if not in_hard_range(v):
        return False
    if prev is None:
        return True
    return abs(v - prev) <= MAX_DELTA_VS_PREV


# ---------- fallback ----------
def get_dxy_synthetic_from_fx() -> float:
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


# ---------- CNBC parsing ----------
def extract_candidates_from_text_near_last(text: str) -> List[float]:
    cands = []
    patterns = [
        r'Last\s*\|\s*[^0-9]{0,20}\d{1,2}:\d{2}\s*[AP]M\s*EST[^0-9]{0,140}(?P<v>\d{2,3}\.\d{2,4})',
        r'Last[^0-9]{0,120}(?P<v>\d{2,3}\.\d{2,4})',
    ]
    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.IGNORECASE | re.DOTALL):
            v = parse_float_safe(m.group("v"))
            if v is not None:
                cands.append(round(v, 4))
    return cands


def scrape_cnbc_once(user_agent: str, viewport: dict, prev_dxy: Optional[float]) -> Tuple[float, str]:
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
        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "media", "font"]
            else route.continue_(),
        )

        xhr_candidates: List[Tuple[float, str]] = []

        def on_response(resp):
            try:
                ctype = (resp.headers or {}).get("content-type", "")
                u = resp.url.lower()
                if ("json" in ctype) or any(k in u for k in ["quote", "quotes", "chart", "graphql", "api"]):
                    txt = resp.text()
                    # strict context .DXY
                    for m in re.finditer(
                        r'(\.DXY|ICE U\.S\. Dollar Index).{0,1200}?(lastPrice|last|price).{0,80}?(\d{2,3}\.\d{2,4})',
                        txt, flags=re.IGNORECASE | re.DOTALL
                    ):
                        v = parse_float_safe(m.group(3))
                        if v is not None:
                            xhr_candidates.append((round(v, 4), f"xhr:{u[:120]}"))
            except Exception:
                pass

        page.on("response", on_response)

        page.goto(URL, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(8000)

        # 1) body text near Last
        try:
            body_text = page.locator("body").inner_text(timeout=5000)
            cands = extract_candidates_from_text_near_last(body_text)
            for v in cands:
                if is_plausible_vs_prev(v, prev_dxy):
                    browser.close()
                    return v, "dom:text-near-last-label"
        except Exception:
            pass

        # 2) html near Last
        html = page.content()
        cands2 = extract_candidates_from_text_near_last(html)
        for v in cands2:
            if is_plausible_vs_prev(v, prev_dxy):
                browser.close()
                return v, "html:near-last-label"

        # 3) xhr strict
        for v, tr in reversed(xhr_candidates):
            if is_plausible_vs_prev(v, prev_dxy):
                browser.close()
                return v, tr

        save_debug(page, "cnbc_parse_fail")
        browser.close()
        raise RuntimeError("Cannot parse plausible CNBC Last price")


def scrape_cnbc_with_retry(prev_dxy: Optional[float], max_rounds: int = 4) -> Tuple[float, str, str]:
    last_err = ""
    profiles = [
        (USER_AGENTS[0], {"width": 1366, "height": 768}),
        (USER_AGENTS[1], {"width": 412, "height": 915}),
    ]
    for r in range(1, max_rounds + 1):
        for ua, vp in profiles:
            try:
                print(f"[TRY] round={r} vp={vp['width']}x{vp['height']} prev={prev_dxy}")
                v, trace = scrape_cnbc_once(ua, vp, prev_dxy)
                print(f"[OK] cnbc price={v} trace={trace}")
                return v, "cnbc_playwright", trace
            except Exception as e:
                last_err = str(e)
                print(f"[FAIL] {last_err}")
        time.sleep(4 * r)
    raise RuntimeError(last_err)


def append_csv(price: float, source: str, parse_trace: str):
    df = load_df_safe()
    prev_dxy = get_prev_dxy(df)

    # dedup by time
    if len(df) > 0 and pd.notna(df.iloc[-1]["datetime_utc"]):
        try:
            last_dt = pd.to_datetime(df.iloc[-1]["datetime_utc"], utc=True)
            now_dt = datetime.now(timezone.utc)
            if abs((now_dt - last_dt.to_pydatetime()).total_seconds()) < DEDUP_SECONDS:
                print("SKIP duplicate < 60s")
                return
        except Exception:
            pass

    # final guard before write
    if not is_plausible_vs_prev(price, prev_dxy):
        print(f"[WARN] reject before write: new={price}, prev={prev_dxy}")
        return

    ref = prev_dxy if (prev_dxy is not None and prev_dxy > 0) else 100.0
    pct = round(((price - ref) / ref) * 100.0, 6)

    row = {
        "datetime_utc": now_utc_str(),
        "dxy_index": float(price),
        "source": source,
        "parse_trace": parse_trace,
        "dxy_change_pct": pct,
    }

    # no concat warning: append via loc
    df.loc[len(df)] = row

    try:
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        df = df.sort_values("datetime_utc").reset_index(drop=True)
        df["datetime_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    tmp = CSV_PATH + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, CSV_PATH)

    print("APPENDED:", row)


if __name__ == "__main__":
    ensure_dirs()

    # ensure header exists
    if not os.path.exists(CSV_PATH):
        pd.DataFrame(columns=["datetime_utc", "dxy_index", "source", "parse_trace", "dxy_change_pct"]).to_csv(
            CSV_PATH, index=False
        )

    df0 = load_df_safe()
    prev = get_prev_dxy(df0)

    try:
        price, source, trace = scrape_cnbc_with_retry(prev_dxy=prev, max_rounds=4)
    except Exception as e:
        print("[WARN] CNBC failed -> fallback synthetic:", e)
        syn = get_dxy_synthetic_from_fx()
        # synthetic cũng phải qua guard prev
        if is_plausible_vs_prev(syn, prev):
            price, source, trace = syn, "synthetic_fx_fallback", "fallback:synthetic_fx"
        else:
            print(f"[WARN] synthetic also implausible: syn={syn}, prev={prev}. skip write.")
            raise SystemExit(0)

    print(f"[INFO] final_price={price} source={source} trace={trace}")
    append_csv(price, source, trace)
