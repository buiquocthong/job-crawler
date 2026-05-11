#!/usr/bin/env python3
"""
Vancouver Job Crawler  (v4 — OAuth2 Google Drive cho Personal Gmail)
=====================================================================
Crawl Indeed + Glassdoor: tài chính / data science / C-suite
Lọc  : Vancouver BC area | min $60k/yr hoặc $30/hr | 3 ngày gần nhất
Upload: Google Drive (OAuth2 Refresh Token) → gửi link qua MS Teams / Power Automate

Cách chạy:
  pip install -r requirements.txt
  python3 vancouver_job_crawler.py        # Chạy thật
  python3 vancouver_job_crawler.py --demo # Demo không cần proxy/credentials

Biến môi trường (set trong GitHub Secrets):
  POWER_AUTOMATE_URL        — URL Power Automate (tuỳ chọn)

  # --- Google Drive OAuth2 (personal Gmail) ---
  GDRIVE_CLIENT_ID          — OAuth2 Client ID từ Google Cloud Console
  GDRIVE_CLIENT_SECRET      — OAuth2 Client Secret
  GDRIVE_REFRESH_TOKEN      — Refresh Token (lấy 1 lần bằng --get-token)
  GOOGLE_DRIVE_FOLDER_ID    — ID folder trên Drive của bạn

  JOB_PROXY                 — "user:pass@host:port"  (tuỳ chọn)
  SCRAPER_API_KEY           — ScraperAPI key để bypass Indeed block (tuỳ chọn)

═══════════════════════════════════════════════════════════════════
  HƯỚNG DẪN LẤY OAUTH2 CREDENTIALS (chỉ làm 1 lần)
═══════════════════════════════════════════════════════════════════
  1. Vào https://console.cloud.google.com
  2. APIs & Services → Credentials → Create Credentials → OAuth 2.0 Client IDs
  3. Application type: Desktop App → đặt tên → Create
  4. Copy Client ID và Client Secret
  5. Chạy: python3 vancouver_job_crawler.py --get-token
  6. Dán refresh_token vào GitHub Secret "GDRIVE_REFRESH_TOKEN"
═══════════════════════════════════════════════════════════════════
"""

import os, sys, time, logging, re, json, tempfile
import urllib.parse, urllib.request
from datetime import date, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from jobspy import scrape_jobs

try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request as GoogleRequest
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
    GDRIVE_AVAILABLE = True
except ImportError:
    GDRIVE_AVAILABLE = False

DEMO_MODE      = "--demo"      in sys.argv
GET_TOKEN_MODE = "--get-token" in sys.argv

# ─── CẤU HÌNH ───────────────────────────────────────────────────────────────

_proxy_env         = os.getenv("JOB_PROXY", "")
PROXIES: list[str] = [_proxy_env] if _proxy_env else []
POWER_AUTOMATE_URL = os.getenv("POWER_AUTOMATE_URL", "")
SCRAPER_API_KEY    = os.getenv("SCRAPER_API_KEY", "")

GDRIVE_CLIENT_ID     = os.getenv("GDRIVE_CLIENT_ID", "")
GDRIVE_CLIENT_SECRET = os.getenv("GDRIVE_CLIENT_SECRET", "")
GDRIVE_REFRESH_TOKEN = os.getenv("GDRIVE_REFRESH_TOKEN", "")
GDRIVE_FOLDER_ID     = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

RESULTS_PER_SEARCH = 50
DAYS_OLD           = 3
DISTANCE_KM        = 35
MIN_ANNUAL         = 60_000
MIN_HOURLY         = 30.0
MIN_MONTHLY        = 5_000
SITES              = ["indeed", "glassdoor"]
OUTPUT_FILE        = Path(f"vancouver_jobs_{date.today()}.csv")

KEYWORD_GROUPS = {
    "finance_analyst": [
        "Financial Analyst", "FP&A Analyst", "Investment Analyst",
        "Quantitative Researcher", "Actuarial Analyst", "CFA Analyst",
    ],
    "data_science": [
        "Data Scientist", "Machine Learning Engineer",
        "Quantitative Analyst", "Research Scientist", "Analytics Engineer",
    ],
    "credentials": [
        "CFA Investment", "Actuary", "PhD Research", "Investment Management",
    ],
    "c_suite": [
        "Chief Executive Officer CEO", "Chief Technology Officer CTO",
        "Chief Information Officer CIO", "Chief Science Officer CSO",
        "Chief AI Officer", "Chief Investment Officer",
        "President", "CEO", "CTO", "CIO",
    ],
    "trading_investment": [
        "Equity Trader", "Portfolio Manager", "Trader",
        "Capital Markets Analyst", "Investment Associate",
    ],
}

ALLOWED_CITIES = {
    "vancouver", "burnaby", "north vancouver", "west vancouver",
    "new westminster", "richmond", "coquitlam", "port moody", "remote",
}

LOCATIONS = [
    "Vancouver, BC, Canada",
    "Burnaby, BC, Canada",
    "North Vancouver, BC, Canada",
    "West Vancouver, BC, Canada",
]

OUTPUT_COLS = [
    "title", "company_name", "location_str", "salary_display",
    "min_amount", "max_amount", "interval", "currency", "salary_source",
    "apply_method", "date_posted", "job_url", "job_url_direct",
    "site", "search_keyword", "search_group", "is_remote", "company_industry", "job_type",
]

# ─── LOGGING ────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ─── DEMO DATA ──────────────────────────────────────────────────────────────

def make_demo_data() -> pd.DataFrame:
    today = date.today()
    rows = [
        ("Financial Analyst",        "Telus",              "Vancouver",       70000, 90000,  "yearly",  "organic",      "https://telus.com/careers/1",  0, "Financial Analyst"),
        ("FP&A Manager",             "Lululemon",          "Vancouver",       95000, 120000, "yearly",  "organic",      "https://lululemon.com/jobs/2",  1, "FP&A Analyst"),
        ("Investment Analyst",       "RBC Wealth Mgmt",    "Vancouver",       80000, 105000, "yearly",  "sponsored",    "https://rbc.com/careers/3",    1, "Investment Analyst"),
        ("Quantitative Researcher",  "Absolute Return",    "Vancouver",      110000, 150000, "yearly",  "organic",      "https://absolutereturn.com/4", 0, "Quantitative Researcher"),
        ("Data Scientist",           "Hootsuite",          "Vancouver",       90000, 115000, "yearly",  "apply_direct", "",                             2, "Data Scientist"),
        ("Senior Data Scientist",    "SAP Canada",         "Vancouver",       95000, 130000, "yearly",  "organic",      "https://sap.com/ca/5",         0, "Data Scientist"),
        ("ML Engineer",              "Microsoft Canada",   "Vancouver",      105000, 145000, "yearly",  "organic",      "https://microsoft.com/jobs/6", 1, "Machine Learning Engineer"),
        ("Actuarial Analyst",        "Sun Life Financial", "Vancouver",       68000,  88000, "yearly",  "sponsored",    "https://sunlife.com/7",        2, "Actuarial Analyst"),
        ("Portfolio Manager CFA",    "Mackenzie Invest.",  "Vancouver",      130000, 175000, "yearly",  "organic",      "https://mackenzieinv.com/8",   0, "CFA Investment"),
        ("Equity Trader",            "Canaccord Genuity",  "Vancouver",      100000, 160000, "yearly",  "organic",      "https://canaccord.com/9",      1, "Equity Trader"),
        ("Chief Technology Officer", "Visier",             "Vancouver",      200000, 280000, "yearly",  "organic",      "https://visier.com/cto",       0, "Chief Technology Officer CTO"),
        ("Chief Investment Officer", "BC Investment Corp", "Vancouver",      250000, 350000, "yearly",  "organic",      "https://bcimc.com/cio",        1, "Chief Investment Officer"),
        ("Chief AI Officer",         "Ballard Power",      "Burnaby",        220000, 300000, "yearly",  "organic",      "https://ballard.com/caio",     0, "Chief AI Officer"),
        ("Systems Analyst",          "EA Sports",          "Burnaby",         75000,  95000, "yearly",  "apply_direct", "",                             2, "Data Scientist"),
        ("Investment Associate",     "Nicola Wealth",      "Vancouver",       85000, 110000, "yearly",  "organic",      "https://nicolawealth.com/15",  1, "Investment Associate"),
        ("Research Scientist AI",    "D-Wave Systems",     "Burnaby",        115000, 155000, "yearly",  "organic",      "https://dwavesys.com/17",      1, "PhD Research"),
        ("CEO",                      "Fintech Startup BC", "Vancouver",      180000, 250000, "yearly",  "apply_direct", "",                             2, "Chief Executive Officer CEO"),
        ("Capital Markets Analyst",  "CIBC Wood Gundy",    "Vancouver",       72000,  92000, "yearly",  "sponsored",    "https://cibc.com/18",          0, "Capital Markets Analyst"),
        ("Investment Manager",       "Family Office",      "West Vancouver",    None,   None, None,     "organic",      "https://familyoffice.com/19",  0, "Investment Management"),
        ("Data Analyst",             "BCAA",               "North Vancouver",   35.0,   45.0, "hourly", "apply_direct", "",                             1, "Data Scientist"),
        # Bị lọc — lương thấp
        ("Junior Analyst",   "Small Co",     "Vancouver",  40000, 55000, "yearly", "organic", "", 1, "Financial Analyst"),
        ("Data Entry Clerk", "Small Office", "Vancouver",   18.0,  22.0, "hourly", "organic", "", 1, "Financial Analyst"),
        # Bị lọc — sai địa điểm
        ("Financial Analyst", "TD Bank Toronto", "Toronto", 80000, 100000, "yearly", "organic", "", 0, "Financial Analyst"),
    ]
    records = []
    for title, co, city, mn, mx, intv, listing, url_d, days, kw in rows:
        records.append({
            "title": title, "company_name": co,
            "location": f"{city}, BC, Canada",
            "min_amount": mn, "max_amount": mx, "interval": intv,
            "listing_type": listing, "job_url_direct": url_d,
            "job_url": f"https://ca.indeed.com/viewjob?jk=demo{abs(hash(title+co))%99999:05d}",
            "date_posted": today - timedelta(days=days),
            "site": "indeed" if days % 2 == 0 else "glassdoor",
            "is_remote": False, "company_industry": "Finance & Technology",
            "job_type": "fulltime", "search_keyword": kw,
            "search_group": "demo", "currency": "CAD",
        })
    log.info(f"[DEMO] Tạo {len(records)} jobs mẫu (trước khi lọc)")
    return pd.DataFrame(records)

# ─── CRAWL ──────────────────────────────────────────────────────────────────

def crawl_jobs() -> pd.DataFrame:
    all_frames: list[pd.DataFrame] = []
    total = sum(len(v) for v in KEYWORD_GROUPS.values()) * len(LOCATIONS)
    done  = 0

    for group_name, keywords in KEYWORD_GROUPS.items():
        for keyword in keywords:
            for location in LOCATIONS:
                done += 1
                log.info(f"[{done}/{total}] '{keyword}' @ {location}")
                try:
                    df = scrape_jobs(
                        site_name=SITES, search_term=keyword, location=location,
                        country_indeed="canada", results_wanted=RESULTS_PER_SEARCH,
                        hours_old=DAYS_OLD * 24, distance=DISTANCE_KM,
                        description_format="markdown",
                        proxies=PROXIES or None, verbose=0,
                    )
                    if df is not None and not df.empty:
                        df["search_keyword"] = keyword
                        df["search_group"]   = group_name
                        has_sal  = int(df["min_amount"].notna().sum()) if "min_amount" in df.columns else 0
                        has_desc = int(df["description"].notna().sum()) if "description" in df.columns else 0
                        log.info(f"  → {len(df)} jobs | salary_direct={has_sal} | desc={has_desc}")
                        all_frames.append(df)
                    else:
                        log.warning("  → 0 jobs. Cấu hình JOB_PROXY nếu thấy 403.")
                except Exception as e:
                    log.warning(f"  → Lỗi: {e}")
                time.sleep(2)

    if not all_frames:
        log.error("Không lấy được job nào. Kiểm tra JOB_PROXY / VPN.")
        return pd.DataFrame()

    raw = pd.concat(all_frames, ignore_index=True)
    log.info(f"Tổng thô (có trùng): {len(raw)}")

    # Lưu descriptions riêng để debug
    if "description" in raw.columns:
        desc_path = Path(f"vancouver_jobs_{date.today()}_desc.jsonl")
        with open(desc_path, "w", encoding="utf-8") as f:
            for _, row in raw[["job_url", "description"]].iterrows():
                f.write(json.dumps({"url": row["job_url"], "desc": row["description"] or ""}) + "\n")
        log.info(f"Saved descriptions → {desc_path}")

    return raw

# ─── HELPERS ────────────────────────────────────────────────────────────────

def _isna(v) -> bool:
    if v is None: return True
    try: return bool(pd.isna(v))
    except: return False


def _to_text(v) -> str:
    if v is None: return ""
    try:
        if pd.isna(v): return ""
    except: pass
    if isinstance(v, dict):
        return " ".join(str(x) for x in v.values() if x)
    return str(v)


def _to_number(v):
    if _isna(v): return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return int(n) if n >= 100 else round(n, 2)


def _parse_amount(raw, suffix=""):
    if raw is None: return None
    try: amount = float(str(raw).replace(",", ""))
    except ValueError: return None
    if str(suffix or "").lower() == "k": amount *= 1000
    return int(amount) if amount >= 100 else round(amount, 2)


def _location_to_str(loc) -> str:
    if loc is None: return ""
    if isinstance(loc, str): return loc
    try:
        parts = []
        if getattr(loc, "city", None):  parts.append(loc.city)
        if getattr(loc, "state", None): parts.append(loc.state)
        return ", ".join(parts)
    except Exception:
        return str(loc)


def _get_apply_method(row) -> str:
    url = str(row.get("job_url_direct") or "")
    lst = str(row.get("listing_type") or "").lower()
    if url.startswith("http"): return "Apply on Company Site"
    if "apply_direct" in lst or "easy" in lst: return "Apply Now"
    return "Apply on Company Site"

# ─── SALARY PARSING ─────────────────────────────────────────────────────────

# Interval pattern (dùng chung nhiều regex)
_INTV_PAT = (
    r"(?P<interval>"
    r"per\s*year|a\s*year|/\s*year|/\s*yr|yr\.?|yearly|annually|annual|"
    r"per\s*hour|an\s*hour|/\s*hour|/\s*hr|hr\.?|hourly|"
    r"per\s*month|a\s*month|/\s*month|/\s*mo|monthly|"
    r"per\s*week|a\s*week|/\s*week|/\s*wk|weekly|"
    r"per\s*day|a\s*day|/\s*day|daily"
    r")"
)


def _normalize_interval_text(text):
    lbl = str(text or "").strip().lower().replace(" ", "").replace("/", "")
    return {
        "peryear": "yearly", "ayear": "yearly", "year": "yearly", "yr": "yearly",
        "yr.": "yearly", "yearly": "yearly", "annually": "yearly", "annual": "yearly",
        "perhour": "hourly", "anhour": "hourly", "hour": "hourly", "hr": "hourly",
        "hr.": "hourly", "hourly": "hourly",
        "permonth": "monthly", "amonth": "monthly", "month": "monthly",
        "mo": "monthly", "monthly": "monthly",
        "perweek": "weekly", "aweek": "weekly", "week": "weekly",
        "wk": "weekly", "weekly": "weekly",
        "perday": "daily", "aday": "daily", "day": "daily", "daily": "daily",
    }.get(lbl)


def _normalize_currency_text(text):
    lbl = str(text or "").strip().upper()
    return {"$": None, "C$": "CAD", "CAD": "CAD", "US$": "USD", "USD": "USD"}.get(lbl)


def _clean_interval(v):
    lbl = str(v or "").strip().lower()
    if not lbl or lbl in {"none", "nan"}: return None
    return {
        "year": "yearly", "yr": "yearly", "annual": "yearly", "yearly": "yearly",
        "hour": "hourly", "hr": "hourly", "hourly": "hourly",
        "month": "monthly", "monthly": "monthly",
        "week": "weekly", "weekly": "weekly",
        "day": "daily", "daily": "daily",
    }.get(lbl, lbl)


def _clean_currency(v, location=None, site=None):
    lbl = str(v or "").strip().upper()
    if lbl and lbl not in {"NONE", "NAN"}: return lbl
    loc = str(location or "").lower()
    if "canada" in loc or ", bc" in loc or str(site or "").lower() in {"indeed", "glassdoor"}:
        return "CAD"
    return "USD"


def _extract_salary_from_text(value, currency_hint="CAD"):
    """
    Parse salary từ text string. Trả về (min, max, interval, currency) hoặc None.

    Hỗ trợ các format phổ biến trên Indeed CA / Glassdoor CA:
      $80,000 - $100,000 a year
      $45.00–$55.00 per hour
      $80K–$100K annually
      From $60,000 a year
      70,000 to 90,000 per year
      Salary: $90,000 - $120,000
    """
    text = _to_text(value)
    if not text: return None
    # Normalize unicode dashes và whitespace
    text = (text.replace("\u2013", "-").replace("\u2014", "-")
                .replace("\u2212", "-").replace("\xa0", " ")
                .replace("\u2019", "'"))

    _CUR = r"(?:CAD|USD|C\$|US\$|\$)"

    # P1: Range với currency prefix — $80,000 - $100,000 a year | $45/hr - $55/hr
    p1 = (
        r"(?P<currency>" + _CUR + r")\s*"
        r"(?P<min>\d[\d,]*(?:\.\d+)?)\s*(?P<min_k>[kK]?)\s*"
        r"(?:[-\u2013/]|to|–)\s*"
        r"(?:" + _CUR + r")?\s*"
        r"(?P<max>\d[\d,]*(?:\.\d+)?)\s*(?P<max_k>[kK]?)\s*" + _INTV_PAT + r"?"
    )
    # P2: Single value với currency — $75,000/year | $35/hr | $120k annually
    p2 = (
        r"(?P<currency>" + _CUR + r")\s*"
        r"(?P<min>\d[\d,]*(?:\.\d+)?)\s*(?P<min_k>[kK]?)\s*" + _INTV_PAT
    )
    # P3: "From $60,000 a year" | "Up to $90,000 annually"
    p3 = (
        r"(?:from|up\s+to|starting\s+at|minimum|at\s+least)\s+"
        r"(?P<currency>" + _CUR + r")\s*"
        r"(?P<min>\d[\d,]*(?:\.\d+)?)\s*(?P<min_k>[kK]?)\s*" + _INTV_PAT + r"?"
    )
    # P4: Plain number range "70,000 to 90,000 per year" (no $ prefix)
    p4 = (
        r"(?<!\d)(?P<min>\d{2,3},\d{3}(?:\.\d+)?)\s*(?P<min_k>[kK]?)\s*"
        r"(?:[-\u2013]|to)\s*"
        r"(?P<max>\d{2,3},\d{3}(?:\.\d+)?)\s*(?P<max_k>[kK]?)\s*" + _INTV_PAT + r"?"
    )
    # P5: Salary label trước — "Salary: 90,000 - 120,000" hoặc "Compensation: $80K-$100K"
    p5 = (
        r"(?:salary|compensation|pay|wage|remuneration|earnings)[:\s]+\s*"
        r"(?P<currency>" + _CUR + r")?\s*"
        r"(?P<min>\d[\d,]*(?:\.\d+)?)\s*(?P<min_k>[kK]?)\s*"
        r"(?:[-\u2013]|to)?\s*"
        r"(?:" + _CUR + r")?\s*"
        r"(?P<max>\d[\d,]*(?:\.\d+)?)?\s*(?P<max_k>[kK]?)\s*" + _INTV_PAT + r"?"
    )

    for pattern in [p1, p2, p3, p4, p5]:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m: continue
        gd = m.groupdict()
        interval = _normalize_interval_text(gd.get("interval") or "")
        currency = _normalize_currency_text(gd.get("currency") or "") or currency_hint
        mn = _parse_amount(gd.get("min"), gd.get("min_k", ""))
        mx = _parse_amount(gd.get("max"), gd.get("max_k", "")) if gd.get("max") else None
        if not mn or mn <= 0: continue

        # Heuristic: đoán interval nếu thiếu
        if not interval:
            if mn < 300:    interval = "hourly"
            elif mn < 1000: interval = "monthly"
            else:           interval = "yearly"

        # Sanity checks
        if mx and mx / mn > 10: continue   # range quá rộng → sai
        if mn > 500_000: continue           # số vô lý

        return mn, mx, interval, currency

    return None


def _extract_from_comp_obj(comp):
    """Extract (min, max, interval, currency) từ jobspy Compensation object hoặc dict."""
    if isinstance(comp, dict):
        mn = _to_number(comp.get("min_amount") or comp.get("min") or comp.get("minAmount"))
        mx = _to_number(comp.get("max_amount") or comp.get("max") or comp.get("maxAmount"))
        iv_raw = comp.get("interval") or comp.get("pay_period") or comp.get("payPeriod")
        cu_raw = comp.get("currency")
    else:
        mn = _to_number(getattr(comp, "min_amount", None) or getattr(comp, "min", None))
        mx = _to_number(getattr(comp, "max_amount", None) or getattr(comp, "max", None))
        iv_raw = getattr(comp, "interval", None) or getattr(comp, "pay_period", None)
        cu_raw = getattr(comp, "currency", None)

    # jobspy trả CompensationInterval Enum — lấy .value
    if iv_raw is not None and hasattr(iv_raw, "value"):
        iv_raw = iv_raw.value
    if cu_raw is not None and hasattr(cu_raw, "value"):
        cu_raw = cu_raw.value

    interval = _clean_interval(str(iv_raw) if iv_raw else None)
    currency = _clean_currency(str(cu_raw) if cu_raw else None, None, None)
    return mn, mx, interval, currency


def _resolve_salary_fields(row):
    """
    Ưu tiên:
    1. min_amount / max_amount trực tiếp từ jobspy
    2. compensation object (jobspy Compensation)
    3. salary / salary_text field (text parse)
    4. description (text parse toàn bộ)
    """
    intv = _clean_interval(row.get("interval"))
    curr = _clean_currency(row.get("currency"), row.get("location"), row.get("site"))
    src  = row.get("salary_source")

    # 1. Direct numeric fields
    mn = _to_number(row.get("min_amount"))
    mx = _to_number(row.get("max_amount"))
    if mn is not None or mx is not None:
        return mn, mx, intv, curr, src or "direct_data"

    # 2. Compensation object/dict
    comp = row.get("compensation")
    if comp is not None and not _isna(comp):
        mn2, mx2, iv2, cu2 = _extract_from_comp_obj(comp)
        if mn2 is not None or mx2 is not None:
            return mn2, mx2, iv2 or intv, cu2 or curr, "compensation_obj"

    # 3. salary / salary_text text fields
    for field in ["salary", "salary_text"]:
        parsed = _extract_salary_from_text(row.get(field), curr)
        if parsed:
            mn3, mx3, iv3, cu3 = parsed
            return mn3, mx3, iv3 or intv, cu3 or curr, "text_parse"

    # 4. Description (toàn bộ, không cắt)
    desc = row.get("description") or ""
    parsed = _extract_salary_from_text(desc, curr)
    if parsed:
        mn3, mx3, iv3, cu3 = parsed
        return mn3, mx3, iv3 or intv, cu3 or curr, "desc_parse"

    return None, None, intv, curr, src


def _format_salary(row) -> str:
    mn   = row.get("min_amount")
    mx   = row.get("max_amount")
    intv = str(row.get("interval") or "")
    curr = str(row.get("currency") or "CAD")
    src  = str(row.get("salary_source") or "")
    if _isna(mn) and _isna(mx): return "N/A"

    def fmt(v):
        if _isna(v): return None
        return f"${int(float(v)):,}" if float(v) > 200 else f"${float(v):.2f}"

    parts = [p for p in [fmt(mn), fmt(mx)] if p]
    lbl   = {"yearly": "/yr", "hourly": "/hr", "monthly": "/mo",
              "weekly": "/wk", "daily": "/day"}.get(intv.lower(), "")
    est   = " (est.)" if "parse" in src else ""
    return f"{curr} {' - '.join(parts)}{lbl}{est}"

# ─── NORMALIZE ───────────────────────────────────────────────────────────────

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Đảm bảo tồn tại tất cả các cột cần thiết
    for col in ["min_amount", "max_amount", "interval", "currency", "date_posted",
                "listing_type", "job_url_direct", "is_remote", "location",
                "company_name", "title", "job_url", "site", "search_keyword",
                "description", "salary_source", "salary", "salary_text", "compensation"]:
        if col not in df.columns:
            df[col] = None

    df["location_str"] = df["location"].apply(_location_to_str)

    sal = df.apply(_resolve_salary_fields, axis=1, result_type="expand")
    sal.columns = ["min_amount", "max_amount", "interval", "currency", "salary_source"]
    df[["min_amount", "max_amount", "interval", "currency", "salary_source"]] = sal

    df["apply_method"]   = df.apply(_get_apply_method, axis=1)
    df["salary_display"] = df.apply(_format_salary, axis=1)
    df["date_posted"]    = pd.to_datetime(df["date_posted"], errors="coerce").dt.date

    total   = len(df)
    has_sal = int(df["min_amount"].notna().sum())
    by_src  = df["salary_source"].value_counts().to_dict()
    log.info(f"Salary coverage sau normalize: {has_sal}/{total} ({has_sal/total*100:.0f}%)")
    log.info(f"  Theo nguồn: {by_src}")
    return df

# ─── SALARY ENRICHMENT ──────────────────────────────────────────────────────

ENRICH_TIMEOUT  = 15
ENRICH_MAX_JOBS = 80
ENRICH_WORKERS  = 4
ENRICH_SLEEP    = 1.5

_ENRICH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Upgrade-Insecure-Requests": "1",
}


def _build_proxies() -> dict | None:
    if not _proxy_env: return None
    proxy_url = f"http://{_proxy_env}" if not _proxy_env.startswith("http") else _proxy_env
    return {"http": proxy_url, "https": proxy_url}


def _fetch_html(url: str) -> str | None:
    """
    Fetch HTML của một URL.
    Ưu tiên ScraperAPI (nếu có key) để bypass Indeed block.
    Fallback sang requests trực tiếp với proxy.
    """
    # ScraperAPI — bypass Indeed/Glassdoor anti-bot tốt nhất
    if SCRAPER_API_KEY:
        try:
            api_url = (
                "http://api.scraperapi.com/?"
                + urllib.parse.urlencode({
                    "api_key": SCRAPER_API_KEY,
                    "url": url,
                    "render": "true",
                })
            )
            resp = requests.get(api_url, timeout=30)
            if resp.status_code == 200:
                return resp.text
            log.debug(f"ScraperAPI {resp.status_code} for {url[:60]}")
        except Exception as e:
            log.debug(f"ScraperAPI error: {e}")

    # Fallback: requests trực tiếp với proxy
    try:
        proxies = _build_proxies()
        resp = requests.get(url, headers=_ENRICH_HEADERS, proxies=proxies,
                            timeout=ENRICH_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        log.debug(f"HTTP {resp.status_code} for {url[:60]}")
    except requests.exceptions.Timeout:
        log.debug(f"Timeout: {url[:60]}")
    except Exception as e:
        log.debug(f"Fetch error {url[:60]}: {e}")

    return None


def _parse_indeed_mosaic(html: str, currency_hint: str = "CAD"):
    """
    Indeed nhúng salary vào JSON trong mosaic-data / window._initialData.
    Đây là nguồn chính xác nhất, không phụ thuộc JS rendering.

    Các pattern được test với Indeed CA 2025-2026.
    """
    patterns = [
        # Formatted salary string (hiển thị trực tiếp)
        r'"(?:formattedSalary|salaryRange|displaySalary)"\s*:\s*"([^"]{5,100})"',
        # salaryInfo object
        r'"salaryInfo"\s*:\s*\{[^}]{0,500}"formattedSalary"\s*:\s*"([^"]{5,100})"',
        # compensation block
        r'"compensation"\s*:\s*\{[^}]{0,500}"formattedSalary"\s*:\s*"([^"]{5,100})"',
        # Numeric min/max trong JSON
        r'"(?:salaryMin|minSalary|minAmount)"\s*:\s*([\d.]+)',
        # mosaic-data script block
        r'<script[^>]*id=["\']mosaic-data["\'][^>]*>(.*?)</script>',
    ]

    for pat in patterns:
        m = re.search(pat, html, re.DOTALL | re.IGNORECASE)
        if not m: continue
        try:
            fragment = m.group(1)

            # Nếu là JSON block lớn, tìm salary string bên trong
            if fragment.startswith("{") or "<" not in fragment:
                sal_m = re.search(
                    r'"(?:formattedSalary|salaryRange|displaySalary|salary)"\s*:\s*"([^"]{5,100})"',
                    fragment, re.IGNORECASE
                )
                if sal_m:
                    parsed = _extract_salary_from_text(sal_m.group(1), currency_hint)
                    if parsed: return (*parsed, "page_mosaic")

                # Numeric min/max
                min_m = re.search(r'"(?:salaryMin|minSalary|minAmount)"\s*:\s*([\d.]+)', fragment)
                max_m = re.search(r'"(?:salaryMax|maxSalary|maxAmount)"\s*:\s*([\d.]+)', fragment)
                if min_m:
                    mn = float(min_m.group(1))
                    mx = float(max_m.group(1)) if max_m else None
                    intv = "yearly" if mn > 1000 else "hourly"
                    return (int(mn), int(mx) if mx else None, intv, currency_hint, "page_mosaic")
            else:
                # Là salary string trực tiếp
                parsed = _extract_salary_from_text(fragment, currency_hint)
                if parsed: return (*parsed, "page_mosaic")
        except Exception:
            continue
    return None


def _parse_jsonld(html: str, currency_hint: str = "CAD"):
    """Parse salary từ JSON-LD schema.org/JobPosting."""
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    ):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue

        # Hỗ trợ cả list và object
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict): continue
            bs = item.get("baseSalary") or item.get("estimatedSalary")
            if not bs: continue

            if isinstance(bs, dict):
                val = bs.get("value") or {}
                if isinstance(val, dict):
                    mn_v = val.get("minValue", "")
                    mx_v = val.get("maxValue", "")
                    unit = val.get("unitText", "")
                    sal_text = f"${mn_v} - ${mx_v} {unit}".strip()
                elif isinstance(val, (int, float, str)):
                    sal_text = f"${val} {bs.get('unitText','')}".strip()
                else:
                    continue
            elif isinstance(bs, str):
                sal_text = bs
            else:
                continue

            parsed = _extract_salary_from_text(sal_text, currency_hint)
            if parsed:
                return (*parsed, "page_jsonld")
    return None


# HTML selectors để extract salary text từ Indeed + Glassdoor
_HTML_SALARY_SELECTORS = [
    r'(?:data-testid=["\']salaryInfoAndJobType["\'][^>]*>)(.*?)(?=<)',
    r'(?:class=["\'][^"\']*salary[^"\']*["\'][^>]*>)\s*([^<]{5,80})',
    r'(?:aria-label=["\']Salary["\'][^>]*>)\s*([^<]{5,80})',
    r'(?:data-test=["\']salary-estimate["\'][^>]*>)\s*([^<]{5,80})',
    r'(?:class=["\'][^"\']*SalaryEstimate[^"\']*["\'][^>]*>)\s*([^<]{5,80})',
]


def _fetch_salary_from_url(url: str, currency_hint: str = "CAD"):
    """Fetch một URL và trả về (mn, mx, interval, currency, source) hoặc None."""
    if not url or not url.startswith("http"):
        return None

    html = _fetch_html(url)
    if not html:
        return None

    # Indeed: mosaic JSON là nguồn chính xác nhất
    if "indeed.com" in url:
        result = _parse_indeed_mosaic(html, currency_hint)
        if result: return result

    # JSON-LD (schema.org) — chuẩn nhất cho các ATS
    result = _parse_jsonld(html, currency_hint)
    if result: return result

    # HTML selectors
    for sel in _HTML_SALARY_SELECTORS:
        m = re.search(sel, html, re.IGNORECASE | re.DOTALL)
        if m:
            candidate = re.sub(r"<[^>]+>", " ", m.group(1)).strip()
            parsed = _extract_salary_from_text(candidate, currency_hint)
            if parsed:
                return (*parsed, "page_html")

    # Fallback: scan text đầu trang (salary thường xuất hiện sớm)
    text_only = re.sub(r"<[^>]+>", " ", html[:20000])
    text_only = re.sub(r"\s+", " ", text_only)
    parsed = _extract_salary_from_text(text_only, currency_hint)
    if parsed:
        return (*parsed, "page_text")

    return None


def enrich_salaries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Với các job salary_display == 'N/A', fetch job page để lấy salary.
    Ưu tiên job_url_direct (trang company ATS) → fallback job_url (Indeed).
    """
    mask  = df["salary_display"] == "N/A"
    na_df = df[mask].head(ENRICH_MAX_JOBS)
    if na_df.empty:
        log.info("Không có job N/A cần enrich.")
        return df

    proxy_info = f"proxy={_proxy_env[:20]}..." if _proxy_env else "no proxy"
    scraper_info = "ScraperAPI=ON" if SCRAPER_API_KEY else "ScraperAPI=OFF"
    log.info(f"🔍 Enriching {len(na_df)} jobs ({proxy_info} | {scraper_info})...")
    enriched = 0

    def _process_row(idx_row):
        idx, row = idx_row
        curr = str(row.get("currency") or "CAD")
        for url in [row.get("job_url_direct"), row.get("job_url")]:
            url_str = str(url or "").strip()
            if not url_str or not url_str.startswith("http"):
                continue
            time.sleep(ENRICH_SLEEP)
            result = _fetch_salary_from_url(url_str, curr)
            if result:
                return idx, result
        return idx, None

    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as executor:
        futures = {executor.submit(_process_row, (idx, row)): idx
                   for idx, row in na_df.iterrows()}
        done = 0
        for future in as_completed(futures):
            done += 1
            idx, result = future.result()
            if result:
                mn, mx, intv, curr, src = result
                df.at[idx, "min_amount"]     = mn
                df.at[idx, "max_amount"]     = mx
                df.at[idx, "interval"]       = intv
                df.at[idx, "currency"]       = curr
                df.at[idx, "salary_source"]  = src
                df.at[idx, "salary_display"] = _format_salary(df.loc[idx])
                enriched += 1
                log.info(f"  ✅ [{done}/{len(na_df)}] {df.at[idx,'title'][:30]} → "
                         f"{df.at[idx,'salary_display']} ({src})")
            elif done % 10 == 0:
                log.info(f"  ⏳ [{done}/{len(na_df)}] enriched={enriched}")

    pct = f"{enriched/len(na_df)*100:.0f}%" if len(na_df) > 0 else "0%"
    log.info(f"✅ Enrich xong: +{enriched}/{len(na_df)} jobs có lương ({pct})")
    if enriched == 0 and not _proxy_env and not SCRAPER_API_KEY:
        log.warning("⚠️  Enrich = 0. Indeed thường chặn request trực tiếp.")
        log.warning("   Set JOB_PROXY=user:pass@host:port hoặc SCRAPER_API_KEY để bypass.")
    return df

# ─── LỌC ────────────────────────────────────────────────────────────────────

def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    n0 = len(df)

    df = df.drop_duplicates(subset=["job_url"], keep="first")
    log.info(f"Sau dedup URL     : {len(df)} (bỏ {n0 - len(df)})")

    df = df[df["location_str"].apply(
        lambda l: any(c in l.lower() for c in ALLOWED_CITIES) if l else False)]
    log.info(f"Sau lọc địa điểm  : {len(df)}")

    cutoff = date.today() - timedelta(days=DAYS_OLD)
    df = df[df["date_posted"].apply(
        lambda d: d is None or (isinstance(d, date) and d >= cutoff))]
    log.info(f"Sau lọc ngày      : {len(df)}")

    df = df[df.apply(_salary_ok, axis=1)]
    log.info(f"Sau lọc lương     : {len(df)}")
    return df.reset_index(drop=True)


def _salary_ok(row) -> bool:
    mn   = row.get("min_amount")
    mx   = row.get("max_amount")
    intv = str(row.get("interval") or "").lower()
    if _isna(mn) and _isna(mx): return True   # không có lương → giữ lại, enrich sau
    amount = float(mn if not _isna(mn) else mx)
    return amount >= {"yearly": MIN_ANNUAL, "hourly": MIN_HOURLY,
                      "monthly": MIN_MONTHLY, "weekly": MIN_HOURLY * 40,
                      "daily": MIN_HOURLY * 8}.get(intv, MIN_ANNUAL)

# ─── XUẤT FILE ───────────────────────────────────────────────────────────────

def save_csv(df: pd.DataFrame) -> Path:
    cols = [c for c in OUTPUT_COLS if c in df.columns]
    out  = df[cols].copy()
    if "min_amount" in out.columns:
        out = out.sort_values(["min_amount", "date_posted"],
                              ascending=[False, False], na_position="last")
    out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    log.info(f"Saved {len(out)} jobs → {OUTPUT_FILE.resolve()}")
    return OUTPUT_FILE


def save_excel(df: pd.DataFrame) -> Path | None:
    try:
        import openpyxl
        from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
        from openpyxl.utils import get_column_letter
    except ImportError:
        log.warning("openpyxl chưa cài — bỏ qua xuất Excel. pip install openpyxl")
        return None

    xlsx_path = OUTPUT_FILE.with_suffix(".xlsx")
    cols = [c for c in OUTPUT_COLS if c in df.columns]
    out  = df[cols].copy()
    if "min_amount" in out.columns:
        out = out.sort_values(["min_amount", "date_posted"],
                              ascending=[False, False], na_position="last")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Vancouver Jobs"

    hdr_fill = PatternFill("solid", fgColor="1F4E79")
    hdr_font = Font(bold=True, color="FFFFFF", size=11)
    thin     = Side(border_style="thin", color="CCCCCC")
    bdr      = Border(left=thin, right=thin, top=thin, bottom=thin)

    for ci, col in enumerate(cols, 1):
        cell = ws.cell(row=1, column=ci, value=col)
        cell.fill = hdr_fill
        cell.font = hdr_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = bdr

    na_fill = PatternFill("solid", fgColor="FFE0E0")
    sal_col = cols.index("salary_display") + 1 if "salary_display" in cols else None

    for ri, row_data in enumerate(out.itertuples(index=False), 2):
        is_na = sal_col and str(getattr(row_data, "salary_display", "")) == "N/A"
        for ci, val in enumerate(row_data, 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.border = bdr
            cell.alignment = Alignment(vertical="center", wrap_text=False)
            if is_na and ci == sal_col:
                cell.fill = na_fill

    col_widths = {
        "title": 36, "company_name": 24, "location_str": 22,
        "salary_display": 30, "min_amount": 12, "max_amount": 12,
        "interval": 10, "currency": 9, "salary_source": 18,
        "apply_method": 24, "date_posted": 13, "job_url": 50,
        "job_url_direct": 50, "site": 10, "search_keyword": 28,
        "search_group": 16, "is_remote": 9, "company_industry": 26, "job_type": 12,
    }
    for ci, col in enumerate(cols, 1):
        ws.column_dimensions[get_column_letter(ci)].width = col_widths.get(col, 16)

    ws.row_dimensions[1].height = 30
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    wb.save(xlsx_path)
    log.info(f"Saved Excel → {xlsx_path.resolve()}")
    return xlsx_path

# ─── GOOGLE DRIVE UPLOAD (OAuth2 — Personal Gmail) ───────────────────────────

def _build_oauth2_service():
    creds = Credentials(
        token=None,
        refresh_token=GDRIVE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GDRIVE_CLIENT_ID,
        client_secret=GDRIVE_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    if not creds.valid:
        creds.refresh(GoogleRequest())
    return build("drive", "v3", credentials=creds)


def _delete_existing_files(service, folder_id: str, filename: str):
    result = service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
        fields="files(id,name)"
    ).execute()
    for f in result.get("files", []):
        service.files().delete(fileId=f["id"]).execute()
        log.info(f"🗑️  Đã xoá file cũ: {f['name']} (id={f['id']})")


def _verify_upload(service, file_id: str, expected_name: str, expected_folder_id: str) -> bool:
    try:
        meta = service.files().get(
            fileId=file_id, fields="id,name,parents,trashed,size"
        ).execute()
        checks = {
            "Tên file khớp" : meta.get("name") == expected_name,
            "Không bị trash": not meta.get("trashed", True),
            "Đúng folder"   : expected_folder_id in meta.get("parents", []),
            "Có dung lượng" : int(meta.get("size", 0)) > 0,
        }
        for label, ok in checks.items():
            log.info(f"  Verify [{'✅' if ok else '❌'}] {label}")
        return all(checks.values())
    except HttpError as e:
        log.error(f"❌ Không verify được file {file_id}: {e}")
        return False


def upload_to_drive(file_path: Path) -> tuple[str | None, str | None]:
    """Upload file lên Google Drive. Trả về (folder_url, file_url)."""
    if not GDRIVE_AVAILABLE:
        log.warning("⚠️  google-api-python-client chưa cài — bỏ qua Drive upload.")
        return None, None

    missing = [name for name, val in [
        ("GDRIVE_CLIENT_ID",       GDRIVE_CLIENT_ID),
        ("GDRIVE_CLIENT_SECRET",   GDRIVE_CLIENT_SECRET),
        ("GDRIVE_REFRESH_TOKEN",   GDRIVE_REFRESH_TOKEN),
        ("GOOGLE_DRIVE_FOLDER_ID", GDRIVE_FOLDER_ID),
    ] if not val]
    if missing:
        log.warning(f"⚠️  Thiếu secrets: {', '.join(missing)} — bỏ qua Drive upload.")
        return None, None

    try:
        log.info("🔐 Kết nối Google Drive (OAuth2)...")
        service = _build_oauth2_service()
        log.info("✅ Xác thực OAuth2 thành công.")

        _delete_existing_files(service, GDRIVE_FOLDER_ID, file_path.name)

        suffix   = file_path.suffix.lower()
        mimetype = ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    if suffix == ".xlsx" else "text/csv")
        file_meta = {"name": file_path.name, "parents": [GDRIVE_FOLDER_ID]}
        media     = MediaFileUpload(str(file_path), mimetype=mimetype, resumable=True)
        log.info(f"⬆️  Uploading '{file_path.name}' ({file_path.stat().st_size:,} bytes)...")

        uploaded = service.files().create(
            body=file_meta, media_body=media, fields="id,name,size"
        ).execute()

        file_id = uploaded["id"]
        log.info(f"✅ Upload thành công: {uploaded['name']} (id={file_id})")

        service.permissions().create(
            fileId=file_id, body={"type": "anyone", "role": "reader"},
        ).execute()

        _verify_upload(service, file_id, file_path.name, GDRIVE_FOLDER_ID)

        folder_url = f"https://drive.google.com/drive/folders/{GDRIVE_FOLDER_ID}"
        file_url   = f"https://drive.google.com/file/d/{file_id}/view"
        return folder_url, file_url

    except HttpError as e:
        error_str = str(e)
        if "storageQuotaExceeded" in error_str:
            log.error("❌ 403 storageQuotaExceeded — kiểm tra Folder ID và OAuth scope.")
        elif "invalid_grant" in error_str or "Token has been expired" in error_str:
            log.error("❌ Refresh token hết hạn. Chạy lại --get-token và cập nhật secret.")
        elif "404" in error_str or "notFound" in error_str:
            log.error("❌ 404 Không tìm thấy folder. Kiểm tra GOOGLE_DRIVE_FOLDER_ID.")
        elif "403" in error_str:
            log.error("❌ 403 Không có quyền truy cập folder.")
        else:
            log.error(f"❌ Drive upload lỗi: {e}")
        return None, None

    except Exception as e:
        if "invalid_grant" in str(e):
            log.error("❌ OAuth2 refresh token không hợp lệ. Chạy lại --get-token.")
        else:
            log.error(f"❌ Drive upload lỗi không mong đợi: {e}")
        return None, None

# ─── LẤY OAUTH2 TOKEN (chạy 1 lần) ─────────────────────────────────────────

def get_oauth2_token():
    """
    Chạy: python3 vancouver_job_crawler.py --get-token
    Mở browser → xác thực → in refresh_token ra console.
    """
    import webbrowser
    from http.server import HTTPServer, BaseHTTPRequestHandler

    client_id     = GDRIVE_CLIENT_ID     or input("Nhập GDRIVE_CLIENT_ID: ").strip()
    client_secret = GDRIVE_CLIENT_SECRET or input("Nhập GDRIVE_CLIENT_SECRET: ").strip()

    REDIRECT_URI    = "http://localhost:8080"
    auth_code_holder = {}

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if "code" in params:
                auth_code_holder["code"] = params["code"][0]
                body, code = b"<h2>Xac thuc thanh cong! Ban co the dong tab nay.</h2>", 200
            elif "error" in params:
                auth_code_holder["error"] = params["error"][0]
                body, code = f"<h2>Loi: {params['error'][0]}</h2>".encode(), 400
            else:
                body, code = b"<h2>Dang cho...</h2>", 200
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={urllib.parse.quote(client_id)}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
        "&response_type=code"
        "&scope=https://www.googleapis.com/auth/drive"
        "&access_type=offline"
        "&prompt=consent"
    )

    print("\n" + "═" * 62)
    print("  BƯỚC 1: Browser sẽ tự mở — đăng nhập Gmail và cho phép")
    print("═" * 62)
    try:
        webbrowser.open(auth_url)
        print("  (Browser đã tự mở)\n")
    except Exception:
        print(f"  Copy URL sau và mở thủ công:\n  {auth_url}\n")

    print("  Đang chờ xác thực tại localhost:8080...")
    HTTPServer(("localhost", 8080), _Handler).handle_request()

    if "error" in auth_code_holder:
        print(f"\n❌ Lỗi xác thực: {auth_code_holder['error']}")
        return
    if "code" not in auth_code_holder:
        print("\n❌ Không nhận được auth code.")
        return

    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "code": auth_code_holder["code"],
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    })
    if resp.status_code != 200:
        print(f"\n❌ Lỗi lấy token: {resp.text}")
        return

    refresh_token = resp.json().get("refresh_token")
    if not refresh_token:
        print("\n❌ Không nhận được refresh_token.")
        print("   Vào https://myaccount.google.com/permissions → thu hồi quyền → chạy lại.")
        return

    print("═" * 62)
    print("  ✅ Lấy token thành công!")
    print("═" * 62)
    print(f"\n  REFRESH TOKEN:\n  {refresh_token}\n")
    print("  BƯỚC TIẾP THEO:")
    print("  1. Copy refresh token trên")
    print("  2. GitHub repo → Settings → Secrets → Actions")
    print("  3. Update secret: GDRIVE_REFRESH_TOKEN = <paste ở đây>")

# ─── MS TEAMS / POWER AUTOMATE NOTIFICATION ─────────────────────────────────

def send_teams_notification(df: pd.DataFrame, csv_path: Path,
                            drive_folder_url: str | None = None,
                            xlsx_file_url:    str | None = None):
    if not POWER_AUTOMATE_URL:
        log.warning("⚠️  POWER_AUTOMATE_URL chưa cấu hình — bỏ qua gửi Teams.")
        return

    today_str  = date.today().strftime("%d/%m/%Y")
    total      = len(df)
    has_salary = int(df["salary_display"].ne("N/A").sum()) if "salary_display" in df.columns else 0
    download_url = xlsx_file_url or drive_folder_url or ""

    adaptive_card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.3",
        "body": [
            {
                "type": "TextBlock",
                "text": f"Vancouver Jobs — {today_str}",
                "weight": "Bolder",
                "size": "Medium",
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Tổng jobs:",  "value": str(total)},
                    {"title": "Có lương:",   "value": f"{has_salary}/{total}"},
                    {"title": "Nguồn:",      "value": "Indeed + Glassdoor CA"},
                    {"title": "File:",       "value": csv_path.name.replace(".csv", ".xlsx")},
                ],
            },
        ],
        "actions": ([
            {"type": "Action.OpenUrl", "title": "Mở File Excel", "url": download_url},
            {"type": "Action.OpenUrl", "title": "Mở Drive Folder", "url": drive_folder_url or download_url},
        ] if download_url else []),
    }

    try:
        resp = requests.post(
            POWER_AUTOMATE_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"card": adaptive_card}),
            timeout=30,
        )
        if resp.status_code in (200, 202):
            log.info("✅ Đã gửi Adaptive Card qua Power Automate.")
        else:
            log.error(f"❌ Power Automate lỗi {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.error(f"❌ Lỗi Power Automate: {e}")

# ─── SUMMARY ────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame):
    SEP = "═" * 62
    print(f"\n{SEP}")
    print(f" JOB CRAWLER {'[DEMO]' if DEMO_MODE else ''}")
    print(SEP)

    if df.empty:
        print("  ⚠️  Không có kết quả. Cấu hình JOB_PROXY / SCRAPER_API_KEY và chạy lại.")
        print(SEP + "\n"); return

    print(f"  Tổng jobs         : {len(df)}")

    if "site" in df.columns:
        for site, cnt in df["site"].value_counts().items():
            print(f"  {site.capitalize():<18}: {cnt}")

    if "search_keyword" in df.columns:
        print("\n  Top keywords:")
        for kw, cnt in df["search_keyword"].value_counts().head(8).items():
            print(f"    • {kw:<40}: {cnt}")

    has_salary = int(df["salary_display"].ne("N/A").sum()) if "salary_display" in df.columns else 0
    pct = f"{has_salary/len(df)*100:.0f}%" if len(df) > 0 else "0%"
    print(f"\n  Có lương          : {has_salary}  ({pct})")
    print(f"  Không có lương    : {len(df) - has_salary}")

    if "salary_source" in df.columns:
        print("\n  Nguồn lương:")
        for src, cnt in df["salary_source"].value_counts().items():
            print(f"    • {str(src):<32}: {cnt}")

    if "min_amount" in df.columns and df["min_amount"].notna().any():
        print("\n  Top 5 lương cao nhất:")
        for _, r in df.nlargest(5, "min_amount").iterrows():
            t = str(r.get("title", ""))[:36]
            c = str(r.get("company_name", ""))[:22]
            s = str(r.get("salary_display", ""))
            print(f"    {t:<37} {c:<23} {s}")

    print(f"\n  File CSV: {OUTPUT_FILE.resolve()}")
    print(SEP + "\n")

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    if GET_TOKEN_MODE:
        get_oauth2_token()
        return

    print("\n" + "═" * 62)
    print(f"  Vancouver Job Crawler v4 {'[DEMO MODE]' if DEMO_MODE else ''}")
    print(f"  Sites   : {', '.join(SITES)}")
    print(f"  Ngày    : {DAYS_OLD} ngày gần nhất")
    print(f"  Lương   : >= ${MIN_ANNUAL:,}/yr  hoặc  ${MIN_HOURLY}/hr")
    print(f"  Proxy   : {'OK — ' + _proxy_env[:30] if _proxy_env else 'chưa cấu hình'}")
    print(f"  Scraper : {'✅ ScraperAPI' if SCRAPER_API_KEY else '⚠️  chưa cấu hình'}")
    print(f"  P.Auto  : {'✅' if POWER_AUTOMATE_URL else '⚠️  chưa cấu hình'}")

    drive_ready = all([GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET,
                       GDRIVE_REFRESH_TOKEN, GDRIVE_FOLDER_ID])
    if drive_ready:
        print("  GDrive  : ✅ OAuth2 personal Gmail")
    else:
        missing = [n for n, v in [
            ("CLIENT_ID", GDRIVE_CLIENT_ID), ("CLIENT_SECRET", GDRIVE_CLIENT_SECRET),
            ("REFRESH_TOKEN", GDRIVE_REFRESH_TOKEN), ("FOLDER_ID", GDRIVE_FOLDER_ID),
        ] if not v]
        print(f"  GDrive  : ⚠️  Thiếu: {', '.join(missing)}")
    print("═" * 62 + "\n")

    raw_df = make_demo_data() if DEMO_MODE else crawl_jobs()
    if raw_df.empty:
        print_summary(raw_df); return

    norm_df     = normalize(raw_df)
    filtered_df = filter_jobs(norm_df)

    # Enrich salary cho các job N/A (chỉ ở chế độ thực)
    if not DEMO_MODE:
        filtered_df = enrich_salaries(filtered_df)
        filtered_df["salary_display"] = filtered_df.apply(_format_salary, axis=1)

    csv_path  = save_csv(filtered_df)
    xlsx_path = save_excel(filtered_df)
    print_summary(filtered_df)

    drive_folder_url, _ = upload_to_drive(csv_path)

    xlsx_file_url = None
    if xlsx_path:
        folder_url2, xlsx_file_url = upload_to_drive(xlsx_path)
        if folder_url2:
            drive_folder_url = folder_url2

    if drive_folder_url: log.info(f"📁 Drive folder : {drive_folder_url}")
    if xlsx_file_url:    log.info(f"📊 Excel file   : {xlsx_file_url}")

    send_teams_notification(filtered_df, csv_path,
                            drive_folder_url=drive_folder_url,
                            xlsx_file_url=xlsx_file_url)


if __name__ == "__main__":
    main()
