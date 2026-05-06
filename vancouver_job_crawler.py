#!/usr/bin/env python3
"""
Vancouver Job Crawler  (v3 — OAuth2 Google Drive cho Personal Gmail)
=====================================================================
Crawl Indeed + Glassdoor: tài chính / data science / C-suite
Lọc  : Vancouver BC area | min $60k/yr hoặc $30/hr | 3 ngày gần nhất
Upload: Google Drive (OAuth2 Refresh Token) → gửi link qua MS Teams / Power Automate

Cách chạy:
  pip install -r requirements.txt
  python3 vancouver_job_crawler.py        # Chạy thật
  python3 vancouver_job_crawler.py --demo # Demo không cần proxy/credentials

Biến môi trường (set trong GitHub Secrets):
  TEAMS_WEBHOOK_URL         — URL webhook kênh Teams (tuỳ chọn)
  POWER_AUTOMATE_URL        — URL Power Automate (tuỳ chọn)

  # --- Google Drive OAuth2 (personal Gmail) ---
  GDRIVE_CLIENT_ID          — OAuth2 Client ID từ Google Cloud Console
  GDRIVE_CLIENT_SECRET      — OAuth2 Client Secret
  GDRIVE_REFRESH_TOKEN      — Refresh Token (lấy 1 lần bằng script lấy token bên dưới)
  GOOGLE_DRIVE_FOLDER_ID    — ID folder trên Drive của bạn (share "Editor" cho OAuth app)

  JOB_PROXY                 — "user:pass@host:port"  (tuỳ chọn)
  GITHUB_RUN_URL            — tự điền bởi workflow
  GITHUB_REPO               — "owner/repo"  (tự điền bởi workflow)

═══════════════════════════════════════════════════════════════════
  HƯỚNG DẪN LẤY OAUTH2 CREDENTIALS (chỉ làm 1 lần)
═══════════════════════════════════════════════════════════════════
  1. Vào https://console.cloud.google.com
  2. APIs & Services → Credentials → Create Credentials → OAuth 2.0 Client IDs
  3. Application type: Desktop App → đặt tên → Create
  4. Copy Client ID và Client Secret
  5. Chạy script lấy token (ở cuối file này):
       python3 vancouver_job_crawler.py --get-token
  6. Dán refresh_token vào GitHub Secret "GDRIVE_REFRESH_TOKEN"
═══════════════════════════════════════════════════════════════════
"""

import os, sys, time, logging, re, json, tempfile, webbrowser
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from jobspy import scrape_jobs
import base64

# Google Drive
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
TEAMS_WEBHOOK_URL  = os.getenv("TEAMS_WEBHOOK_URL", "")
GITHUB_RUN_URL     = os.getenv("GITHUB_RUN_URL", "")
GITHUB_REPO        = os.getenv("GITHUB_REPO", "")
POWER_AUTOMATE_URL = os.getenv("POWER_AUTOMATE_URL", "")

# Google Drive — OAuth2 (personal Gmail)
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

# ─── LOGGING ────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ─── DEMO DATA ──────────────────────────────────────────────────────────────

def make_demo_data() -> pd.DataFrame:
    today = date.today()
    rows = [
        ("Financial Analyst",        "Telus",              "Vancouver",       70000, 90000,  "yearly",  "organic",   "https://telus.com/careers/1",   0, "Financial Analyst"),
        ("FP&A Manager",             "Lululemon",          "Vancouver",       95000, 120000, "yearly",  "organic",   "https://lululemon.com/jobs/2",  1, "FP&A Analyst"),
        ("Investment Analyst",       "RBC Wealth Mgmt",    "Vancouver",       80000, 105000, "yearly",  "sponsored", "https://rbc.com/careers/3",     1, "Investment Analyst"),
        ("Quantitative Researcher",  "Absolute Return",    "Vancouver",      110000, 150000, "yearly",  "organic",   "https://absolutereturn.com/4",  0, "Quantitative Researcher"),
        ("Data Scientist",           "Hootsuite",          "Vancouver",       90000, 115000, "yearly",  "apply_direct","",                            2, "Data Scientist"),
        ("Senior Data Scientist",    "SAP Canada",         "Vancouver",       95000, 130000, "yearly",  "organic",   "https://sap.com/ca/5",          0, "Data Scientist"),
        ("ML Engineer",              "Microsoft Canada",   "Vancouver",      105000, 145000, "yearly",  "organic",   "https://microsoft.com/jobs/6",  1, "Machine Learning Engineer"),
        ("Actuarial Analyst",        "Sun Life Financial", "Vancouver",       68000,  88000, "yearly",  "sponsored", "https://sunlife.com/7",         2, "Actuarial Analyst"),
        ("Portfolio Manager CFA",    "Mackenzie Invest.",  "Vancouver",      130000, 175000, "yearly",  "organic",   "https://mackenzieinv.com/8",    0, "CFA Investment"),
        ("Equity Trader",            "Canaccord Genuity",  "Vancouver",      100000, 160000, "yearly",  "organic",   "https://canaccord.com/9",       1, "Equity Trader"),
        ("Chief Technology Officer", "Visier",             "Vancouver",      200000, 280000, "yearly",  "organic",   "https://visier.com/cto",        0, "Chief Technology Officer CTO"),
        ("Chief Investment Officer", "BC Investment Corp", "Vancouver",      250000, 350000, "yearly",  "organic",   "https://bcimc.com/cio",         1, "Chief Investment Officer"),
        ("Chief AI Officer",         "Ballard Power",      "Burnaby",        220000, 300000, "yearly",  "organic",   "https://ballard.com/caio",      0, "Chief AI Officer"),
        ("Systems Analyst",          "EA Sports",          "Burnaby",         75000,  95000, "yearly",  "apply_direct","",                            2, "Data Scientist"),
        ("Investment Associate",     "Nicola Wealth",      "Vancouver",       85000, 110000, "yearly",  "organic",   "https://nicolawealth.com/15",   1, "Investment Associate"),
        ("Research Scientist AI",    "D-Wave Systems",     "Burnaby",        115000, 155000, "yearly",  "organic",   "https://dwavesys.com/17",       1, "PhD Research"),
        ("CEO",                      "Fintech Startup BC", "Vancouver",      180000, 250000, "yearly",  "apply_direct","",                            2, "Chief Executive Officer CEO"),
        ("Capital Markets Analyst",  "CIBC Wood Gundy",    "Vancouver",       72000,  92000, "yearly",  "sponsored", "https://cibc.com/18",           0, "Capital Markets Analyst"),
        ("Investment Manager",       "Family Office",      "West Vancouver",   None,   None, None,      "organic",   "https://familyoffice.com/19",   0, "Investment Management"),
        ("Data Analyst",             "BCAA",               "North Vancouver",  35.0,   45.0, "hourly",  "apply_direct","",                            1, "Data Scientist"),
        # Bị lọc — lương thấp
        ("Junior Analyst",  "Small Co",      "Vancouver",  40000, 55000, "yearly", "organic", "", 1, "Financial Analyst"),
        ("Data Entry Clerk","Small Office",  "Vancouver",   18.0,  22.0, "hourly", "organic", "", 1, "Financial Analyst"),
        # Bị lọc — sai địa điểm
        ("Financial Analyst","TD Bank Toronto","Toronto",  80000,100000, "yearly", "organic", "", 0, "Financial Analyst"),
    ]
    records = []
    for title,co,city,mn,mx,intv,listing,url_d,days,kw in rows:
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
                        # Debug: log salary coverage của batch này
                        has_sal = int(df["min_amount"].notna().sum()) if "min_amount" in df.columns else 0
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

    # Lưu raw data (bao gồm description) để có thể re-process sau
    raw_path = Path(f"vancouver_jobs_{date.today()}_raw.csv")
    raw_cols  = [c for c in raw.columns if c != "description"]  # description riêng
    raw[raw_cols].to_csv(raw_path, index=False, encoding="utf-8-sig")

    # Lưu description riêng (có thể rất lớn)
    if "description" in raw.columns:
        desc_path = Path(f"vancouver_jobs_{date.today()}_desc.jsonl")
        with open(desc_path, "w", encoding="utf-8") as f:
            for _, row in raw[["job_url","description"]].iterrows():
                f.write(json.dumps({"url": row["job_url"], "desc": row["description"] or ""}) + "\n")
        log.info(f"Saved descriptions → {desc_path}")

    return raw

# ─── NORMALIZE & SALARY FIX ─────────────────────────────────────────────────

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["min_amount","max_amount","interval","currency","date_posted",
                "listing_type","job_url_direct","is_remote","location",
                "company_name","title","job_url","site","search_keyword",
                "description","salary_source","salary","salary_text","compensation"]:
        if col not in df.columns:
            df[col] = None

    df["location_str"] = df["location"].apply(_location_to_str)

    sal = df.apply(_resolve_salary_fields, axis=1, result_type="expand")
    sal.columns = ["min_amount","max_amount","interval","currency","salary_source"]
    df[["min_amount","max_amount","interval","currency","salary_source"]] = sal

    df["apply_method"]   = df.apply(_get_apply_method, axis=1)
    df["salary_display"] = df.apply(_format_salary, axis=1)
    df["date_posted"]    = pd.to_datetime(df["date_posted"], errors="coerce").dt.date

    # Log salary coverage để debug
    total = len(df)
    has_sal = int(df["min_amount"].notna().sum())
    by_src  = df["salary_source"].value_counts().to_dict()
    log.info(f"Salary coverage sau normalize: {has_sal}/{total} ({has_sal/total*100:.0f}%)")
    log.info(f"  Theo nguồn: {by_src}")
    desc_null = int(df["description"].isna().sum())
    log.info(f"  Description null/empty: {desc_null}/{total} "
             f"({'⚠️ jobspy không lấy được description → salary parse bị hạn chế' if desc_null > total*0.5 else 'OK'})")
    return df


def _location_to_str(loc) -> str:
    if loc is None: return ""
    if isinstance(loc, str): return loc
    try:
        parts = []
        if getattr(loc,"city",None):  parts.append(loc.city)
        if getattr(loc,"state",None): parts.append(loc.state)
        return ", ".join(parts)
    except Exception:
        return str(loc)


def _get_apply_method(row) -> str:
    url = str(row.get("job_url_direct") or "")
    lst = str(row.get("listing_type")   or "").lower()
    if url.startswith("http"): return "Apply on Company Site"
    if "apply_direct" in lst or "easy" in lst: return "Apply Now"
    return "Apply on Company Site"


def _isna(v) -> bool:
    if v is None: return True
    try: return bool(pd.isna(v))
    except: return False


def _resolve_salary_fields(row):
    intv = _clean_interval(row.get("interval"))
    curr = _clean_currency(row.get("currency"), row.get("location"), row.get("site"))
    src  = row.get("salary_source")

    mn = _to_number(row.get("min_amount"))
    mx = _to_number(row.get("max_amount"))
    if mn is not None or mx is not None:
        return mn, mx, intv, curr, src or "direct_data"

    comp = row.get("compensation")
    if comp is not None and not _isna(comp):
        if isinstance(comp, dict):
            mn2, mx2, iv2, cu2 = _extract_from_comp_dict(comp)
        else:
            mn2 = _to_number(getattr(comp, "min_amount", None) or getattr(comp, "min", None))
            mx2 = _to_number(getattr(comp, "max_amount", None) or getattr(comp, "max", None))
            iv_raw = getattr(comp, "interval", None) or getattr(comp, "pay_period", None)
            # BUG FIX: jobspy trả CompensationInterval Enum, cần lấy .value
            # str(CompensationInterval.YEARLY) = "CompensationInterval.YEARLY" → sai
            # CompensationInterval.YEARLY.value = "yearly" → đúng
            if iv_raw is not None and hasattr(iv_raw, "value"):
                iv_raw = iv_raw.value
            iv2 = _clean_interval(str(iv_raw) if iv_raw else None)
            cu_raw = getattr(comp, "currency", None)
            if cu_raw is not None and hasattr(cu_raw, "value"):
                cu_raw = cu_raw.value
            cu2 = _clean_currency(str(cu_raw) if cu_raw else None, row.get("location"), row.get("site"))
        if mn2 is not None or mx2 is not None:
            return mn2, mx2, iv2 or intv, cu2 or curr, "compensation_obj"

    # Thứ tự ưu tiên parse: salary field → salary_text → description (đầy đủ, không cắt 2000)
    for field in ["salary", "salary_text"]:
        parsed = _extract_salary_from_text(row.get(field), curr)
        if parsed:
            mn3,mx3,iv3,cu3 = parsed
            return mn3, mx3, iv3 or intv, cu3 or curr, "text_parse"

    # Parse toàn bộ description (không cắt ngắn còn 2000 ký tự)
    desc = row.get("description") or ""
    parsed = _extract_salary_from_text(desc, curr)
    if parsed:
        mn3,mx3,iv3,cu3 = parsed
        return mn3, mx3, iv3 or intv, cu3 or curr, "desc_parse"

    return None, None, intv, curr, src


def _extract_from_comp_dict(comp: dict):
    mn2 = _to_number(comp.get("min_amount") or comp.get("min") or comp.get("minAmount"))
    mx2 = _to_number(comp.get("max_amount") or comp.get("max") or comp.get("maxAmount"))
    iv_raw = comp.get("interval") or comp.get("pay_period") or comp.get("payPeriod")
    if iv_raw is not None and hasattr(iv_raw, "value"):
        iv_raw = iv_raw.value
    iv2 = _clean_interval(iv_raw)
    cu2 = _clean_currency(comp.get("currency"), None, None)
    return mn2, mx2, iv2, cu2


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

def _extract_salary_from_text(value, currency_hint="CAD"):
    text = _to_text(value)
    if not text: return None
    text = (text.replace("\u2013","-").replace("\u2014","-")
                .replace("\u2212","-").replace("\xa0"," ")
                .replace("\u2019","'"))

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

    for i, pattern in enumerate([p1, p2, p3, p4, p5]):
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m: continue
        gd = m.groupdict()
        interval = _normalize_interval_text(gd.get("interval") or "")
        currency = _normalize_currency_text(gd.get("currency") or "") or currency_hint

        mn = _parse_amount(gd.get("min"), gd.get("min_k",""))
        mx = _parse_amount(gd.get("max"), gd.get("max_k","")) if gd.get("max") else None

        if not mn or mn <= 0: continue

        # Heuristic tự động đoán interval nếu thiếu
        if not interval:
            if mn < 300:    interval = "hourly"
            elif mn < 1000: interval = "monthly"
            else:           interval = "yearly"

        # Sanity check: loại bỏ số phone / zip code
        if mn > 0 and mx and mx / mn > 10: continue   # range quá rộng → sai
        if mn > 500_000: continue                      # số vô lý

        return mn, mx, interval, currency

    return None


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
    try: amount = float(str(raw).replace(",",""))
    except ValueError: return None
    if str(suffix or "").lower() == "k": amount *= 1000
    return int(amount) if amount >= 100 else round(amount, 2)


def _normalize_interval_text(text):
    lbl = str(text or "").strip().lower().replace(" ","").replace("/","")
    return {
        "peryear":"yearly","ayear":"yearly","year":"yearly","yr":"yearly","yr.":"yearly",
        "yearly":"yearly","annually":"yearly","annual":"yearly",
        "perhour":"hourly","anhour":"hourly","hour":"hourly","hr":"hourly","hr.":"hourly",
        "hourly":"hourly",
        "permonth":"monthly","amonth":"monthly","month":"monthly","mo":"monthly","monthly":"monthly",
        "perweek":"weekly","aweek":"weekly","week":"weekly","wk":"weekly","weekly":"weekly",
        "perday":"daily","aday":"daily","day":"daily","daily":"daily",
    }.get(lbl)


def _normalize_currency_text(text):
    lbl = str(text or "").strip().upper()
    return {"$":None,"C$":"CAD","CAD":"CAD","US$":"USD","USD":"USD"}.get(lbl)


def _clean_interval(v):
    lbl = str(v or "").strip().lower()
    if not lbl or lbl in {"none","nan"}: return None
    return {"year":"yearly","yr":"yearly","annual":"yearly","yearly":"yearly",
            "hour":"hourly","hr":"hourly","hourly":"hourly",
            "month":"monthly","monthly":"monthly",
            "week":"weekly","weekly":"weekly",
            "day":"daily","daily":"daily"}.get(lbl, lbl)


def _clean_currency(v, location=None, site=None):
    lbl = str(v or "").strip().upper()
    if lbl and lbl not in {"NONE","NAN"}: return lbl
    loc = str(location or "").lower()
    if "canada" in loc or ", bc" in loc or str(site or "").lower() in {"indeed","glassdoor"}:
        return "CAD"
    return "USD"


def _format_salary(row) -> str:
    mn   = row.get("min_amount")
    mx   = row.get("max_amount")
    intv = str(row.get("interval")      or "")
    curr = str(row.get("currency")      or "CAD")
    src  = str(row.get("salary_source") or "")
    if _isna(mn) and _isna(mx): return "N/A"
    def fmt(v):
        if _isna(v): return None
        return f"${int(float(v)):,}" if float(v) > 200 else f"${float(v):.2f}"
    parts = [p for p in [fmt(mn), fmt(mx)] if p]
    lbl   = {"yearly":"/yr","hourly":"/hr","monthly":"/mo",
             "weekly":"/wk","daily":"/day"}.get(intv.lower(),"")
    est   = " (est.)" if "parse" in src else ""
    return f"{curr} {' - '.join(parts)}{lbl}{est}"


# ─── SALARY ENRICHMENT — Fetch job page để lấy salary ───────────────────────
#
#  Với các job vẫn còn N/A sau normalize(), module này fetch trực tiếp
#  trang job (ưu tiên job_url_direct, fallback job_url) và parse salary
#  từ HTML. Chạy với max_workers=6 để nhanh, có retry và timeout an toàn.
#
#  Các selector được test với Indeed CA và Glassdoor CA (2025-2026).
# ─────────────────────────────────────────────────────────────────────────────

ENRICH_TIMEOUT    = 15   # giây timeout mỗi request
ENRICH_MAX_JOBS   = 80   # tối đa bao nhiêu job N/A sẽ được enrich
ENRICH_WORKERS    = 4    # concurrent threads (thấp hơn để tránh rate limit)
ENRICH_SLEEP      = 1.5  # sleep giữa các request (giây)

# Build proxy dict từ JOB_PROXY env (dùng lại cho enrich)
def _build_proxies() -> dict | None:
    if not _proxy_env: return None
    proxy_url = f"http://{_proxy_env}" if not _proxy_env.startswith("http") else _proxy_env
    return {"http": proxy_url, "https": proxy_url}

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

# Selector patterns để extract salary text từ HTML (Indeed + Glassdoor)
_HTML_SALARY_SELECTORS = [
    # Indeed: salary widget trên đầu trang
    r'(?:data-testid=["\']salaryInfoAndJobType["\'][^>]*>)(.*?)(?=<)',
    r'(?:class=["\'][^"\']*salary[^"\']*["\'][^>]*>)\s*([^<]{5,80})',
    r'(?:aria-label=["\']Salary["\'][^>]*>)\s*([^<]{5,80})',
    # Glassdoor: salary section
    r'(?:data-test=["\']salary-estimate["\'][^>]*>)\s*([^<]{5,80})',
    r'(?:class=["\'][^"\']*SalaryEstimate[^"\']*["\'][^>]*>)\s*([^<]{5,80})',
    # JSON-LD structured data (Indeed, LinkedIn, nhiều ATS)
    r'"baseSalary"\s*:\s*\{[^}]*"value"\s*:\s*\{[^}]*"minValue"\s*:\s*([\d.]+)',
    r'"salary"\s*:\s*"([^"]{5,100})"',
]

def _fetch_salary_from_url(url: str, currency_hint: str = "CAD"):
    """Fetch một URL và parse salary. Return (mn, mx, interval, currency, src) hoặc None."""
    if not url or not url.startswith("http"):
        return None
    proxies = _build_proxies()
    try:
        resp = requests.get(url, headers=_ENRICH_HEADERS, proxies=proxies,
                            timeout=ENRICH_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            log.debug(f"HTTP {resp.status_code} for {url[:60]}")
            return None
        html = resp.text

        # --- Indeed specific: parse mosaic-data JSON (chứa salary không qua JS) ---
        if "indeed.com" in url:
            result = _parse_indeed_mosaic_json(html, currency_hint)
            if result:
                return result

        # Thử JSON-LD (schema.org/JobPosting) — chuẩn nhất
        for jld_match in re.finditer(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, re.DOTALL | re.IGNORECASE
        ):
            try:
                data = json.loads(jld_match.group(1))
                sal_text = _extract_salary_from_jsonld(data)
                if sal_text:
                    parsed = _extract_salary_from_text(sal_text, currency_hint)
                    if parsed:
                        return (*parsed, "page_jsonld")
            except Exception:
                pass

        # Thử HTML selectors
        for sel in _HTML_SALARY_SELECTORS:
            m = re.search(sel, html, re.IGNORECASE | re.DOTALL)
            if m:
                candidate = re.sub(r"<[^>]+>", " ", m.group(1)).strip()
                parsed = _extract_salary_from_text(candidate, currency_hint)
                if parsed:
                    return (*parsed, "page_html")

        # Fallback: tìm salary pattern trong text visible đầu trang (8KB)
        # Chỉ lấy phần đầu trang vì salary thường xuất hiện sớm
        text_only = re.sub(r"<[^>]+>", " ", html[:20000])
        text_only = re.sub(r"\s+", " ", text_only)
        parsed = _extract_salary_from_text(text_only, currency_hint)
        if parsed:
            return (*parsed, "page_text")

    except requests.exceptions.Timeout:
        log.debug(f"Timeout fetching {url[:60]}")
    except requests.exceptions.ProxyError as e:
        log.debug(f"Proxy error {url[:60]}: {e}")
    except Exception as e:
        log.debug(f"Fetch error {url[:60]}: {e}")
    return None


def _parse_indeed_mosaic_json(html: str, currency_hint: str = "CAD"):
    """
    Indeed nhúng salary vào JSON trong window._initialData hoặc mosaic-data.
    Đây là nguồn chính xác nhất, không qua JS rendering.
    """
    # Pattern 1: window._initialData = {...}
    for pat in [
        r'window\._initialData\s*=\s*(\{.*?\});\s*(?:window|</script>)',
        r'"salaryInfo"\s*:\s*(\{[^}]{10,300}\})',
        r'"compensation"\s*:\s*(\{[^}]{10,400}\})',
        r'<script[^>]*id=["\']mosaic-data["\'][^>]*>(.*?)</script>',
    ]:
        m = re.search(pat, html, re.DOTALL | re.IGNORECASE)
        if not m: continue
        try:
            # Extract salary-related keys từ JSON fragment
            frag = m.group(1)
            # Look for salary patterns trong JSON string
            salary_pattern = re.search(
                r'"(?:formattedSalary|salaryRange|salaryMin|salary|baseSalary)"\s*:\s*"([^"]{5,100})"',
                frag, re.IGNORECASE
            )
            if salary_pattern:
                parsed = _extract_salary_from_text(salary_pattern.group(1), currency_hint)
                if parsed: return (*parsed, "page_mosaic")

            # Try numeric min/max
            min_m = re.search(r'"(?:salaryMin|minSalary|minAmount)"\s*:\s*([\d.]+)', frag)
            max_m = re.search(r'"(?:salaryMax|maxSalary|maxAmount)"\s*:\s*([\d.]+)', frag)
            if min_m:
                mn = float(min_m.group(1))
                mx = float(max_m.group(1)) if max_m else None
                intv = "yearly" if mn > 1000 else "hourly"
                return (int(mn), int(mx) if mx else None, intv, currency_hint, "page_mosaic")
        except Exception:
            continue
    return None


def _extract_salary_from_jsonld(data) -> str | None:
    """Trích salary string từ JSON-LD object (schema.org/JobPosting)."""
    if isinstance(data, list):
        for item in data:
            r = _extract_salary_from_jsonld(item)
            if r: return r
        return None
    if not isinstance(data, dict): return None

    # schema.org baseSalary
    bs = data.get("baseSalary") or data.get("estimatedSalary")
    if bs:
        if isinstance(bs, dict):
            val = bs.get("value") or {}
            if isinstance(val, dict):
                mn = val.get("minValue","")
                mx = val.get("maxValue","")
                unit = val.get("unitText","")
                if mn or mx:
                    return f"${mn} - ${mx} {unit}".strip()
            elif isinstance(val, (int, float, str)):
                unit = bs.get("unitText","")
                return f"${val} {unit}".strip()
        elif isinstance(bs, str):
            return bs

    # Tìm đệ quy trong các key khác
    for key in ["jobBenefits", "description"]:
        v = data.get(key)
        if isinstance(v, str) and len(v) > 0:
            parsed = _extract_salary_from_text(v[:3000], "CAD")
            if parsed:
                mn, mx, intv, curr = parsed
                label = {"yearly":"per year","hourly":"per hour","monthly":"per month"}.get(intv,"")
                return f"${mn} - ${mx} {label}".strip() if mx else f"${mn} {label}".strip()
    return None


def enrich_salaries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Với các job salary_display == 'N/A', fetch job page để lấy salary.
    Ưu tiên job_url_direct (trang company, ít bị block hơn) → fallback job_url (Indeed).
    Dùng proxy nếu JOB_PROXY được cấu hình.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    mask = df["salary_display"] == "N/A"
    na_df = df[mask].head(ENRICH_MAX_JOBS)
    if na_df.empty:
        log.info("Không có job N/A cần enrich.")
        return df

    proxy_info = f"proxy={_proxy_env[:20]}..." if _proxy_env else "no proxy"
    log.info(f"🔍 Enriching salary cho {len(na_df)} jobs ({proxy_info})...")
    enriched = 0

    def _process_row(idx_row):
        idx, row = idx_row
        curr = str(row.get("currency") or "CAD")
        urls_tried = []

        # Ưu tiên: job_url_direct (company ATS) → job_url (Indeed/Glassdoor)
        for url in [row.get("job_url_direct"), row.get("job_url")]:
            url_str = str(url or "").strip()
            if not url_str or not url_str.startswith("http"):
                continue
            urls_tried.append(url_str[:50])
            time.sleep(ENRICH_SLEEP)
            result = _fetch_salary_from_url(url_str, curr)
            if result:
                return idx, result
        log.debug(f"Enrich failed: {row.get('title','?')[:30]} | tried: {urls_tried}")
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
                df.at[idx, "min_amount"]    = mn
                df.at[idx, "max_amount"]    = mx
                df.at[idx, "interval"]      = intv
                df.at[idx, "currency"]      = curr
                df.at[idx, "salary_source"] = src
                df.at[idx, "salary_display"] = _format_salary(df.loc[idx])
                enriched += 1
                log.info(f"  ✅ [{done}/{len(na_df)}] {df.at[idx,'title'][:30]} → "
                         f"{df.at[idx,'salary_display']} ({src})")
            elif done % 10 == 0:
                log.info(f"  ⏳ [{done}/{len(na_df)}] enriched={enriched}")

    pct = f"{enriched/len(na_df)*100:.0f}%" if na_df.shape[0] > 0 else "0%"
    log.info(f"✅ Enrich xong: +{enriched}/{len(na_df)} jobs có lương ({pct})")
    if enriched == 0 and not _proxy_env:
        log.warning("⚠️  Enrich = 0. Indeed thường chặn request không có proxy.")
        log.warning("   Set biến môi trường JOB_PROXY=user:pass@host:port để bypass.")
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
    if _isna(mn) and _isna(mx): return True
    amount = float(mn if not _isna(mn) else mx)
    return amount >= {"yearly":MIN_ANNUAL,"hourly":MIN_HOURLY,
                      "monthly":MIN_MONTHLY,"weekly":MIN_HOURLY*40,
                      "daily":MIN_HOURLY*8}.get(intv, MIN_ANNUAL)

# ─── XUẤT CSV ───────────────────────────────────────────────────────────────

OUTPUT_COLS = [
    "title","company_name","location_str","salary_display",
    "min_amount","max_amount","interval","currency","salary_source",
    "apply_method","date_posted","job_url","job_url_direct",
    "site","search_keyword","search_group","is_remote","company_industry","job_type",
]

def save_results(df: pd.DataFrame) -> Path:
    cols = [c for c in OUTPUT_COLS if c in df.columns]
    out  = df[cols].copy()
    if "min_amount" in out.columns:
        out = out.sort_values(["min_amount","date_posted"],
                              ascending=[False,False], na_position="last")
    out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    log.info(f"Saved {len(out)} jobs → {OUTPUT_FILE.resolve()}")
    return OUTPUT_FILE

# ─── GOOGLE DRIVE UPLOAD (OAuth2 — Personal Gmail) ───────────────────────────
#
#  Tại sao dùng OAuth2 thay vì Service Account?
#  ─────────────────────────────────────────────
#  Service Account không có storage quota (0 GB).
#  Khi upload lên "My Drive" được share → Drive API coi file thuộc về SA → 403.
#  Shared Drive fix được vấn đề này NHƯNG chỉ có trên Google Workspace, không có trên
#  personal Gmail.
#
#  OAuth2 Refresh Token = upload dưới danh nghĩa chính tài khoản Gmail của bạn
#  → file thuộc về bạn → dùng 15 GB quota cá nhân → không bao giờ bị 403 quota.
#
#  Secrets cần thiết:
#    GDRIVE_CLIENT_ID      — từ Google Cloud Console (OAuth2 Desktop App)
#    GDRIVE_CLIENT_SECRET  — từ Google Cloud Console
#    GDRIVE_REFRESH_TOKEN  — chạy: python3 vancouver_job_crawler.py --get-token
#    GOOGLE_DRIVE_FOLDER_ID — ID folder trên Drive của bạn
# ─────────────────────────────────────────────────────────────────────────────

def _build_oauth2_service():
    """
    Tạo Google Drive service dùng OAuth2 credentials (personal Gmail).
    Tự động refresh access token khi hết hạn — chỉ cần refresh_token 1 lần.
    """
    creds = Credentials(
        token=None,                              # access token — sẽ tự refresh
        refresh_token=GDRIVE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GDRIVE_CLIENT_ID,
        client_secret=GDRIVE_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    # Refresh ngay để kiểm tra credentials hợp lệ
    if not creds.valid:
        creds.refresh(GoogleRequest())
    return build("drive", "v3", credentials=creds)


def _delete_existing_files(service, folder_id: str, filename: str):
    """Xoá file cũ trùng tên trong folder (tránh duplicate)."""
    result = service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
        fields="files(id,name)"
    ).execute()
    for f in result.get("files", []):
        service.files().delete(fileId=f["id"]).execute()
        log.info(f"🗑️  Đã xoá file cũ: {f['name']} (id={f['id']})")


def _verify_upload(service, file_id: str, expected_name: str,
                   expected_folder_id: str) -> bool:
    """Xác nhận file đã upload đúng chỗ, đúng tên, không bị trash."""
    try:
        meta = service.files().get(
            fileId=file_id,
            fields="id,name,parents,trashed,size"
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


def upload_to_drive(csv_path: Path) -> str | None:
    """
    Upload CSV lên Google Drive personal (OAuth2).
    Trả về link folder nếu thành công, None nếu lỗi.
    """
    if not GDRIVE_AVAILABLE:
        log.warning("⚠️  google-api-python-client chưa cài — bỏ qua Drive upload.")
        return None

    # Kiểm tra đủ credentials
    missing = [name for name, val in [
        ("GDRIVE_CLIENT_ID",     GDRIVE_CLIENT_ID),
        ("GDRIVE_CLIENT_SECRET", GDRIVE_CLIENT_SECRET),
        ("GDRIVE_REFRESH_TOKEN", GDRIVE_REFRESH_TOKEN),
        ("GOOGLE_DRIVE_FOLDER_ID", GDRIVE_FOLDER_ID),
    ] if not val]

    if missing:
        log.warning(f"⚠️  Thiếu secrets: {', '.join(missing)} — bỏ qua Drive upload.")
        log.warning("    Chạy: python3 vancouver_job_crawler.py --get-token để lấy token.")
        return None

    try:
        log.info("🔐 Khởi tạo Google Drive service (OAuth2 personal)...")
        service = _build_oauth2_service()
        log.info("✅ Xác thực OAuth2 thành công.")

        # Xoá file cũ trùng tên
        log.info(f"🔍 Kiểm tra file trùng tên '{csv_path.name}'...")
        _delete_existing_files(service, GDRIVE_FOLDER_ID, csv_path.name)

        # Upload
        suffix   = csv_path.suffix.lower()
        mimetype = ("application/vnd.openxmlformats-officedocument"
                    ".spreadsheetml.sheet" if suffix == ".xlsx" else "text/csv")
        file_meta = {"name": csv_path.name, "parents": [GDRIVE_FOLDER_ID]}
        media     = MediaFileUpload(str(csv_path), mimetype=mimetype, resumable=True)
        log.info(f"⬆️  Đang upload '{csv_path.name}' ({csv_path.stat().st_size:,} bytes)...")

        uploaded = service.files().create(
            body=file_meta,
            media_body=media,
            fields="id,name,size"
        ).execute()

        file_id = uploaded["id"]
        log.info(f"✅ Upload thành công: {uploaded['name']} (id={file_id})")

        # Cấp quyền xem cho anyone có link (để Teams có thể mở được)
        service.permissions().create(
            fileId=file_id,
            body={"type": "anyone", "role": "reader"},
        ).execute()

        # Verify
        log.info("🔎 Verifying upload...")
        _verify_upload(service, file_id, csv_path.name, GDRIVE_FOLDER_ID)

        file_url   = f"https://drive.google.com/file/d/{file_id}/view"
        folder_url = f"https://drive.google.com/drive/folders/{GDRIVE_FOLDER_ID}"
        return folder_url, file_url

    except HttpError as e:
        error_str = str(e)
        if "storageQuotaExceeded" in error_str:
            log.error("❌ 403 storageQuotaExceeded — Folder ID có thể sai hoặc "
                      "token chưa được authorize đúng scope. Chạy --get-token lại.")
        elif "invalid_grant" in error_str or "Token has been expired" in error_str:
            log.error("❌ Refresh token hết hạn hoặc bị revoke.\n"
                      "   Fix: Chạy lại python3 vancouver_job_crawler.py --get-token "
                      "và cập nhật GDRIVE_REFRESH_TOKEN trong GitHub Secrets.")
        elif "404" in error_str or "notFound" in error_str:
            log.error("❌ 404 Không tìm thấy folder. Kiểm tra GOOGLE_DRIVE_FOLDER_ID.")
        elif "403" in error_str:
            log.error("❌ 403 Không có quyền. "
                      "Đảm bảo bạn đã share folder cho tài khoản Gmail đang dùng OAuth2.")
        else:
            log.error(f"❌ Drive upload lỗi: {e}")
        return None

    except Exception as e:
        if "invalid_grant" in str(e):
            log.error("❌ OAuth2 refresh token không hợp lệ. "
                      "Chạy lại --get-token và cập nhật secret.")
        else:
            log.error(f"❌ Drive upload lỗi không mong đợi: {e}")
        return None, None

# ─── LẤY OAUTH2 TOKEN (chạy 1 lần) ─────────────────────────────────────────

def get_oauth2_token():
    """
    Chạy: python3 vancouver_job_crawler.py --get-token

    Dùng localhost redirect (thay thế oob đã bị Google khai tử từ 2022).
    Script tự mở browser, tự bắt auth code qua local server — không cần copy/paste.
    """
    import urllib.parse
    import urllib.request
    from http.server import HTTPServer, BaseHTTPRequestHandler

    client_id     = GDRIVE_CLIENT_ID     or input("Nhập GDRIVE_CLIENT_ID: ").strip()
    client_secret = GDRIVE_CLIENT_SECRET or input("Nhập GDRIVE_CLIENT_SECRET: ").strip()

    REDIRECT_URI = "http://localhost:8080"
    auth_code_holder = {}   # dùng dict để closure ghi được vào

    # ── Local server bắt redirect từ Google ──
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)

            if "code" in params:
                auth_code_holder["code"] = params["code"][0]
                body = b"<h2>Xac thuc thanh cong! Ban co the dong tab nay.</h2>"
                self.send_response(200)
            elif "error" in params:
                auth_code_holder["error"] = params["error"][0]
                body = f"<h2>Loi: {params['error'][0]}</h2>".encode()
                self.send_response(400)
            else:
                body = b"<h2>Dang cho...</h2>"
                self.send_response(200)

            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass   # tắt log mặc định của HTTPServer

    # ── Tạo auth URL ──
    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={urllib.parse.quote(client_id)}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
        "&response_type=code"
        "&scope=https://www.googleapis.com/auth/drive"
        "&access_type=offline"
        "&prompt=consent"
    )

    print("\n" + "═"*62)
    print("  BƯỚC 1: Browser sẽ tự mở — đăng nhập Gmail và cho phép")
    print("═"*62)
    print(f"\n  URL: {auth_url}\n")

    try:
        import webbrowser
        webbrowser.open(auth_url)
        print("  (Browser đã tự mở)\n")
    except Exception:
        print("  (Copy URL trên và mở thủ công)\n")

    # ── Chạy local server chờ Google redirect về ──
    print("  Đang chờ xác thực... (server đang lắng nghe tại localhost:8080)")
    server = HTTPServer(("localhost", 8080), _Handler)
    server.handle_request()   # xử lý đúng 1 request rồi dừng

    if "error" in auth_code_holder:
        print(f"\n❌ Lỗi xác thực: {auth_code_holder['error']}")
        return
    if "code" not in auth_code_holder:
        print("\n❌ Không nhận được auth code.")
        return

    auth_code = auth_code_holder["code"]
    print("  ✅ Đã nhận auth code từ Google.\n")

    # ── Đổi auth code lấy token ──
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code"         : auth_code,
            "client_id"    : client_id,
            "client_secret": client_secret,
            "redirect_uri" : REDIRECT_URI,    # phải khớp với Cloud Console
            "grant_type"   : "authorization_code",
        }
    )

    if resp.status_code != 200:
        print(f"\n❌ Lỗi lấy token: {resp.text}")
        return

    token_data    = resp.json()
    refresh_token = token_data.get("refresh_token")

    if not refresh_token:
        print("\n❌ Không nhận được refresh_token.")
        print("   Thử vào https://myaccount.google.com/permissions")
        print("   Thu hồi quyền của app → chạy lại --get-token.")
        return

    print("═"*62)
    print("  ✅ Lấy token thành công!")
    print("═"*62)
    print(f"\n  REFRESH TOKEN:\n  {refresh_token}\n")
    print("  BƯỚC TIẾP THEO:")
    print("  1. Copy refresh token trên")
    print("  2. GitHub repo → Settings → Secrets → Actions")
    print("  3. Update secret: GDRIVE_REFRESH_TOKEN = <paste ở đây>")
    print("  4. Cũng cập nhật: GDRIVE_CLIENT_ID và GDRIVE_CLIENT_SECRET\n")

# ─── MS TEAMS / POWER AUTOMATE NOTIFICATION ─────────────────────────────────

def send_teams_notification(df: pd.DataFrame, csv_path: Path,
                            drive_folder_url: str | None = None,
                            xlsx_file_url:    str | None = None):

    today_str  = date.today().strftime("%d/%m/%Y")
    total      = len(df)
    has_salary = int(df["salary_display"].ne("N/A").sum()) if "salary_display" in df.columns else 0

    # Chọn link tốt nhất để gắn vào nút tải
    download_url = xlsx_file_url or drive_folder_url or ""

    # ── Gửi qua TEAMS_WEBHOOK_URL (Adaptive Card trực tiếp) ──
    teams_webhook = os.getenv("TEAMS_WEBHOOK_URL", "")
    if teams_webhook:
        adaptive_card = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"Vancouver Jobs — {today_str}",
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": "Accent"
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Tổng jobs tìm được:", "value": str(total)},
                                {"title": "Có thông tin lương:", "value": f"{has_salary} / {total}"},
                                {"title": "Nguồn:", "value": "Indeed CA + Glassdoor CA"},
                                {"title": "File:", "value": csv_path.name.replace(".csv", ".xlsx")},
                            ]
                        }
                    ],
                    "actions": [
                        {
                            "type": "Action.OpenUrl",
                            "title": "Tải file Excel hôm nay",
                            "url": download_url
                        },
                        {
                            "type": "Action.OpenUrl",
                            "title": "Mở folder Drive",
                            "url": drive_folder_url or download_url
                        }
                    ] if download_url else []
                }
            }]
        }
        try:
            resp = requests.post(teams_webhook,
                                 headers={"Content-Type": "application/json"},
                                 data=json.dumps(adaptive_card),
                                 timeout=30)
            if resp.status_code in (200, 202):
                log.info("✅ Đã gửi Adaptive Card vào Teams (webhook).")
            else:
                log.error(f"❌ Teams webhook lỗi {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.error(f"❌ Lỗi Teams webhook: {e}")

    # ── Gửi qua POWER_AUTOMATE_URL (fallback / song song) ──
    if POWER_AUTOMATE_URL:
        payload = {
            "text": (
                f"Vancouver Jobs — {today_str}\n"
                f"Tìm được {total} jobs | Có lương: {has_salary}\n"
                f"Tải file: {download_url or 'N/A'}"
            ),
            "drive_url":  drive_folder_url or "",
            "file_url":   xlsx_file_url    or "",
        }
        try:
            resp = requests.post(POWER_AUTOMATE_URL,
                                 headers={"Content-Type": "application/json"},
                                 data=json.dumps(payload),
                                 timeout=30)
            if resp.status_code in (200, 202):
                log.info("✅ Đã gửi vào Teams (Power Automate).")
            else:
                log.error(f"❌ Power Automate lỗi {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.error(f"❌ Lỗi Power Automate: {e}")

    if not teams_webhook and not POWER_AUTOMATE_URL:
        log.warning("⚠️  Chưa cấu hình TEAMS_WEBHOOK_URL hoặc POWER_AUTOMATE_URL.")

# ─── SUMMARY ────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame):
    SEP = "═" * 62
    print(f"\n{SEP}")
    print(f" JOB CRAWLER {'[DEMO]' if DEMO_MODE else ''}")
    print(SEP)
    if df.empty:
        print("  ⚠️  Không có kết quả. Cấu hình JOB_PROXY và chạy lại.")
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
        for _, r in df.nlargest(5,"min_amount").iterrows():
            t = str(r.get("title",""))[:36]
            c = str(r.get("company_name",""))[:22]
            s = str(r.get("salary_display",""))
            print(f"    {t:<37} {c:<23} {s}")

    print(f"\n  File CSV: {OUTPUT_FILE.resolve()}")
    print(SEP + "\n")

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    # Mode đặc biệt: lấy OAuth2 token
    if GET_TOKEN_MODE:
        get_oauth2_token()
        return

    print("\n" + "═"*62)
    print(f"  Vancouver Job Crawler v3 {'[DEMO MODE]' if DEMO_MODE else ''}")
    print(f"  Sites  : {', '.join(SITES)}")
    print(f"  Ngày   : {DAYS_OLD} ngày gần nhất")
    print(f"  Lương  : >= ${MIN_ANNUAL:,}/yr  hoặc  ${MIN_HOURLY}/hr")
    print(f"  Proxy  : {'OK ' + str(len(PROXIES)) + ' proxy' if PROXIES else 'chưa cấu hình'}")
    print(f"  P.Auto : {'OK' if POWER_AUTOMATE_URL else 'chưa cấu hình'}")

    # Kiểm tra Drive credentials
    drive_ready = all([GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET,
                       GDRIVE_REFRESH_TOKEN, GDRIVE_FOLDER_ID])
    if drive_ready:
        print("  GDrive : ✅ OAuth2 personal Gmail")
    else:
        missing = [n for n,v in [
            ("CLIENT_ID",GDRIVE_CLIENT_ID),("CLIENT_SECRET",GDRIVE_CLIENT_SECRET),
            ("REFRESH_TOKEN",GDRIVE_REFRESH_TOKEN),("FOLDER_ID",GDRIVE_FOLDER_ID)
        ] if not v]
        print(f"  GDrive : ⚠️  Thiếu: {', '.join(missing)}")
    print("═"*62 + "\n")

    raw_df      = make_demo_data() if DEMO_MODE else crawl_jobs()
    if raw_df.empty:
        print_summary(raw_df); return

    norm_df     = normalize(raw_df)
    filtered_df = filter_jobs(norm_df)

    # Enrich salary cho các job còn N/A (chỉ chế độ thực, không demo)
    if not DEMO_MODE:
        filtered_df = enrich_salaries(filtered_df)
        # Recompute salary_display sau enrich
        filtered_df["salary_display"] = filtered_df.apply(_format_salary, axis=1)

    csv_path  = save_results(filtered_df)
    xlsx_path = _save_excel(filtered_df)
    print_summary(filtered_df)

    # Upload cả CSV và XLSX lên Drive
    drive_folder_url, _ = upload_to_drive(csv_path)          # upload CSV

    xlsx_file_url = None
    if xlsx_path:
        drive_folder_url2, xlsx_file_url = upload_to_drive(xlsx_path)  # upload XLSX
        if drive_folder_url2:
            drive_folder_url = drive_folder_url2  # cùng folder, lấy lại cho chắc

    if drive_folder_url:
        log.info(f"📁 Drive folder : {drive_folder_url}")
    if xlsx_file_url:
        log.info(f"📊 Excel file   : {xlsx_file_url}")

    send_teams_notification(
        filtered_df, csv_path,
        drive_folder_url=drive_folder_url,
        xlsx_file_url=xlsx_file_url,       # ← truyền link file xuống
    )

# ─── XUẤT EXCEL ─────────────────────────────────────────────────────────────

def _save_excel(df: pd.DataFrame) -> Path | None:
    """Xuất DataFrame ra .xlsx cùng tên với OUTPUT_FILE nhưng đuôi .xlsx."""
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
        out = out.sort_values(["min_amount","date_posted"],
                              ascending=[False,False], na_position="last")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Vancouver Jobs"

    # Header style
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

    # Data rows — highlight N/A salary in light red
    na_fill  = PatternFill("solid", fgColor="FFE0E0")
    sal_col  = cols.index("salary_display") + 1 if "salary_display" in cols else None

    for ri, row_data in enumerate(out.itertuples(index=False), 2):
        is_na = sal_col and str(getattr(row_data, "salary_display", "")) == "N/A"
        for ci, val in enumerate(row_data, 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.border = bdr
            cell.alignment = Alignment(vertical="center", wrap_text=False)
            if is_na and ci == sal_col:
                cell.fill = na_fill

    # Column widths
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


if __name__ == "__main__":
    main()