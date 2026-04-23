#!/usr/bin/env python3
"""
Vancouver Job Crawler
=====================
Crawl Indeed + Glassdoor: tài chính / data science / C-suite
Lọc: Vancouver BC area | min $60k/yr hoặc $30/hr | 3 ngày gần nhất
Tích hợp: Gửi báo cáo qua MS Teams Incoming Webhook

Cách chạy:
  pip install -r requirements.txt
  python3 vancouver_job_crawler.py        # Chạy thật
  python3 vancouver_job_crawler.py --demo # Demo không cần proxy

Biến môi trường (set trong GitHub Secrets):
  TEAMS_WEBHOOK_URL  — URL webhook kênh Teams
  JOB_PROXY          — "user:pass@host:port"  (tuỳ chọn)
  GITHUB_RUN_URL     — tự điền bởi workflow
  GITHUB_REPO        — "owner/repo"  (tự điền bởi workflow)
"""

import os, sys, time, logging, re, json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from jobspy import scrape_jobs

DEMO_MODE = "--demo" in sys.argv

# ─── CẤU HÌNH ───────────────────────────────────────────────────────────────

_proxy_env         = os.getenv("JOB_PROXY", "")
PROXIES: list[str] = [_proxy_env] if _proxy_env else []
TEAMS_WEBHOOK_URL  = os.getenv("TEAMS_WEBHOOK_URL", "")
GITHUB_RUN_URL     = os.getenv("GITHUB_RUN_URL", "")
GITHUB_REPO        = os.getenv("GITHUB_REPO", "")

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
                        all_frames.append(df)
                        log.info(f"  → {len(df)} jobs thô")
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
    try: return pd.isna(v)
    except: return False


def _resolve_salary_fields(row):
    """Waterfall: structured → compensation dict → text parse."""
    intv = _clean_interval(row.get("interval"))
    curr = _clean_currency(row.get("currency"), row.get("location"), row.get("site"))
    src  = row.get("salary_source")

    # 1. Structured columns
    mn = _to_number(row.get("min_amount"))
    mx = _to_number(row.get("max_amount"))
    if mn is not None or mx is not None:
        return mn, mx, intv, curr, src or "direct_data"

    # 2. compensation dict (jobspy >= 0.19)
    comp = row.get("compensation")
    if comp and isinstance(comp, dict):
        mn2 = _to_number(comp.get("min_amount") or comp.get("min"))
        mx2 = _to_number(comp.get("max_amount") or comp.get("max"))
        iv2 = _clean_interval(comp.get("interval") or comp.get("pay_period"))
        cu2 = _clean_currency(comp.get("currency"), row.get("location"), row.get("site"))
        if mn2 is not None or mx2 is not None:
            return mn2, mx2, iv2 or intv, cu2 or curr, "compensation_dict"

    # 3. Text parse (salary / salary_text / description[:2000])
    for field in ["salary", "salary_text"]:
        parsed = _extract_salary_from_text(row.get(field), curr)
        if parsed:
            mn3,mx3,iv3,cu3 = parsed
            return mn3, mx3, iv3 or intv, cu3 or curr, "text_parse"

    parsed = _extract_salary_from_text((row.get("description") or "")[:2000], curr)
    if parsed:
        mn3,mx3,iv3,cu3 = parsed
        return mn3, mx3, iv3 or intv, cu3 or curr, "desc_parse"

    return None, None, intv, curr, src


def _extract_salary_from_text(value, currency_hint="CAD"):
    text = _to_text(value)
    if not text: return None
    text = (text.replace("\u2013","-").replace("\u2014","-")
                .replace("\u2212","-").replace("\xa0"," "))

    # Pattern 1 — Range: $80,000 - $100,000 /yr  OR  $80K - $100K a year
    p1 = (r"(?P<currency>CAD|USD|C\$|US\$|\$)\s*"
          r"(?P<min>\d[\d,]*(?:\.\d+)?)\s*(?P<min_k>[kK]?)\s*"
          r"[-\u2013to ]+\s*(?:CAD|USD|C\$|US\$|\$)?\s*"
          r"(?P<max>\d[\d,]*(?:\.\d+)?)\s*(?P<max_k>[kK]?)\s*"
          r"(?P<interval>per\s*year|a\s*year|yr|yearly|annually|"
          r"per\s*hour|an\s*hour|hr|hourly|"
          r"per\s*month|a\s*month|monthly|"
          r"per\s*week|a\s*week|weekly|per\s*day|a\s*day|daily)?")

    # Pattern 2 — Single: $80,000/yr  $45/hr
    p2 = (r"(?P<currency>CAD|USD|C\$|US\$|\$)\s*"
          r"(?P<min>\d[\d,]*(?:\.\d+)?)\s*(?P<min_k>[kK]?)\s*[/\s]*"
          r"(?P<interval>per\s*year|a\s*year|yr|yearly|annually|"
          r"per\s*hour|an\s*hour|hr|hourly|"
          r"per\s*month|a\s*month|monthly|"
          r"per\s*week|a\s*week|weekly|per\s*day|a\s*day|daily)")

    # Pattern 3 — "Up to $X a year"
    p3 = (r"[Uu]p\s+to\s+(?P<currency>CAD|USD|C\$|US\$|\$)\s*"
          r"(?P<min>\d[\d,]*(?:\.\d+)?)\s*(?P<min_k>[kK]?)\s*"
          r"(?P<interval>per\s*year|a\s*year|yr|yearly|annually|"
          r"per\s*hour|an\s*hour|hr|hourly|per\s*month|a\s*month|monthly)?")

    # Pattern 4 — Numbers only: 80,000 – 120,000  (implied yearly)
    p4 = r"(?<!\d)(?P<min>\d{2,3},\d{3})\s*[-\u2013]\s*(?P<max>\d{2,3},\d{3})(?!\d)"

    for i, pattern in enumerate([p1, p2, p3, p4]):
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m: continue
        gd = m.groupdict()
        interval = _normalize_interval_text(gd.get("interval") or "")
        currency = _normalize_currency_text(gd.get("currency") or "") or currency_hint

        if i == 3:
            mn = _parse_amount(gd.get("min"), "")
            mx = _parse_amount(gd.get("max"), "")
            interval = interval or "yearly"
        elif gd.get("max"):
            mn = _parse_amount(gd.get("min"), gd.get("min_k",""))
            mx = _parse_amount(gd.get("max"), gd.get("max_k",""))
        else:
            mn = _parse_amount(gd.get("min"), gd.get("min_k",""))
            mx = None

        if mn and mn > 0:
            if mn < 300 and not interval: interval = "hourly"
            elif mn >= 1000 and not interval: interval = "yearly"
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
    lbl = str(text or "").strip().lower().replace(" ","")
    return {"peryear":"yearly","ayear":"yearly","yr":"yearly","yearly":"yearly","annually":"yearly",
            "perhour":"hourly","anhour":"hourly","hr":"hourly","hourly":"hourly",
            "permonth":"monthly","amonth":"monthly","monthly":"monthly",
            "perweek":"weekly","aweek":"weekly","weekly":"weekly",
            "perday":"daily","aday":"daily","daily":"daily"}.get(lbl)


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

# ─── LỌC ────────────────────────────────────────────────────────────────────

def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    n0 = len(df)

    df = df.drop_duplicates(subset=["job_url"], keep="first")
    log.info(f"Sau dedup URL     : {len(df)} (bỏ {n0 - len(df)})")

    df = df[df["location_str"].apply(lambda l: any(c in l.lower() for c in ALLOWED_CITIES) if l else False)]
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

# ─── MS TEAMS NOTIFICATION ──────────────────────────────────────────────────

def send_teams_notification(df: pd.DataFrame, csv_path: Path):
    if not TEAMS_WEBHOOK_URL:
        log.warning("TEAMS_WEBHOOK_URL chưa cấu hình — bỏ qua.")
        return

    today_str  = date.today().strftime("%d/%m/%Y")
    total      = len(df)
    has_salary = int(df["salary_display"].ne("N/A").sum()) if "salary_display" in df.columns else 0

    # Top 10 jobs table
    rows_md = []
    for _, r in df.head(10).iterrows():
        title   = str(r.get("title",""))[:40]
        company = str(r.get("company_name",""))[:25]
        salary  = str(r.get("salary_display","N/A"))
        loc     = str(r.get("location_str",""))[:20]
        url     = str(r.get("job_url",""))
        link    = f"[{title}]({url})" if url.startswith("http") else title
        rows_md.append(f"| {link} | {company} | {loc} | {salary} |")

    table = ("| Vị trí | Công ty | Địa điểm | Lương |\n"
             "|--------|---------|----------|-------|\n"
             + "\n".join(rows_md))

    kw_line = ""
    if "search_keyword" in df.columns and total > 0:
        top_kw  = df["search_keyword"].value_counts().head(5)
        kw_line = "**Top keywords:** " + "  ·  ".join(
            f"{k} ({v})" for k,v in top_kw.items()) + "\n\n"

    links = []
    if GITHUB_REPO:
        raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/data/{csv_path.name}"
        links.append(f"[📥 Download CSV]({raw_url})")
    if GITHUB_RUN_URL:
        links.append(f"[🔗 GitHub Actions Run]({GITHUB_RUN_URL})")

    body = (f"## 🏙️ Vancouver Jobs — {today_str}\n\n"
            f"**Tổng:** {total} jobs  |  "
            f"**Có lương:** {has_salary}  |  "
            f"**N/A lương:** {total - has_salary}\n\n"
            f"{kw_line}"
            f"### Top {min(10,total)} jobs\n\n"
            f"{table}\n\n"
            f"{'  |  '.join(links)}")

    payload = {
        "@type": "MessageCard", "@context": "http://schema.org/extensions",
        "themeColor": "0072C6",
        "summary": f"Vancouver Jobs {today_str} — {total} jobs",
        "sections": [{
            "activityTitle":    f"📋 Vancouver Job Crawler — {today_str}",
            "activitySubtitle": f"{total} jobs  |  {has_salary} có lương  |  {total-has_salary} N/A",
            "text": body, "markdown": True,
        }],
    }

    try:
        resp = requests.post(TEAMS_WEBHOOK_URL,
                             headers={"Content-Type":"application/json"},
                             data=json.dumps(payload), timeout=30)
        if resp.status_code == 200:
            log.info("✅ Đã gửi báo cáo lên MS Teams.")
        else:
            log.error(f"❌ Teams webhook lỗi {resp.status_code}: {resp.text[:300]}")
    except Exception as e:
        log.error(f"❌ Lỗi gửi Teams: {e}")

# ─── SUMMARY ────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame):
    SEP = "═" * 62
    print(f"\n{SEP}")
    print(f"  VANCOUVER JOB CRAWLER {'[DEMO]' if DEMO_MODE else ''}")
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
    pct = f"{has_salary/len(df)*100:.0f}%"
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

# ─── MAIN ───────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═"*62)
    print(f"  Vancouver Job Crawler {'[DEMO MODE]' if DEMO_MODE else ''}")
    print(f"  Sites  : {', '.join(SITES)}")
    print(f"  Ngày   : {DAYS_OLD} ngày gần nhất")
    print(f"  Lương  : >= ${MIN_ANNUAL:,}/yr  hoac  ${MIN_HOURLY}/hr")
    print(f"  Proxy  : {'OK ' + str(len(PROXIES)) + ' proxy' if PROXIES else 'chua cau hinh'}")
    print(f"  Teams  : {'OK' if TEAMS_WEBHOOK_URL else 'chua cau hinh'}")
    print("═"*62 + "\n")

    raw_df = make_demo_data() if DEMO_MODE else crawl_jobs()
    if raw_df.empty:
        print_summary(raw_df); return

    norm_df     = normalize(raw_df)
    filtered_df = filter_jobs(norm_df)
    csv_path    = save_results(filtered_df)
    print_summary(filtered_df)
    send_teams_notification(filtered_df, csv_path)

if __name__ == "__main__":
    main()
