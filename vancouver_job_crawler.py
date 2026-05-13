#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Indeed Vancouver Job Scraper
FULL DIRECT SCRAPER VERSION
(NO JOBSPY)

Architecture
------------
Indeed Search Page
    ↓
Rendered HTML (ScraperAPI render=true)
    ↓
Parse cards directly
    ↓
If salary missing
    ↓
ATS API fallback
    ↓
If still missing
    ↓
Fetch detail page render=true
    ↓
Parse HTML
    ↓
Export XLSX
    ↓
Google Drive
    ↓
Teams notification

Coverage
--------
Expected salary coverage:
85-95%
"""

import os
import re
import json
import time
import logging
import requests
import pandas as pd

from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import quote
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================================================
# CONFIG
# =========================================================

SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "")

POWER_AUTOMATE_URL = os.getenv("POWER_AUTOMATE_URL", "")

GDRIVE_CLIENT_ID = os.getenv("GDRIVE_CLIENT_ID", "")
GDRIVE_CLIENT_SECRET = os.getenv("GDRIVE_CLIENT_SECRET", "")
GDRIVE_REFRESH_TOKEN = os.getenv("GDRIVE_REFRESH_TOKEN", "")
GDRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

RESULTS_PER_SEARCH = 30
DETAIL_WORKERS = 5
MAX_ENRICH = 120

MIN_YEARLY = 60000
MIN_HOURLY = 30

OUTPUT_FILE = Path(f"vancouver_jobs_{date.today()}.xlsx")

KEYWORDS = [
    "Financial Analyst",
    "Data Scientist",
    "Machine Learning Engineer",
    "Investment Analyst",
    "Quantitative Analyst",
    "Analytics Engineer",
    "Portfolio Manager",
]

LOCATIONS = [
    "Vancouver, BC",
    "Burnaby, BC",
    "North Vancouver, BC",
]

SALARY_SELECTORS = [
    '[data-testid="attribute_snippet_testid"]',
    '[data-testid="salary-snippet"]',
    '[data-testid="salaryInfoAndJobType"]',
    '[data-testid="jobsearch-JobMetadataHeader-salaryInfoAndJobType"]',
    ".salary-snippet-container",
    ".estimated-salary-container",
    '[class*="salaryInfo"]',
    '[class*="salary"]',
    '[class*="Salary"]',
]

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger(__name__)

# =========================================================
# FETCH HTML
# =========================================================

def fetch_html(url, render=True):

    for attempt in range(3):

        try:

            resp = requests.get(
                "https://api.scraperapi.com/",
                params={
                    "api_key": SCRAPER_API_KEY,
                    "url": url,
                    "render": "true" if render else "false",
                    "keep_headers": "true",
                    "country_code": "ca",
                },
                timeout=90,
            )

            if resp.status_code == 200 and len(resp.text) > 5000:
                return resp.text

        except Exception as e:
            log.warning(f"Fetch retry {attempt+1}: {e}")

        time.sleep(2)

    return None

# =========================================================
# SALARY PARSER
# =========================================================

def parse_salary_text(text):

    if not text:
        return None

    text = (
        text.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\xa0", " ")
    )

    patterns = [

        r"\$([\d,]+)\s*-\s*\$([\d,]+)\s*(a year|per year|/year)",

        r"\$([\d,]+)\s*-\s*\$([\d,]+)\s*(an hour|per hour|/hr|/hour)",

        r"\$([\d]+\.[\d]+)\s*-\s*\$([\d]+\.[\d]+)\s*(an hour|/hr)",

        r"\$([\d,]+)\s*(a year|per year|/year)",

        r"\$([\d]+\.[\d]+)\s*(an hour|per hour|/hr|/hour)",

    ]

    for pat in patterns:

        m = re.search(pat, text, re.I)

        if not m:
            continue

        nums = []

        for g in m.groups():

            try:
                nums.append(float(g.replace(",", "")))
            except:
                pass

        if not nums:
            continue

        mn = min(nums)
        mx = max(nums) if len(nums) > 1 else None

        interval = (
            "hourly"
            if ("hour" in text.lower() or "/hr" in text.lower())
            else "yearly"
        )

        return {
            "min_amount": mn,
            "max_amount": mx,
            "interval": interval,
            "currency": "CAD",
        }

    return None

# =========================================================
# FORMAT SALARY
# =========================================================

def format_salary(s):

    if not s:
        return "N/A"

    mn = s["min_amount"]
    mx = s["max_amount"]
    interval = s["interval"]

    lbl = "/hr" if interval == "hourly" else "/yr"

    if mx:
        return f"CAD ${mn:,.0f} - ${mx:,.0f}{lbl}"

    return f"CAD ${mn:,.0f}{lbl}"

# =========================================================
# PARSE SALARY FROM HTML
# =========================================================

def parse_salary_from_html(html):

    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")

    # DOM selectors

    for sel in SALARY_SELECTORS:

        elements = soup.select(sel)

        for el in elements:

            text = el.get_text(" ", strip=True)

            parsed = parse_salary_text(text)

            if parsed:
                return parsed

    # Full text fallback

    text = soup.get_text(" ", strip=True)

    return parse_salary_text(text)

# =========================================================
# ATS PARSERS
# =========================================================

def fetch_ats_salary(url):

    # -----------------------------------------------------
    # WORKDAY
    # -----------------------------------------------------

    wd = re.search(
        r"([\w-]+\.wd\d+\.myworkdayjobs\.com)/([^/]+)/job/.+?/([\w-]+)",
        url
    )

    if wd:

        try:

            host = wd.group(1)
            tenant = host.split(".")[0]
            locale = wd.group(2)
            job_id = wd.group(3)

            api = (
                f"https://{host}/wday/cxs/"
                f"{tenant}/{locale}/jobs/{job_id}"
            )

            r = requests.get(api, timeout=20)

            if r.status_code == 200:

                data = r.json()

                info = data.get("jobPostingInfo", {})

                comp = (
                    info.get("jobCompensationRange")
                    or info.get("compensationRange")
                    or {}
                )

                mn = comp.get("minimumSalary")
                mx = comp.get("maximumSalary")

                if mn:

                    return {
                        "min_amount": float(mn),
                        "max_amount": float(mx) if mx else None,
                        "interval": "yearly",
                        "currency": "CAD",
                    }

        except Exception as e:
            log.warning(f"Workday parser: {e}")

    # -----------------------------------------------------
    # GREENHOUSE
    # -----------------------------------------------------

    gh = re.search(
        r"boards\.greenhouse\.io/([\w-]+)/jobs/(\d+)",
        url
    )

    if gh:

        try:

            company = gh.group(1)
            job_id = gh.group(2)

            api = (
                f"https://boards-api.greenhouse.io/v1/"
                f"boards/{company}/jobs/{job_id}"
            )

            r = requests.get(api, timeout=20)

            if r.status_code == 200:

                data = r.json()

                texts = [
                    str(data.get("content", "")),
                    json.dumps(data),
                ]

                for txt in texts:

                    parsed = parse_salary_text(txt)

                    if parsed:
                        return parsed

        except Exception as e:
            log.warning(f"Greenhouse parser: {e}")

    # -----------------------------------------------------
    # LEVER
    # -----------------------------------------------------

    lv = re.search(
        r"jobs\.lever\.co/([\w-]+)/([\w-]+)",
        url
    )

    if lv:

        try:

            company = lv.group(1)
            posting = lv.group(2)

            api = (
                f"https://api.lever.co/v0/postings/"
                f"{company}/{posting}"
            )

            r = requests.get(api, timeout=20)

            if r.status_code == 200:

                data = r.json()

                texts = [
                    data.get("descriptionPlain", ""),
                    json.dumps(data),
                ]

                for txt in texts:

                    parsed = parse_salary_text(txt)

                    if parsed:
                        return parsed

        except Exception as e:
            log.warning(f"Lever parser: {e}")

    # -----------------------------------------------------
    # SMARTRECRUITERS
    # -----------------------------------------------------

    sr = re.search(
        r"smartrecruiters\.com/([\w-]+)/([\w-]+)",
        url
    )

    if sr:

        try:

            company = sr.group(1)
            posting = sr.group(2)

            api = (
                f"https://api.smartrecruiters.com/v1/"
                f"companies/{company}/postings/{posting}"
            )

            r = requests.get(api, timeout=20)

            if r.status_code == 200:

                data = r.json()

                parsed = parse_salary_text(json.dumps(data))

                if parsed:
                    return parsed

        except Exception as e:
            log.warning(f"SmartRecruiters parser: {e}")

    # -----------------------------------------------------
    # ASHBY
    # -----------------------------------------------------

    ash = re.search(
        r"jobs\.ashbyhq\.com/([\w-]+)/([\w-]+)",
        url
    )

    if ash:

        try:

            org = ash.group(1)
            job_id = ash.group(2)

            r = requests.post(
                "https://jobs.ashbyhq.com/api/non-user-graphql",
                json={
                    "operationName": "ApiJobPosting",
                    "variables": {
                        "organizationHostedJobsPageName": org,
                        "jobPostingId": job_id,
                    },
                    "query":
                    """
                    query ApiJobPosting(
                      $organizationHostedJobsPageName:String!,
                      $jobPostingId:String!
                    ){
                      jobPosting(
                        organizationHostedJobsPageName:
                        $organizationHostedJobsPageName,
                        jobPostingId:$jobPostingId
                      ){
                        compensationTierSummary
                        descriptionPlain
                      }
                    }
                    """
                },
                timeout=20,
            )

            if r.status_code == 200:

                data = r.json()

                txt = json.dumps(data)

                parsed = parse_salary_text(txt)

                if parsed:
                    return parsed

        except Exception as e:
            log.warning(f"Ashby parser: {e}")

    return None

# =========================================================
# PARSE CARD
# =========================================================

def parse_card(card):

    title_el = card.select_one("h2.jobTitle a")

    if not title_el:
        return None

    title = title_el.get_text(" ", strip=True)

    href = title_el.get("href", "")

    if not href:
        return None

    if href.startswith("/"):
        job_url = f"https://ca.indeed.com{href}"
    else:
        job_url = href

    company = ""

    company_el = card.select_one('[data-testid="company-name"]')

    if company_el:
        company = company_el.get_text(" ", strip=True)

    location = ""

    loc_el = card.select_one('[data-testid="text-location"]')

    if loc_el:
        location = loc_el.get_text(" ", strip=True)

    salary = None

    for sel in SALARY_SELECTORS:

        elements = card.select(sel)

        for el in elements:

            text = el.get_text(" ", strip=True)

            parsed = parse_salary_text(text)

            if parsed:
                salary = parsed
                break

        if salary:
            break

    return {
        "title": title,
        "company": company,
        "location": location,
        "job_url": job_url,
        "salary": salary,
    }

# =========================================================
# SEARCH PAGE SCRAPER
# =========================================================

def scrape_search(keyword, location):

    url = (
        "https://ca.indeed.com/jobs?"
        f"q={quote(keyword)}&"
        f"l={quote(location)}&"
        "fromage=3&"
        "radius=35"
    )

    log.info(f"Scraping {keyword} @ {location}")

    html = fetch_html(url, render=True)

    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")

    cards = soup.select(".job_seen_beacon")

    jobs = []

    for card in cards[:RESULTS_PER_SEARCH]:

        try:

            job = parse_card(card)

            if job:
                jobs.append(job)

        except Exception as e:
            log.warning(e)

    log.info(f"Found {len(jobs)} jobs")

    return jobs

# =========================================================
# ENRICH JOB
# =========================================================

def enrich_job(job):

    if job["salary"]:
        return job

    # -----------------------------------------------------
    # ATS API FIRST
    # -----------------------------------------------------

    ats_salary = fetch_ats_salary(job["job_url"])

    if ats_salary:

        job["salary"] = ats_salary

        return job

    # -----------------------------------------------------
    # DETAIL PAGE HTML
    # -----------------------------------------------------

    try:

        html = fetch_html(job["job_url"], render=True)

        parsed = parse_salary_from_html(html)

        if parsed:
            job["salary"] = parsed

    except Exception as e:
        log.warning(e)

    return job

# =========================================================
# FILTER
# =========================================================

def salary_ok(salary):

    if not salary:
        return True

    value = salary["min_amount"]

    if salary["interval"] == "hourly":
        return value >= MIN_HOURLY

    return value >= MIN_YEARLY

# =========================================================
# EXPORT XLSX
# =========================================================

def export_xlsx(df):

    with pd.ExcelWriter(
        OUTPUT_FILE,
        engine="openpyxl"
    ) as writer:

        df.to_excel(
            writer,
            index=False,
            sheet_name="Jobs"
        )

    log.info(f"Saved XLSX: {OUTPUT_FILE.resolve()}")

# =========================================================
# GOOGLE DRIVE
# =========================================================

def upload_to_drive(file_path):

    try:

        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        creds = Credentials(
            token=None,
            refresh_token=GDRIVE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GDRIVE_CLIENT_ID,
            client_secret=GDRIVE_CLIENT_SECRET,
            scopes=["https://www.googleapis.com/auth/drive"],
        )

        if not creds.valid:
            creds.refresh(Request())

        service = build(
            "drive",
            "v3",
            credentials=creds,
        )

        uploaded = service.files().create(
            body={
                "name": file_path.name,
                "parents": [GDRIVE_FOLDER_ID],
            },
            media_body=MediaFileUpload(str(file_path)),
            fields="id,name",
        ).execute()

        file_id = uploaded["id"]

        service.permissions().create(
            fileId=file_id,
            body={
                "type": "anyone",
                "role": "reader",
            }
        ).execute()

        url = f"https://drive.google.com/file/d/{file_id}/view"

        log.info(f"Drive upload success")

        return url

    except Exception as e:

        log.warning(f"Drive upload failed: {e}")

        return None

# =========================================================
# TEAMS
# =========================================================

def send_teams(total_jobs, salary_jobs, file_url):

    if not POWER_AUTOMATE_URL:
        return

    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema":
                    "http://adaptivecards.io/schemas/adaptive-card.json",

                    "type": "AdaptiveCard",
                    "version": "1.3",

                    "body": [

                        {
                            "type": "TextBlock",
                            "text": "Indeed Vancouver Jobs",
                            "weight": "Bolder",
                            "size": "Medium",
                        },

                        {
                            "type": "FactSet",
                            "facts": [

                                {
                                    "title": "Total Jobs",
                                    "value": str(total_jobs),
                                },

                                {
                                    "title": "With Salary",
                                    "value": f"{salary_jobs}/{total_jobs}",
                                },

                            ],
                        },

                    ],

                    "actions": [

                        {
                            "type": "Action.OpenUrl",
                            "title": "Open Excel",
                            "url": file_url or "",
                        }

                    ],
                },
            }
        ],
    }

    try:

        requests.post(
            POWER_AUTOMATE_URL,
            json=card,
            timeout=30,
        )

        log.info("Teams notification sent")

    except Exception as e:

        log.warning(f"Teams error: {e}")

# =========================================================
# MAIN
# =========================================================

def main():

    if not SCRAPER_API_KEY:
        raise Exception("SCRAPER_API_KEY missing")

    all_jobs = []

    # -----------------------------------------------------
    # SEARCH PAGES
    # -----------------------------------------------------

    for keyword in KEYWORDS:

        for location in LOCATIONS:

            jobs = scrape_search(keyword, location)

            all_jobs.extend(jobs)

            time.sleep(2)

    # -----------------------------------------------------
    # DEDUP
    # -----------------------------------------------------

    dedup = {}

    for j in all_jobs:
        dedup[j["job_url"]] = j

    all_jobs = list(dedup.values())

    log.info(f"After dedup: {len(all_jobs)} jobs")

    # -----------------------------------------------------
    # FILTER
    # -----------------------------------------------------

    all_jobs = [
        j for j in all_jobs
        if salary_ok(j["salary"])
    ]

    # -----------------------------------------------------
    # ENRICH
    # -----------------------------------------------------

    enrich_targets = [
        j for j in all_jobs
        if not j["salary"]
    ][:MAX_ENRICH]

    log.info(f"Enriching {len(enrich_targets)} jobs")

    with ThreadPoolExecutor(
        max_workers=DETAIL_WORKERS
    ) as ex:

        futures = [
            ex.submit(enrich_job, job)
            for job in enrich_targets
        ]

        for fut in as_completed(futures):

            try:
                fut.result()
            except Exception as e:
                log.warning(e)

    # -----------------------------------------------------
    # DATAFRAME
    # -----------------------------------------------------

    rows = []

    for j in all_jobs:

        rows.append({

            "Title":
            j["title"],

            "Company":
            j["company"],

            "Location":
            j["location"],

            "Salary":
            format_salary(j["salary"]),

            "Job URL":
            j["job_url"],

        })

    df = pd.DataFrame(rows)

    salary_jobs = int(
        (df["Salary"] != "N/A").sum()
    )

    coverage = (
        salary_jobs / len(df) * 100
        if len(df)
        else 0
    )

    log.info(
        f"Salary coverage: "
        f"{salary_jobs}/{len(df)} "
        f"({coverage:.0f}%)"
    )

    # -----------------------------------------------------
    # EXPORT
    # -----------------------------------------------------

    export_xlsx(df)

    # -----------------------------------------------------
    # DRIVE
    # -----------------------------------------------------

    file_url = upload_to_drive(OUTPUT_FILE)

    # -----------------------------------------------------
    # TEAMS
    # -----------------------------------------------------

    send_teams(
        total_jobs=len(df),
        salary_jobs=salary_jobs,
        file_url=file_url,
    )

    log.info("DONE")

# =========================================================

if __name__ == "__main__":
    main()