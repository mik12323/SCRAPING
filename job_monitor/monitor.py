import sys
import os
import json
import time
import re
import html
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'shared')))
from discord_bot import DiscordNotifier

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
SEEN_IDS_PATH = os.path.join(SCRIPT_DIR, "seen_ids.json")
BASE_URL = "https://www.onlinejobs.ph"
SEARCH_URL = f"{BASE_URL}/jobseekers/jobsearch"


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def load_seen_ids():
    if os.path.exists(SEEN_IDS_PATH):
        with open(SEEN_IDS_PATH, "r") as f:
            return json.load(f)
    return {}


def save_seen_ids(seen_ids):
    with open(SEEN_IDS_PATH, "w") as f:
        json.dump(seen_ids, f, indent=2)


def fetch_jobs(keyword):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    params = {"jobkeyword": keyword}

    try:
        resp = requests.get(SEARCH_URL, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] Failed to fetch jobs for '{keyword}': {e}")
        return []

    return parse_jobs(resp.text)


def parse_jobs(page_html):
    soup = BeautifulSoup(page_html, "html.parser")
    jobs = []

    job_cards = soup.select("a[href*='/jobseekers/job/']")

    for card in job_cards:
        href = card.get("href", "")
        if not href or href.count("/") < 3:
            continue

        job_id_match = re.search(r"-(\d+)$", href)
        if not job_id_match:
            continue

        job_id = job_id_match.group(1)
        job_url = BASE_URL + href if href.startswith("/") else href

        inner = card.select_one("div.jobpost-cat-box")
        if not inner:
            continue

        title_tag = inner.select_one("h4")
        if not title_tag:
            continue

        employment_type = ""
        badge = title_tag.select_one("span.badge")
        if badge:
            employment_type = badge.get_text(strip=True)
            badge.decompose()

        title = title_tag.get_text(strip=True)

        date_tag = inner.select_one("p[data-temp]")
        posted_date = ""
        if date_tag:
            posted_date = date_tag.get("data-temp", "")

        salary = ""
        salary_dd = inner.select_one("dl.row dd")
        if salary_dd:
            salary = salary_dd.get_text(strip=True)

        desc_div = inner.select_one("div.desc")
        description = ""
        if desc_div:
            description = desc_div.get_text(separator=" ", strip=True)

        tags = []
        tag_div = inner.select_one("div.job-tag")
        if tag_div:
            for tag_badge in tag_div.select("a.badge"):
                tag_text = tag_badge.get_text(strip=True)
                if tag_text:
                    tags.append(tag_text)

        jobs.append({
            "id": job_id,
            "title": html.unescape(title),
            "url": job_url,
            "posted_date": posted_date,
            "salary": html.unescape(salary) if salary else "Not specified",
            "description": html.unescape(description),
            "employment_type": employment_type,
            "tags": tags,
        })

    return jobs


def matches_keywords(job, keywords):
    text = f"{job['title']} {job['description']} {' '.join(job['tags'])}".lower()
    return any(kw.lower() in text for kw in keywords)


def build_embed(job, snippet_length):
    desc = job["description"][:snippet_length]
    if len(job["description"]) > snippet_length:
        desc += "..."

    fields = [
        {"name": "Salary", "value": job["salary"], "inline": True},
        {"name": "Type", "value": job["employment_type"] or "Not specified", "inline": True},
        {"name": "Posted", "value": job["posted_date"][:10] if job["posted_date"] else "Unknown", "inline": True},
    ]

    if job["tags"]:
        fields.append({"name": "Skills", "value": ", ".join(job["tags"][:5]), "inline": False})

    embed = {
        "title": job["title"],
        "url": job["url"],
        "description": desc,
        "color": 0x00CC66,
        "fields": fields,
        "footer": {"text": "OnlineJobs.ph Monitor"},
        "timestamp": datetime.utcnow().isoformat(),
    }

    return embed


def send_discord(webhook_url, job, snippet_length):
    embed = build_embed(job, snippet_length)

    payload = {
        "content": f"**New Job Found!** <@&{''}>",
        "embeds": [embed],
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 5)
            print(f"[RATE LIMIT] Waiting {retry_after}s...")
            time.sleep(retry_after)
            requests.post(webhook_url, json=payload, timeout=10)
        elif resp.status_code not in (200, 204):
            print(f"[ERROR] Discord webhook returned {resp.status_code}: {resp.text[:200]}")
    except requests.RequestException as e:
        print(f"[ERROR] Failed to send Discord notification: {e}")


def run_once(config):
    keywords = config["keywords"]
    webhook_url = config.get("discord_webhook") or os.getenv("ONLINEJOBS_HOOK", "")
    crawl_delay = config.get("crawl_delay_seconds", 5)
    snippet_length = config.get("description_snippet_length", 150)

    if not webhook_url:
        print("[ERROR] No Discord webhook configured. Set ONLINEJOBS_HOOK in .env or discord_webhook in config.json")
        return

    seen_ids = load_seen_ids()
    new_count = 0

    for i, keyword in enumerate(keywords):
        print(f"[SEARCH] Fetching jobs for '{keyword}'...")

        jobs = fetch_jobs(keyword)
        print(f"[INFO] Found {len(jobs)} jobs on page 1")

        matched = [j for j in jobs if matches_keywords(j, keywords)]
        print(f"[INFO] {len(matched)} jobs match your keywords")

        for job in matched:
            if job["id"] not in seen_ids:
                seen_ids[job["id"]] = {
                    "title": job["title"],
                    "keyword": keyword,
                    "found_at": datetime.now().isoformat(),
                }
                send_discord(webhook_url, job, snippet_length)
                new_count += 1
                print(f"[NEW] {job['title']} — {job['salary']}")

        if i < len(keywords) - 1:
            print(f"[WAIT] Sleeping {crawl_delay}s before next keyword...")
            time.sleep(crawl_delay)

    save_seen_ids(seen_ids)
    print(f"[DONE] {new_count} new jobs sent to Discord\n")
    return new_count


def main():
    config = load_config()
    interval = config.get("poll_interval_minutes", 10)

    print("=" * 50)
    print("OnlineJobs.ph Job Monitor")
    print(f"Keywords: {config['keywords']}")
    print(f"Polling every {interval} minute(s)")
    print("=" * 50)

    while True:
        try:
            run_once(config)
        except KeyboardInterrupt:
            print("\n[STOP] Monitor stopped by user")
            break
        except Exception as e:
            print(f"[ERROR] {e}")

        print(f"[WAIT] Next check in {interval} minute(s)...")
        try:
            time.sleep(interval * 60)
        except KeyboardInterrupt:
            print("\n[STOP] Monitor stopped by user")
            break


if __name__ == "__main__":
    main()
