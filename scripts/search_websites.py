"""
会社名と電話番号から公式サイトURLを検索し、company_contacts に保存する。

Serper API を使用:
    https://google.serper.dev/search

使い方:
    python scripts/search_websites.py
    python scripts/search_websites.py --limit 50
    python scripts/search_websites.py --prefecture 神奈川県
    python scripts/search_websites.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

SKIP_DOMAINS = [
    "mlit.go.jp", ".go.jp", "pref.", "city.", "town.",
    "google.com", "google.co.jp", "bing.com", "yahoo.co.jp",
    "suumo.jp", "athome.co.jp", "homes.co.jp", "chintai.net",
    "reins.or.jp", "takken.or.jp",
    "ekiten.jp", "itp.ne.jp", "mapion.co.jp", "navitime.co.jp",
    "hotpepper.jp", "tabelog.com", "gurunavi.com",
    "wikipedia.org", "wikidata.org",
    "facebook.com", "twitter.com", "x.com", "instagram.com",
    "youtube.com", "linkedin.com",
    "amazon.co.jp", "rakuten.co.jp",
    "mynavi.jp", "rikunabi.com", "indeed.com",
    "townwork.net", "hellowork.go.jp",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_skip_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return any(domain in host for domain in SKIP_DOMAINS)


def search_website(company_name: str, tel: str | None, address: str | None) -> str | None:
    parts = [company_name, "公式"]
    if tel:
        parts.append(tel)
    elif address:
        parts.append(address)

    resp = requests.post(
        "https://google.serper.dev/search",
        headers={
            "X-API-KEY": SERPER_API_KEY or "",
            "Content-Type": "application/json",
        },
        json={
            "q": " ".join(parts),
            "gl": "jp",
            "hl": "ja",
            "num": 10,
        },
        timeout=15,
    )

    if resp.status_code == 429:
        print("  API rate limit/quota reached. Stop for now.")
        sys.exit(0)

    if resp.status_code != 200:
        print(f"  API error {resp.status_code}: {resp.text[:200]}")
        return None

    for item in resp.json().get("organic", []):
        url = item.get("link", "")
        if url and not is_skip_url(url):
            return url

    return None


def upsert_contact(client, company_id: str, website_url: str | None) -> None:
    existing = (
        client.table("company_contacts")
        .select("id")
        .eq("company_id", company_id)
        .limit(1)
        .execute()
    )
    payload = {
        "company_id": company_id,
        "website_url": website_url,
        "source_url": website_url,
        "confidence": "low" if website_url else "invalid",
        "checked_at": now_iso(),
        "is_valid": bool(website_url),
    }
    if existing.data:
        client.table("company_contacts").update(payload).eq("id", existing.data[0]["id"]).execute()
    else:
        client.table("company_contacts").insert(payload).execute()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--prefecture", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not SERPER_API_KEY:
        print("ERROR: SERPER_API_KEY is not set in .env")
        sys.exit(1)
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: Supabase connection is not configured")
        sys.exit(1)

    from supabase import create_client

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    done_res = (
        client.table("company_contacts")
        .select("company_id")
        .not_.is_("checked_at", "null")
        .execute()
    )
    done_ids = {row["company_id"] for row in done_res.data or []}
    print(f"processed: {len(done_ids)}")

    query = client.table("companies").select("id,company_name,tel,prefecture,city,address")
    if args.prefecture:
        query = query.ilike("prefecture", f"%{args.prefecture}%")
    companies_res = query.limit(args.limit + len(done_ids)).execute()

    targets = [company for company in companies_res.data if company["id"] not in done_ids][: args.limit]
    print(f"targets: {len(targets)}{' (dry-run)' if args.dry_run else ''}\n")

    found = 0
    not_found = 0

    for i, company in enumerate(targets, 1):
        name = company["company_name"]
        tel = company.get("tel")
        address = company.get("address") or f"{company.get('prefecture', '')}{company.get('city', '')}"

        print(f"[{i:>3}/{len(targets)}] {name}", end="  ", flush=True)
        url = search_website(name, tel=tel, address=address or None)

        if url:
            print(f"-> {url}")
            found += 1
        else:
            print("-> not found")
            not_found += 1

        if not args.dry_run:
            upsert_contact(client, company["id"], url)

        time.sleep(0.2)

    print(f"\ndone: found={found} / not_found={not_found}")


if __name__ == "__main__":
    main()
