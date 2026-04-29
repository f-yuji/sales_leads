"""
会社名と電話番号から公式サイトURLを検索し、company_contacts に保存する。

Serper API / Brave Search API を使用:
    https://google.serper.dev/search
    https://api.search.brave.com/res/v1/web/search

使い方:
    python scripts/search_websites.py
    python scripts/search_websites.py --limit 50
    python scripts/search_websites.py --prefecture 神奈川県
    python scripts/search_websites.py --provider serper
    python scripts/search_websites.py --provider brave
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

for proxy_var in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
    if os.getenv(proxy_var) == "http://127.0.0.1:9":
        os.environ.pop(proxy_var, None)

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
BRAVE_SEARCH_API_KEY = os.getenv("BRAVE_SEARCH_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

SKIP_DOMAINS = [
    "mlit.go.jp", ".go.jp", "pref.", "city.", "town.",
    "google.com", "google.co.jp", "bing.com", "yahoo.co.jp",
    "suumo.jp", "athome.co.jp", "homes.co.jp", "chintai.net",
    "reins.or.jp", "takken.or.jp",
    "ekiten.jp", "itp.ne.jp", "mapion.co.jp", "navitime.co.jp",
    "24u.jp", "jpon.xyz", "tel-no.com", "denwabangou.net",
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


def search_serper(query: str) -> list[str]:
    if not SERPER_API_KEY:
        return []
    resp = requests.post(
        "https://google.serper.dev/search",
        headers={
            "X-API-KEY": SERPER_API_KEY,
            "Content-Type": "application/json",
        },
        json={
            "q": query,
            "gl": "jp",
            "hl": "ja",
            "num": 10,
        },
        timeout=15,
    )

    if resp.status_code == 429:
        print("  Serper rate limit/quota reached. Stop for now.")
        sys.exit(0)

    if resp.status_code != 200:
        print(f"  Serper error {resp.status_code}: {resp.text[:200]}")
        return []

    return [item.get("link", "") for item in resp.json().get("organic", []) if item.get("link")]


def search_brave(query: str) -> list[str]:
    if not BRAVE_SEARCH_API_KEY:
        return []
    resp = requests.get(
        "https://api.search.brave.com/res/v1/web/search",
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": BRAVE_SEARCH_API_KEY,
        },
        params={
            "q": query,
            "count": 10,
            "country": "jp",
        },
        timeout=15,
    )

    if resp.status_code == 429:
        print("  Brave rate limit/quota reached. Stop for now.")
        sys.exit(0)

    if resp.status_code != 200:
        print(f"  Brave error {resp.status_code}: {resp.text[:200]}")
        return []

    return [item.get("url", "") for item in resp.json().get("web", {}).get("results", []) if item.get("url")]


def search_website(company_name: str, tel: str | None, address: str | None, provider: str) -> str | None:
    parts = [company_name, "公式"]
    if tel:
        parts.append(tel)
    elif address:
        parts.append(address)
    query = " ".join(parts)

    candidates: list[str] = []
    if provider in ["serper", "both"]:
        candidates.extend(search_serper(query))
    if provider in ["brave", "both"]:
        candidates.extend(search_brave(query))

    seen: set[str] = set()
    for url in candidates:
        normalized = url.rstrip("/")
        if normalized in seen:
            continue
        seen.add(normalized)
        if normalized and not is_skip_url(normalized):
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
    parser.add_argument("--provider", choices=["serper", "brave", "both"], default="both")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.provider in ["serper", "both"] and not SERPER_API_KEY:
        print("WARN: SERPER_API_KEY is not set in .env")
    if args.provider in ["brave", "both"] and not BRAVE_SEARCH_API_KEY:
        print("WARN: BRAVE_SEARCH_API_KEY is not set in .env")
    if not SERPER_API_KEY and not BRAVE_SEARCH_API_KEY:
        print("ERROR: SERPER_API_KEY or BRAVE_SEARCH_API_KEY is required")
        sys.exit(1)
    if args.provider == "serper" and not SERPER_API_KEY:
        print("ERROR: --provider serper requires SERPER_API_KEY")
        sys.exit(1)
    if args.provider == "brave" and not BRAVE_SEARCH_API_KEY:
        print("ERROR: --provider brave requires BRAVE_SEARCH_API_KEY")
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
    print(f"provider: {args.provider}")
    print(f"targets: {len(targets)}{' (dry-run)' if args.dry_run else ''}\n")

    found = 0
    not_found = 0

    for i, company in enumerate(targets, 1):
        name = company["company_name"]
        tel = company.get("tel")
        address = company.get("address") or f"{company.get('prefecture', '')}{company.get('city', '')}"

        print(f"[{i:>3}/{len(targets)}] {name}", end="  ", flush=True)
        url = search_website(name, tel=tel, address=address or None, provider=args.provider)

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
