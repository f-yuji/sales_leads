"""
会社名＋電話番号で公式サイトURLを検索し company_contacts に保存。
Google Custom Search API 使用（無料枠 100件/日）。

使い方:
    python scripts/search_websites.py              # 100件処理
    python scripts/search_websites.py --limit 50
    python scripts/search_websites.py --prefecture 神奈川県
    python scripts/search_websites.py --dry-run    # DB保存なし（テスト用）
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

GOOGLE_API_KEY = os.getenv("GOOGLE_SEARCH_API_KEY")
GOOGLE_CX      = os.getenv("GOOGLE_SEARCH_CX")
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# ポータル・政府・SNS など除外するドメインの部分文字列
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
    return any(d in url for d in SKIP_DOMAINS)


def search_website(company_name: str, tel: str | None, address: str | None) -> str | None:
    parts = [company_name]
    if tel:
        parts.append(tel)
    elif address:
        parts.append(address)

    resp = requests.get(
        "https://www.googleapis.com/customsearch/v1",
        params={
            "key": GOOGLE_API_KEY,
            "cx":  GOOGLE_CX,
            "q":   " ".join(parts),
            "num": 5,
            "lr":  "lang_ja",
        },
        timeout=10,
    )

    if resp.status_code == 429:
        print("  ⚠ APIクォータ上限。今日はここまで。")
        sys.exit(0)

    if resp.status_code != 200:
        print(f"  API error {resp.status_code}: {resp.text[:120]}")
        return None

    for item in resp.json().get("items", []):
        url = item.get("link", "")
        if url and not is_skip_url(url):
            return url

    return None


def upsert_contact(client, company_id: int, website_url: str | None) -> None:
    existing = (
        client.table("company_contacts")
        .select("id")
        .eq("company_id", company_id)
        .limit(1)
        .execute()
    )
    payload = {
        "company_id":  company_id,
        "website_url": website_url,
        "source_url":  website_url,
        "confidence":  "low" if website_url else None,
        "checked_at":  now_iso(),
        "is_valid":    bool(website_url),
    }
    if existing.data:
        client.table("company_contacts").update(payload).eq("id", existing.data[0]["id"]).execute()
    else:
        client.table("company_contacts").insert(payload).execute()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",      type=int, default=100)
    parser.add_argument("--prefecture", type=str, default=None)
    parser.add_argument("--dry-run",    action="store_true")
    args = parser.parse_args()

    if not GOOGLE_API_KEY or not GOOGLE_CX:
        print("ERROR: GOOGLE_SEARCH_API_KEY / GOOGLE_SEARCH_CX が .env に未設定")
        sys.exit(1)
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: Supabase 接続情報が未設定")
        sys.exit(1)

    from supabase import create_client
    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # website_url 取得済みの company_id を除外
    done_res = (
        client.table("company_contacts")
        .select("company_id")
        .not_.is_("checked_at", "null")
        .execute()
    )
    done_ids = {row["company_id"] for row in done_res.data or []}
    print(f"処理済み: {len(done_ids)} 件")

    # 未処理の会社を取得
    q = client.table("companies").select("id,company_name,tel,prefecture,city")
    if args.prefecture:
        q = q.ilike("prefecture", f"%{args.prefecture}%")
    companies_res = q.limit(args.limit + len(done_ids)).execute()

    targets = [c for c in companies_res.data if c["id"] not in done_ids][: args.limit]
    print(f"今回処理: {len(targets)} 件{'（dry-run）' if args.dry_run else ''}\n")

    found = not_found = 0

    for i, company in enumerate(targets, 1):
        name    = company["company_name"]
        tel     = company.get("tel")
        address = f"{company.get('prefecture', '')}{company.get('city', '')}"

        print(f"[{i:>3}/{len(targets)}] {name}", end="  ", flush=True)

        url = search_website(name, tel=tel, address=address or None)

        if url:
            print(f"→ {url}")
            found += 1
        else:
            print("→ 見つからず")
            not_found += 1

        if not args.dry_run:
            upsert_contact(client, company["id"], url)

        time.sleep(0.3)  # API負荷対策

    print(f"\n完了: 取得 {found} 件 / 未取得 {not_found} 件")


if __name__ == "__main__":
    main()
