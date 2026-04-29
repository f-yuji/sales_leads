from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import normalize_company_row, normalize_contact_row
from storage import SupabaseStore, create_store

SOURCE_UPDATED_AT = "2024-08-06T00:00:00+09:00"
BATCH_SIZE = 500
TOKYO_23_WARDS = {
    "千代田区", "中央区", "港区", "新宿区", "文京区", "台東区", "墨田区", "江東区", "品川区", "目黒区",
    "大田区", "世田谷区", "渋谷区", "中野区", "杉並区", "豊島区", "北区", "荒川区", "板橋区", "練馬区",
    "足立区", "葛飾区", "江戸川区",
}

TARGET_MUNICIPALITIES = {
    "東京都": TOKYO_23_WARDS | {
        "八王子市", "立川市", "武蔵野市", "三鷹市", "府中市", "昭島市", "調布市", "町田市", "小金井市",
        "小平市", "日野市", "東村山市", "国分寺市", "国立市", "福生市", "狛江市", "東大和市", "清瀬市",
        "東久留米市", "武蔵村山市", "多摩市", "稲城市", "羽村市", "あきる野市", "西東京市", "瑞穂町",
        "日の出町",
    },
    "神奈川県": {
        "横浜市", "川崎市", "相模原市", "横須賀市", "鎌倉市", "藤沢市", "逗子市", "三浦市", "大和市",
        "海老名市", "座間市", "綾瀬市", "厚木市", "茅ヶ崎市", "葉山町", "寒川町", "愛川町",
    },
    "埼玉県": {
        "さいたま市", "川越市", "川口市", "所沢市", "春日部市", "狭山市", "上尾市", "草加市", "越谷市",
        "蕨市", "戸田市", "入間市", "朝霞市", "志木市", "和光市", "新座市", "桶川市", "久喜市", "北本市",
        "八潮市", "富士見市", "三郷市", "蓮田市", "坂戸市", "幸手市", "鶴ヶ島市", "日高市", "吉川市",
        "ふじみ野市", "白岡市", "伊奈町", "三芳町", "毛呂山町", "越生町", "川島町", "宮代町", "杉戸町",
        "松伏町",
    },
    "千葉県": {
        "千葉市", "市川市", "船橋市", "木更津市", "松戸市", "野田市", "佐倉市", "習志野市", "柏市",
        "市原市", "流山市", "八千代市", "我孫子市", "鎌ケ谷市", "浦安市", "四街道市", "袖ケ浦市",
        "白井市", "印西市", "酒々井町",
    },
}

EXCLUDED_COMPANY_KEYWORDS = {
    "銀行",
    "信託銀行",
    "信用金庫",
    "信用組合",
    "労働金庫",
    "農業協同組合",
    "漁業協同組合",
    "協同組合",
    "証券",
    "生命保険",
    "損害保険",
    "保険",
    "共済",
    "独立行政法人",
    "地方公共団体",
}


def decode_csv(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "cp932", "utf-8"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def extract_municipality(prefecture: str, address: str) -> tuple[str | None, str | None]:
    rest = address.replace(prefecture, "", 1) if address.startswith(prefecture) else address
    if prefecture == "東京都":
        ward = next((ward for ward in TOKYO_23_WARDS if rest.startswith(ward)), None)
        if ward:
            return ward, ward
    match = re.match(r"(.+?[市区町村])", rest)
    if not match:
        return None, None
    municipality = match.group(1)
    if municipality.endswith("区") and prefecture != "東京都":
        city_match = re.match(r"(.+?市)", rest)
        if city_match:
            return city_match.group(1), municipality
    return municipality, None


def in_tokyo_station_50km(row: dict[str, str]) -> bool:
    prefecture = (row.get("都道府県") or "").strip()
    address = (row.get("住所") or "").strip()
    city, ward = extract_municipality(prefecture, address)
    allowed = TARGET_MUNICIPALITIES.get(prefecture, set())
    return bool(city and (city in allowed or ward in allowed))


def is_excluded_text(*parts: str | None) -> bool:
    target = " ".join((part or "").strip() for part in parts)
    return any(keyword in target for keyword in EXCLUDED_COMPANY_KEYWORDS)


def is_excluded_company(row: dict[str, str]) -> bool:
    return is_excluded_text(row.get("商号") or row.get("会社名"), row.get("名称"))


def normalize_company_name_key(name: str | None) -> str:
    normalized = unicodedata.normalize("NFKC", name or "")
    return re.sub(r"\s+", "", normalized)


def company_name_from_row(row: dict[str, str]) -> str:
    return (row.get("商号") or row.get("会社名") or row.get("company_name") or "").strip()


def convert_row(row: dict[str, str]) -> dict[str, str]:
    prefecture = (row.get("都道府県") or "").strip()
    address = (row.get("住所") or "").strip()
    city, ward = extract_municipality(prefecture, address)
    converted = dict(row)
    converted["業種"] = "宅建業"
    converted["source_type"] = "mlit_takken"
    converted["city"] = city or ""
    converted["ward"] = ward or ""
    takken_id = (row.get("宅建ID") or "").strip()
    license_no = (row.get("免許") or "").strip()
    office_name = (row.get("名称") or "").strip()
    converted["免許"] = " / ".join(part for part in [takken_id, license_no, office_name] if part)
    return converted


def build_company(row: dict[str, str]) -> dict:
    company = normalize_company_row(convert_row(row), 35.681236, 139.767125)
    company["business_category"] = "real_estate"
    company["source_type"] = "mlit_takken"
    company["source_updated_at"] = SOURCE_UPDATED_AT
    company["source_url"] = "https://etsuran2.mlit.go.jp/TAKKEN/"
    return company


def insert_supabase_batches(store: SupabaseStore, companies: list[dict]) -> tuple[int, int]:
    created = 0
    status_created = 0
    for start in range(0, len(companies), BATCH_SIZE):
        batch = companies[start:start + BATCH_SIZE]
        result = store.client.table("companies").insert(batch).execute()
        inserted = result.data or []
        created += len(inserted)
        status_rows = [
            {"company_id": row["id"], "status": "未対応"}
            for row in inserted
            if row.get("id")
        ]
        if status_rows:
            store.client.table("sales_status").insert(status_rows).execute()
            status_created += len(status_rows)
        print(f"inserted={created}/{len(companies)}")
    return created, status_created


def main() -> None:
    parser = argparse.ArgumentParser(description="Import MLIT takken CSV for Tokyo-station 50km area.")
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--apply", action="store_true", help="Actually write to the configured store.")
    parser.add_argument("--limit", type=int, default=0, help="Limit writes for trial imports.")
    args = parser.parse_args()

    text = decode_csv(args.csv_path)
    reader = csv.DictReader(text.splitlines())
    total = 0
    target = 0
    excluded = 0
    duplicate_names = 0
    created = 0
    updated = 0
    skipped = 0
    seen_company_names: set[str] = set()
    store = create_store() if args.apply else None
    existing_company_names: set[str] = set()
    bulk_companies: list[dict] = []
    if args.apply and isinstance(store, SupabaseStore):
        existing_company_names = {
            normalize_company_name_key(row.get("company_name"))
            for row in store.list_companies(limit=100000)
            if row.get("company_name")
        }

    for raw in reader:
        total += 1
        if not in_tokyo_station_50km(raw):
            continue
        target += 1
        if is_excluded_company(raw):
            excluded += 1
            continue
        name_key = normalize_company_name_key(company_name_from_row(raw))
        if not name_key:
            skipped += 1
            continue
        if name_key in seen_company_names:
            duplicate_names += 1
            continue
        if name_key in existing_company_names:
            updated += 1
            seen_company_names.add(name_key)
            continue
        seen_company_names.add(name_key)
        if not args.apply:
            continue
        if args.limit and created + updated >= args.limit:
            continue
        company = build_company(raw)
        contact = normalize_contact_row(raw)
        if not company.get("company_name"):
            skipped += 1
            continue
        if isinstance(store, SupabaseStore):
            bulk_companies.append(company)
            created += 1
            continue
        _, was_created = store.upsert_company(company, contact)  # type: ignore[union-attr]
        if was_created:
            created += 1
        else:
            updated += 1

    print(f"total={total}")
    print(f"tokyo_station_50km_candidates={target}")
    print(f"excluded_non_targets={excluded}")
    print(f"duplicate_names_skipped={duplicate_names}")
    print(f"import_candidates={target - excluded - duplicate_names}")
    if args.apply:
        if isinstance(store, SupabaseStore) and bulk_companies:
            created, _ = insert_supabase_batches(store, bulk_companies)
        print(f"created={created}")
        print(f"updated={updated}")
        print(f"skipped={skipped}")
    else:
        print("dry_run=true")


if __name__ == "__main__":
    main()
