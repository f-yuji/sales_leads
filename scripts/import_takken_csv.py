from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import normalize_company_row, normalize_contact_row, normalize_license_row
from storage import create_store

BATCH_SIZE = 100
CENTER_LAT = 35.681236
CENTER_LON = 139.767125
SOURCE_TYPE = "mlit_takken"
SOURCE_URL = "https://etsuran2.mlit.go.jp/TAKKEN/"

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

EXCLUDED_KEYWORDS = {
    "銀行", "信託銀行", "信用金庫", "信用組合", "労働金庫", "農業協同組合", "漁業協同組合",
    "協同組合", "証券", "生命保険", "損害保険", "保険", "共済", "独立行政法人", "地方公共団体",
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
        ward = next((w for w in TOKYO_23_WARDS if rest.startswith(w)), None)
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


def in_target_area(row: dict[str, str]) -> bool:
    prefecture = (row.get("都道府県") or "").strip()
    address = (row.get("住所") or "").strip()
    city, ward = extract_municipality(prefecture, address)
    allowed = TARGET_MUNICIPALITIES.get(prefecture, set())
    return bool(city and (city in allowed or ward in allowed))


def is_excluded(row: dict[str, str]) -> bool:
    target = " ".join([
        (row.get("商号") or row.get("会社名") or "").strip(),
        (row.get("名称") or "").strip(),
    ])
    return any(kw in target for kw in EXCLUDED_KEYWORDS)


def convert_row(row: dict[str, str], source_name: str) -> dict[str, str]:
    prefecture = (row.get("都道府県") or "").strip()
    address = (row.get("住所") or "").strip()
    city, ward = extract_municipality(prefecture, address)
    converted = dict(row)
    converted["source_type"] = SOURCE_TYPE
    converted["source_name"] = source_name
    converted["source_url"] = SOURCE_URL
    converted["city"] = city or ""
    converted["ward"] = ward or ""
    # 免許番号を結合して license_no に入れる
    takken_id = (row.get("宅建ID") or "").strip()
    license_no = (row.get("免許") or row.get("免許番号") or "").strip()
    office_name = (row.get("名称") or "").strip()
    converted["免許番号"] = " / ".join(p for p in [takken_id, license_no, office_name] if p)
    converted["source_record_id"] = takken_id or license_no
    converted["primary_business_category"] = "real_estate"
    converted["業種"] = "real_estate"
    return converted


def main() -> None:
    parser = argparse.ArgumentParser(description="Import MLIT takken CSV (50km area).")
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--apply", action="store_true", help="実際にDBへ書き込む（省略時はdry-run）")
    parser.add_argument("--limit", type=int, default=0, help="書き込み件数上限（テスト用）")
    parser.add_argument("--mark-missing", action="store_true",
                        help="今回のCSVに含まれなかった既存宅建レコードを needs_review=true にする")
    args = parser.parse_args()

    source_name = args.csv_path.name
    text = decode_csv(args.csv_path)
    reader = list(csv.DictReader(text.splitlines()))

    store = create_store() if args.apply else None

    total = 0
    in_area = 0
    excluded = 0
    created = 0
    updated = 0
    skipped = 0
    touched_ids: list = []

    for raw in reader:
        total += 1
        if not in_target_area(raw):
            continue
        in_area += 1
        if is_excluded(raw):
            excluded += 1
            continue

        converted = convert_row(raw, source_name)
        company = normalize_company_row(converted, CENTER_LAT, CENTER_LON)
        license_data = normalize_license_row(converted)
        contact = normalize_contact_row(converted)

        if not company.get("company_name"):
            skipped += 1
            continue

        if not args.apply:
            continue

        if args.limit and (created + updated) >= args.limit:
            continue

        try:
            company_id, was_created = store.upsert_company(company, license=license_data, contact=contact)
            touched_ids.append(company_id)
            if was_created:
                created += 1
            else:
                updated += 1
        except Exception as e:
            print(f"  ERROR: {company.get('company_name')} → {e}", file=sys.stderr)
            skipped += 1

        if (created + updated) % 100 == 0:
            print(f"  進捗: 新規{created} 更新{updated} スキップ{skipped}")

    print(f"\n=== 結果 ===")
    print(f"CSVレコード総数:     {total}")
    print(f"対象エリア内:        {in_area}")
    print(f"除外（金融機関等）:  {excluded}")
    print(f"インポート対象:      {in_area - excluded}")

    if args.apply:
        print(f"新規登録:            {created}")
        print(f"更新:                {updated}")
        print(f"スキップ:            {skipped}")

        if args.mark_missing and touched_ids:
            _mark_missing_as_needs_review(store, touched_ids, source_name)
    else:
        print("（dry-run: --apply を付けると実際に書き込みます）")


def _mark_missing_as_needs_review(store, touched_ids: list, source_name: str) -> None:
    """今回のCSVに含まれなかった既存の宅建レコードを needs_review にする"""
    print("\n未検出レコードをチェック中...")
    from storage import SQLiteStore, SupabaseStore
    from services import now_iso

    if isinstance(store, SQLiteStore):
        import sqlite3
        touched_set = set(str(i) for i in touched_ids)
        with store.connect() as conn:
            existing = conn.execute(
                "select id from companies where source_type = ? and is_active = 1",
                (SOURCE_TYPE,)
            ).fetchall()
            missing = [r["id"] for r in existing if str(r["id"]) not in touched_set]
            if missing:
                conn.execute(
                    f"update companies set needs_review = 1, update_note = ? "
                    f"where id in ({','.join('?' for _ in missing)})",
                    [f"再取込時未検出 ({source_name})"] + missing,
                )
                print(f"needs_review にした件数: {len(missing)}")
            else:
                print("未検出レコードなし")

    elif isinstance(store, SupabaseStore):
        touched_set = set(str(i) for i in touched_ids)
        existing = store.client.table("companies").select("id").eq("source_type", SOURCE_TYPE).eq("is_active", True).execute()
        missing = [r["id"] for r in (existing.data or []) if r["id"] not in touched_set]
        if missing:
            chunk = 500
            for start in range(0, len(missing), chunk):
                ids = missing[start:start + chunk]
                store.client.table("companies").update({
                    "needs_review": True,
                    "update_note": f"再取込時未検出 ({source_name})",
                }).in_("id", ids).execute()
            print(f"needs_review にした件数: {len(missing)}")
        else:
            print("未検出レコードなし")


if __name__ == "__main__":
    main()
