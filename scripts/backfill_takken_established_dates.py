from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from import_takken_csv import company_name_from_row, decode_csv, normalize_company_name_key
from services import parse_japanese_date
from storage import SupabaseStore, create_store


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill established_at from MLIT takken CSV.")
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    text = decode_csv(args.csv_path)
    founded_by_name: dict[str, tuple[str | None, str | None]] = {}
    for row in csv.DictReader(text.splitlines()):
        name_key = normalize_company_name_key(company_name_from_row(row))
        raw = (row.get("設立年月日") or "").strip()
        if name_key and raw and name_key not in founded_by_name:
            founded_by_name[name_key] = (parse_japanese_date(raw), raw)

    store = create_store()
    if not isinstance(store, SupabaseStore):
        raise SystemExit("Supabase only for this backfill script.")

    rows = store.list_companies(limit=100000)
    updates = []
    for company in rows:
        key = normalize_company_name_key(company.get("company_name"))
        established_at, established_raw = founded_by_name.get(key, (None, None))
        if established_raw:
            updates.append(
                {
                    "id": company["id"],
                    "established_at": established_at,
                    "established_raw": established_raw,
                }
            )

    print(f"matched={len(updates)}")
    if not args.apply:
        print("dry_run=true")
        return
    for start in range(0, len(updates), 500):
        batch = updates[start:start + 500]
        store.client.table("companies").upsert(batch).execute()
        print(f"updated={min(start + 500, len(updates))}/{len(updates)}")


if __name__ == "__main__":
    main()
