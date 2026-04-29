from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from import_takken_csv import normalize_company_name_key
from storage import SQLiteStore, SupabaseStore, create_store


def score(row: dict) -> tuple[int, int, int, str]:
    office_type = row.get("office_type") or ""
    has_contact = 1 if row.get("email") or row.get("contact_form_url") or row.get("website_url") else 0
    has_tel = 1 if row.get("tel") else 0
    is_main = 1 if office_type == "主" or "本店" in str(row.get("license_no") or "") else 0
    return (is_main, has_contact, has_tel, str(row.get("imported_at") or ""))


def delete_sqlite(store: SQLiteStore, ids: list[int]) -> None:
    with store.connect() as conn:
        placeholders = ",".join("?" for _ in ids)
        conn.execute(f"delete from company_contacts where company_id in ({placeholders})", ids)
        conn.execute(f"delete from sales_status where company_id in ({placeholders})", ids)
        conn.execute(f"delete from companies where id in ({placeholders})", ids)


def delete_supabase(store: SupabaseStore, ids: list[str]) -> None:
    store.client.table("company_contacts").delete().in_("company_id", ids).execute()
    store.client.table("sales_status").delete().in_("company_id", ids).execute()
    store.client.table("companies").delete().in_("id", ids).execute()


def main() -> None:
    parser = argparse.ArgumentParser(description="Keep one row per company name and delete branch duplicates.")
    parser.add_argument("--apply", action="store_true", help="Actually delete duplicate rows.")
    args = parser.parse_args()

    store = create_store()
    rows = store.list_companies(limit=100000)
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        key = normalize_company_name_key(row.get("company_name"))
        if key:
            groups[key].append(row)

    duplicate_groups = {key: group for key, group in groups.items() if len(group) > 1}
    delete_ids = []
    print(f"store={store.name}")
    print(f"total_rows={len(rows)}")
    print(f"duplicate_company_names={len(duplicate_groups)}")
    for group in list(duplicate_groups.values())[:20]:
        kept = max(group, key=score)
        removed = [row for row in group if row["id"] != kept["id"]]
        delete_ids.extend(row["id"] for row in removed)
        print(f"- keep {kept.get('company_name')} ({kept.get('office_type') or '-'}) delete {len(removed)}")
    for group in list(duplicate_groups.values())[20:]:
        kept = max(group, key=score)
        delete_ids.extend(row["id"] for row in group if row["id"] != kept["id"])
    print(f"duplicate_rows_to_delete={len(delete_ids)}")
    if not args.apply:
        print("dry_run=true")
        return
    if not delete_ids:
        print("deleted=0")
        return
    if isinstance(store, SQLiteStore):
        delete_sqlite(store, delete_ids)
    else:
        delete_supabase(store, delete_ids)
    print(f"deleted={len(delete_ids)}")


if __name__ == "__main__":
    main()
