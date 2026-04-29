from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from import_takken_csv import is_excluded_text
from storage import SQLiteStore, SupabaseStore, create_store


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
    parser = argparse.ArgumentParser(description="Remove obvious non-target companies such as banks from the store.")
    parser.add_argument("--apply", action="store_true", help="Actually delete matching rows.")
    args = parser.parse_args()

    store = create_store()
    rows = store.list_companies(limit=100000)
    targets = [
        row for row in rows
        if row.get("source_type") == "mlit_takken" and is_excluded_text(row.get("company_name"), row.get("office_type"))
    ]
    print(f"store={store.name}")
    print(f"total_rows={len(rows)}")
    print(f"excluded_rows={len(targets)}")
    for row in targets[:20]:
        print(f"- {row.get('id')} {row.get('company_name')} {row.get('office_type') or ''}")
    if len(targets) > 20:
        print(f"... and {len(targets) - 20} more")
    if not args.apply:
        print("dry_run=true")
        return
    ids = [row["id"] for row in targets]
    if not ids:
        print("deleted=0")
        return
    if isinstance(store, SQLiteStore):
        delete_sqlite(store, ids)
    else:
        delete_supabase(store, ids)
    print(f"deleted={len(ids)}")


if __name__ == "__main__":
    main()
