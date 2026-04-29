from __future__ import annotations

import csv
import os
import sqlite3
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from services import SALES_STATUSES, now_iso, passes_filters

load_dotenv()

for proxy_var in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
    if os.getenv(proxy_var) == "http://127.0.0.1:9":
        os.environ.pop(proxy_var, None)

COMPANY_COLUMNS = [
    "company_name",
    "business_category",
    "source_type",
    "prefecture",
    "city",
    "ward",
    "address",
    "tel",
    "license_no",
    "permit_no",
    "representative",
    "office_type",
    "established_at",
    "established_raw",
    "latitude",
    "longitude",
    "distance_km",
    "source_url",
    "source_updated_at",
    "imported_at",
    "last_checked_at",
    "is_active",
]

EXPORT_COLUMNS = [
    "id",
    *COMPANY_COLUMNS,
    "website_url",
    "email",
    "contact_form_url",
    "confidence",
    "status",
    "last_contacted_at",
    "next_action_at",
    "sales_memo",
]

SORT_OPTIONS = {
    "imported_desc": ("imported_at", True),
    "name_asc": ("company_name", False),
    "name_desc": ("company_name", True),
    "established_asc": ("established_at", False),
    "established_desc": ("established_at", True),
}


def create_store() -> "SQLiteStore | SupabaseStore":
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if url and key:
        return SupabaseStore(url, key)
    return SQLiteStore(os.getenv("LOCAL_DB_PATH", "data/sales_leads.sqlite3"))


class SQLiteStore:
    name = "SQLite local"

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                create table if not exists companies (
                    id integer primary key autoincrement,
                    company_name text not null,
                    business_category text not null default 'unknown',
                    source_type text not null default 'csv_import',
                    prefecture text,
                    city text,
                    ward text,
                    address text,
                    tel text,
                    license_no text,
                    permit_no text,
                    representative text,
                    office_type text,
                    established_at text,
                    established_raw text,
                    latitude real,
                    longitude real,
                    distance_km real,
                    source_url text,
                    source_updated_at text,
                    imported_at text,
                    last_checked_at text,
                    is_active integer not null default 1
                );
                create table if not exists company_contacts (
                    id integer primary key autoincrement,
                    company_id integer not null,
                    website_url text,
                    email text,
                    contact_form_url text,
                    source_url text,
                    confidence text,
                    checked_at text,
                    is_valid integer not null default 1,
                    memo text,
                    foreign key(company_id) references companies(id)
                );
                create table if not exists sales_status (
                    id integer primary key autoincrement,
                    company_id integer not null unique,
                    status text not null default '未対応',
                    last_contacted_at text,
                    next_action_at text,
                    memo text,
                    created_at text not null,
                    updated_at text not null,
                    foreign key(company_id) references companies(id)
                );
                create index if not exists idx_companies_license on companies(license_no);
                create index if not exists idx_companies_permit on companies(permit_no);
                create index if not exists idx_companies_tel on companies(tel);
                create index if not exists idx_companies_location on companies(prefecture, city, ward);
                """
            )
            existing_columns = {row["name"] for row in conn.execute("pragma table_info(companies)").fetchall()}
            if "established_at" not in existing_columns:
                conn.execute("alter table companies add column established_at text")
            if "established_raw" not in existing_columns:
                conn.execute("alter table companies add column established_raw text")
            conn.execute("create index if not exists idx_companies_name on companies(company_name)")
            conn.execute("create index if not exists idx_companies_established on companies(established_at)")

    def _find_duplicate_id(self, row: dict[str, Any]) -> int | None:
        conditions = []
        params: list[Any] = []
        for key in ["license_no", "permit_no", "tel"]:
            if row.get(key):
                conditions.append(f"{key} = ?")
                params.append(row[key])
        if row.get("company_name") and row.get("address"):
            conditions.append("(company_name = ? and address = ?)")
            params.extend([row["company_name"], row["address"]])
        if row.get("company_name") and row.get("city"):
            conditions.append("(company_name = ? and city = ?)")
            params.extend([row["company_name"], row["city"]])
        if not conditions:
            return None
        with self.connect() as conn:
            found = conn.execute(f"select id from companies where {' or '.join(conditions)} limit 1", params).fetchone()
            return int(found["id"]) if found else None

    def upsert_company(self, company: dict[str, Any], contact: dict[str, Any] | None = None) -> tuple[int, bool]:
        if not company.get("company_name"):
            raise ValueError("company_name is required")
        existing_id = self._find_duplicate_id(company)
        with self.connect() as conn:
            if existing_id:
                assignments = ", ".join(f"{col} = coalesce(?, {col})" for col in COMPANY_COLUMNS if col != "imported_at")
                params = [company.get(col) for col in COMPANY_COLUMNS if col != "imported_at"]
                params.append(existing_id)
                conn.execute(f"update companies set {assignments} where id = ?", params)
                company_id = existing_id
                created = False
            else:
                cols = ", ".join(COMPANY_COLUMNS)
                placeholders = ", ".join("?" for _ in COMPANY_COLUMNS)
                params = [company.get(col) for col in COMPANY_COLUMNS]
                cursor = conn.execute(f"insert into companies ({cols}) values ({placeholders})", params)
                company_id = int(cursor.lastrowid)
                created = True
                conn.execute(
                    """
                    insert into sales_status (company_id, status, created_at, updated_at)
                    values (?, ?, ?, ?)
                    """,
                    (company_id, "未対応", now_iso(), now_iso()),
                )
            if contact:
                self.upsert_contact(conn, company_id, contact)
            return company_id, created

    def upsert_contact(self, conn: sqlite3.Connection, company_id: int, contact: dict[str, Any]) -> None:
        found = conn.execute("select id from company_contacts where company_id = ? limit 1", (company_id,)).fetchone()
        cols = ["website_url", "email", "contact_form_url", "source_url", "confidence", "checked_at", "is_valid", "memo"]
        if found:
            assignments = ", ".join(f"{col} = coalesce(?, {col})" for col in cols)
            conn.execute(f"update company_contacts set {assignments} where id = ?", [contact.get(col) for col in cols] + [found["id"]])
        else:
            conn.execute(
                f"insert into company_contacts (company_id, {', '.join(cols)}) values (?, {', '.join('?' for _ in cols)})",
                [company_id] + [contact.get(col) for col in cols],
            )

    def list_companies(
        self,
        filters: dict[str, Any] | None = None,
        limit: int = 500,
        offset: int = 0,
        sort: str = "imported_desc",
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            sort_column, sort_desc = SORT_OPTIONS.get(sort, SORT_OPTIONS["imported_desc"])
            rows = conn.execute(
                f"""
                select c.*, cc.website_url, cc.email, cc.contact_form_url, cc.confidence,
                       ss.status, ss.last_contacted_at, ss.next_action_at, ss.memo as sales_memo
                from companies c
                left join company_contacts cc on cc.company_id = c.id
                left join sales_status ss on ss.company_id = c.id
                order by c.{sort_column} {'desc' if sort_desc else 'asc'}, c.id desc
                limit ?
                offset ?
                """,
                (limit, offset),
            ).fetchall()
        data = [dict(row) for row in rows]
        if filters:
            data = [row for row in data if passes_filters(row, filters)]
        return data

    def update_status(self, company_id: str, status: str, memo: str | None, next_action_at: str | None) -> None:
        if status not in SALES_STATUSES:
            raise ValueError("invalid status")
        with self.connect() as conn:
            conn.execute(
                """
                insert into sales_status (company_id, status, memo, next_action_at, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?)
                on conflict(company_id) do update set
                    status = excluded.status,
                    memo = excluded.memo,
                    next_action_at = excluded.next_action_at,
                    updated_at = excluded.updated_at
                """,
                (company_id, status, memo, next_action_at, now_iso(), now_iso()),
            )

    def sidebar_stats(self) -> dict[str, Any]:
        with self.connect() as conn:
            agg = conn.execute(
                "select count(*) as total, max(imported_at) as last_updated from companies"
            ).fetchone()
            status_rows = conn.execute(
                """
                select coalesce(ss.status, '未対応') as status, count(*) as cnt
                from companies c
                left join sales_status ss on ss.company_id = c.id
                group by coalesce(ss.status, '未対応')
                """
            ).fetchall()
        last_updated = (agg["last_updated"] or "")[:16]
        status_counts = {r["status"]: r["cnt"] for r in status_rows}
        return {
            "db_name": self.db_path.name,
            "total": agg["total"],
            "last_updated": last_updated,
            "status_counts": status_counts,
        }

    def stats(self) -> dict[str, Any]:
        rows = self.list_companies(limit=100000)
        total = len(rows)
        email_count = sum(1 for row in rows if row.get("email"))
        form_count = sum(1 for row in rows if row.get("contact_form_url"))
        untouched = sum(1 for row in rows if row.get("status") == "未対応")
        sent = sum(1 for row in rows if row.get("status") == "送信済み")
        replied = sum(1 for row in rows if row.get("status") == "返信あり")
        followup = sum(1 for row in rows if row.get("status") == "要フォロー")
        bounced = sum(1 for row in rows if row.get("status") == "バウンス")
        closed = sum(1 for row in rows if row.get("status") == "クローズ")
        checked_dates = [row.get("last_checked_at") or row.get("imported_at") for row in rows if row.get("last_checked_at") or row.get("imported_at")]
        return {
            "total": total,
            "email_count": email_count,
            "form_count": form_count,
            "untouched": untouched,
            "sent": sent,
            "replied": replied,
            "followup": followup,
            "bounced": bounced,
            "closed": closed,
            "email_rate": round(email_count / total * 100, 1) if total else 0,
            "form_rate": round(form_count / total * 100, 1) if total else 0,
            "reply_rate": round(replied / sent * 100, 1) if sent else 0,
            "last_updated": max(checked_dates) if checked_dates else "-",
        }

    def export_csv(self, path: Path, filters: dict[str, Any] | None = None) -> int:
        rows = self.list_companies(filters=filters, limit=100000)
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS)
            writer.writeheader()
            for row in rows:
                writer.writerow({col: row.get(col) for col in EXPORT_COLUMNS})
        return len(rows)


class SupabaseStore:
    name = "Supabase"

    def __init__(self, url: str, key: str) -> None:
        from supabase import create_client

        self.client = create_client(url, key)
        self.established_sort_available = self._check_established_columns()

    def _check_established_columns(self) -> bool:
        try:
            self.client.table("companies").select("established_at,established_raw").limit(1).execute()
            return True
        except Exception:
            return False

    def upsert_company(self, company: dict[str, Any], contact: dict[str, Any] | None = None) -> tuple[str, bool]:
        existing_id = self._find_duplicate_id(company)
        if existing_id:
            self.client.table("companies").update({k: v for k, v in company.items() if v is not None}).eq("id", existing_id).execute()
            company_id = existing_id
            created = False
        else:
            result = self.client.table("companies").insert(company).execute()
            company_id = result.data[0]["id"]
            created = True
            self.client.table("sales_status").insert(
                {"company_id": company_id, "status": "未対応", "created_at": now_iso(), "updated_at": now_iso()}
            ).execute()
        if contact:
            contact["company_id"] = company_id
            self.client.table("company_contacts").upsert(contact, on_conflict="company_id").execute()
        return company_id, created

    def _find_duplicate_id(self, row: dict[str, Any]) -> str | None:
        for key in ["license_no", "permit_no", "tel"]:
            if row.get(key):
                result = self.client.table("companies").select("id").eq(key, row[key]).limit(1).execute()
                if result.data:
                    return result.data[0]["id"]
        if row.get("company_name") and row.get("address"):
            result = (
                self.client.table("companies")
                .select("id")
                .eq("company_name", row["company_name"])
                .eq("address", row["address"])
                .limit(1)
                .execute()
            )
            if result.data:
                return result.data[0]["id"]
        if row.get("company_name") and row.get("city"):
            result = (
                self.client.table("companies")
                .select("id")
                .eq("company_name", row["company_name"])
                .eq("city", row["city"])
                .limit(1)
                .execute()
            )
            if result.data:
                return result.data[0]["id"]
        return None

    def list_companies(
        self,
        filters: dict[str, Any] | None = None,
        limit: int = 500,
        offset: int = 0,
        sort: str = "imported_desc",
    ) -> list[dict[str, Any]]:
        companies = self._list_company_rows(limit, offset, sort)
        company_ids = [row["id"] for row in companies]
        contacts = self._list_related_rows("company_contacts", company_ids)
        statuses = self._list_related_rows("sales_status", company_ids)
        rows = []
        for company in companies:
            company_id = company["id"]
            contact = {k: v for k, v in contacts.get(company_id, {}).items() if k not in ["id", "company_id"]}
            merged = {**company, **contact}
            status = statuses.get(company_id, {})
            merged["company_id"] = company_id
            merged["contact_id"] = contacts.get(company_id, {}).get("id")
            merged["sales_status_id"] = status.get("id")
            merged["status"] = status.get("status", "未対応")
            merged["last_contacted_at"] = status.get("last_contacted_at")
            merged["next_action_at"] = status.get("next_action_at")
            merged["sales_memo"] = status.get("memo")
            rows.append(merged)
        if filters:
            rows = [row for row in rows if passes_filters(row, filters)]
        return rows

    def _list_company_rows(self, limit: int, offset: int = 0, sort: str = "imported_desc") -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        sort_column, sort_desc = SORT_OPTIONS.get(sort, SORT_OPTIONS["imported_desc"])
        if sort_column == "established_at" and not self.established_sort_available:
            sort_column, sort_desc = SORT_OPTIONS["imported_desc"]
        chunk_size = 1000
        for start in range(offset, offset + limit, chunk_size):
            end = min(start + chunk_size - 1, offset + limit - 1)
            result = (
                self.client.table("companies")
                .select("*")
                .order(sort_column, desc=sort_desc)
                .order("id", desc=True)
                .range(start, end)
                .execute()
            )
            chunk = result.data or []
            rows.extend(chunk)
            if len(chunk) < chunk_size:
                break
        return rows

    def _list_related_rows(self, table: str, company_ids: list[str]) -> dict[str, dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        chunk_size = 500
        for start in range(0, len(company_ids), chunk_size):
            chunk_ids = company_ids[start:start + chunk_size]
            if not chunk_ids:
                continue
            result = self.client.table(table).select("*").in_("company_id", chunk_ids).execute()
            rows.update({row["company_id"]: row for row in result.data or []})
        return rows

    def update_status(self, company_id: str, status: str, memo: str | None, next_action_at: str | None) -> None:
        self.client.table("sales_status").upsert(
            {
                "company_id": company_id,
                "status": status,
                "memo": memo,
                "next_action_at": next_action_at,
                "updated_at": now_iso(),
            },
            on_conflict="company_id",
        ).execute()

    def sidebar_stats(self) -> dict[str, Any]:
        from services import SALES_STATUSES

        total_res = self.client.table("companies").select("*", count="exact").limit(0).execute()
        total = total_res.count or 0

        status_counts: dict[str, int] = {}
        for status in SALES_STATUSES:
            r = self.client.table("sales_status").select("*", count="exact").eq("status", status).limit(0).execute()
            cnt = r.count or 0
            if cnt > 0:
                status_counts[status] = cnt

        last_res = self.client.table("companies").select("imported_at").order("imported_at", desc=True).limit(1).execute()
        last_updated = (last_res.data[0].get("imported_at") or "")[:16] if last_res.data else ""

        return {
            "db_name": "supabase",
            "total": total,
            "last_updated": last_updated,
            "status_counts": status_counts,
        }

    def stats(self) -> dict[str, Any]:
        from services import SALES_STATUSES

        total_res = self.client.table("companies").select("*", count="exact").limit(0).execute()
        total = total_res.count or 0

        email_res = self.client.table("company_contacts").select("*", count="exact").not_.is_("email", "null").limit(0).execute()
        email_count = email_res.count or 0

        form_res = self.client.table("company_contacts").select("*", count="exact").not_.is_("contact_form_url", "null").limit(0).execute()
        form_count = form_res.count or 0

        status_counts: dict[str, int] = {}
        for status in SALES_STATUSES:
            r = self.client.table("sales_status").select("*", count="exact").eq("status", status).limit(0).execute()
            status_counts[status] = r.count or 0

        sent = status_counts.get("送信済み", 0)
        replied = status_counts.get("返信あり", 0)

        last_res = self.client.table("companies").select("imported_at").order("imported_at", desc=True).limit(1).execute()
        last_updated = (last_res.data[0].get("imported_at") or "")[:16] if last_res.data else "-"

        return {
            "total": total,
            "email_count": email_count,
            "form_count": form_count,
            "untouched": status_counts.get("未対応", 0),
            "sent": sent,
            "replied": replied,
            "followup": status_counts.get("要フォロー", 0),
            "bounced": status_counts.get("バウンス", 0),
            "closed": status_counts.get("クローズ", 0),
            "email_rate": round(email_count / total * 100, 1) if total else 0,
            "form_rate": round(form_count / total * 100, 1) if total else 0,
            "reply_rate": round(replied / sent * 100, 1) if sent else 0,
            "last_updated": last_updated,
        }

    def export_csv(self, path: Path, filters: dict[str, Any] | None = None) -> int:
        return SQLiteStore.export_csv(self, path, filters)  # type: ignore[misc]
