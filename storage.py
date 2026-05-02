from __future__ import annotations

import csv
import os
import sqlite3
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from services import SALES_STATUSES, format_jst_datetime, now_iso, passes_filters

load_dotenv()

for proxy_var in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
    if os.getenv(proxy_var) == "http://127.0.0.1:9":
        os.environ.pop(proxy_var, None)

COMPANY_COLUMNS = [
    "company_name",
    "company_name_normalized",
    "corporate_number",
    "primary_business_category",
    "source_type",
    "source_name",
    "source_record_id",
    "prefecture",
    "city",
    "ward",
    "address",
    "tel",
    "representative",
    "established_at",
    "established_raw",
    "latitude",
    "longitude",
    "distance_km",
    "source_url",
    "source_updated_at",
    "imported_at",
    "last_checked_at",
    "last_seen_at",
    "last_manual_updated_at",
    "manual_updated_by",
    "is_active",
    "needs_review",
    "is_branch",
    "is_bank_like",
    "update_note",
    "created_at",
    "updated_at",
]

LICENSE_COLUMNS = [
    "company_id",
    "license_type",
    "license_no",
    "permit_no",
    "registration_no",
    "authority",
    "office_type",
    "source_type",
    "source_name",
    "source_url",
    "source_record_id",
    "issued_at",
    "valid_from",
    "valid_to",
    "last_seen_at",
    "is_active",
    "needs_review",
    "memo",
    "created_at",
    "updated_at",
]

EDITABLE_COMPANY_FIELDS = frozenset({
    "company_name", "company_name_normalized", "corporate_number",
    "primary_business_category", "source_type", "source_name", "source_url",
    "source_record_id", "prefecture", "city", "ward", "address", "tel",
    "representative", "is_active", "needs_review", "update_note",
    "latitude", "longitude",
})

EXPORT_COLUMNS = [
    "id",
    *COMPANY_COLUMNS,
    "website_url",
    "email",
    "contact_form_url",
    "confidence",
    "license_types",
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
                    company_name_normalized text,
                    corporate_number text,
                    primary_business_category text not null default 'unknown',
                    source_type text not null default 'csv_import',
                    source_name text,
                    source_record_id text,
                    prefecture text,
                    city text,
                    ward text,
                    address text,
                    tel text,
                    representative text,
                    established_at text,
                    established_raw text,
                    latitude real,
                    longitude real,
                    distance_km real,
                    source_url text,
                    source_updated_at text,
                    imported_at text,
                    last_checked_at text,
                    last_seen_at text,
                    last_manual_updated_at text,
                    manual_updated_by text,
                    is_active integer not null default 1,
                    needs_review integer not null default 0,
                    update_note text,
                    created_at text,
                    updated_at text
                );
                create table if not exists company_licenses (
                    id integer primary key autoincrement,
                    company_id integer not null,
                    license_type text not null default 'unknown',
                    license_no text,
                    permit_no text,
                    registration_no text,
                    authority text,
                    office_type text,
                    source_type text not null default 'csv_import',
                    source_name text,
                    source_url text,
                    source_record_id text,
                    issued_at text,
                    valid_from text,
                    valid_to text,
                    last_seen_at text,
                    is_active integer not null default 1,
                    needs_review integer not null default 0,
                    memo text,
                    created_at text,
                    updated_at text,
                    foreign key(company_id) references companies(id)
                );
                create table if not exists company_contacts (
                    id integer primary key autoincrement,
                    company_id integer not null,
                    website_url text,
                    website_title text,
                    email text,
                    contact_form_url text,
                    source_url text,
                    confidence text,
                    checked_at text,
                    is_valid integer not null default 1,
                    opt_out integer not null default 0,
                    opt_out_at text,
                    unsubscribe_token text,
                    memo text,
                    foreign key(company_id) references companies(id)
                );
                create table if not exists sales_status (
                    id integer primary key autoincrement,
                    company_id integer not null unique,
                    status text not null default '未対応',
                    priority integer not null default 0,
                    last_contacted_at text,
                    next_action_at text,
                    memo text,
                    created_at text not null,
                    updated_at text not null,
                    foreign key(company_id) references companies(id)
                );
                create table if not exists import_logs (
                    id integer primary key autoincrement,
                    source_type text not null,
                    source_name text,
                    target_category text,
                    target_area text,
                    total_rows integer not null default 0,
                    inserted_companies_count integer not null default 0,
                    updated_companies_count integer not null default 0,
                    inserted_licenses_count integer not null default 0,
                    updated_licenses_count integer not null default 0,
                    inactive_candidates_count integer not null default 0,
                    error_count integer not null default 0,
                    imported_at text not null,
                    memo text
                );
                create table if not exists company_update_logs (
                    id integer primary key autoincrement,
                    company_id integer not null,
                    license_id integer,
                    update_type text not null default 'manual',
                    field_name text,
                    old_value text,
                    new_value text,
                    updated_by text,
                    update_note text,
                    created_at text not null,
                    foreign key(company_id) references companies(id),
                    foreign key(license_id) references company_licenses(id)
                );
                create index if not exists idx_companies_tel on companies(tel);
                create index if not exists idx_companies_corporate on companies(corporate_number);
                create index if not exists idx_companies_normalized on companies(company_name_normalized);
                create index if not exists idx_companies_location on companies(prefecture, city, ward);
                create index if not exists idx_companies_name on companies(company_name);
                create index if not exists idx_companies_established on companies(established_at);
                create index if not exists idx_licenses_company on company_licenses(company_id);
                create index if not exists idx_licenses_type on company_licenses(license_type);
                create index if not exists idx_licenses_no on company_licenses(license_no);
                create index if not exists idx_licenses_permit on company_licenses(permit_no);
                create index if not exists idx_update_logs_company on company_update_logs(company_id);
                """
            )
            # --- 既存DB向けマイグレーション ---
            co_cols = {r["name"] for r in conn.execute("pragma table_info(companies)").fetchall()}
            # business_category → primary_business_category
            if "business_category" in co_cols and "primary_business_category" not in co_cols:
                conn.execute("alter table companies rename column business_category to primary_business_category")
            new_co_cols = [
                ("company_name_normalized", "text"),
                ("corporate_number", "text"),
                ("source_name", "text"),
                ("source_record_id", "text"),
                ("last_seen_at", "text"),
                ("last_manual_updated_at", "text"),
                ("manual_updated_by", "text"),
                ("needs_review", "integer not null default 0"),
                ("is_branch", "integer not null default 0"),
                ("is_bank_like", "integer not null default 0"),
                ("update_note", "text"),
                ("created_at", "text"),
                ("updated_at", "text"),
                ("established_at", "text"),
                ("established_raw", "text"),
            ]
            co_cols = {r["name"] for r in conn.execute("pragma table_info(companies)").fetchall()}
            for col_name, col_def in new_co_cols:
                if col_name not in co_cols:
                    conn.execute(f"alter table companies add column {col_name} {col_def}")

            cc_cols = {r["name"] for r in conn.execute("pragma table_info(company_contacts)").fetchall()}
            for col_name, col_def in [
                ("website_title", "text"),
                ("opt_out", "integer not null default 0"),
                ("opt_out_at", "text"),
                ("unsubscribe_token", "text"),
            ]:
                if col_name not in cc_cols:
                    conn.execute(f"alter table company_contacts add column {col_name} {col_def}")

            ss_cols = {r["name"] for r in conn.execute("pragma table_info(sales_status)").fetchall()}
            if "priority" not in ss_cols:
                conn.execute("alter table sales_status add column priority integer not null default 0")

            ul_cols = {r["name"] for r in conn.execute("pragma table_info(company_update_logs)").fetchall()}
            if "license_id" not in ul_cols:
                conn.execute("alter table company_update_logs add column license_id integer references company_licenses(id)")

    # ---- 重複判定 ----

    def _find_duplicate_id(self, row: dict[str, Any]) -> int | None:
        # source_record_id がある場合はそれを正とし、name/tel マッチは行わない
        if row.get("source_record_id") and row.get("source_type"):
            with self.connect() as conn:
                found = conn.execute(
                    "select id from companies where source_record_id = ? and source_type = ? limit 1",
                    (row["source_record_id"], row["source_type"]),
                ).fetchone()
            return int(found["id"]) if found else None

        conditions: list[str] = []
        params: list[Any] = []
        if row.get("corporate_number"):
            conditions.append("corporate_number = ?")
            params.append(row["corporate_number"])
        if row.get("tel"):
            conditions.append("tel = ?")
            params.append(row["tel"])
        if row.get("company_name_normalized") and row.get("city"):
            conditions.append("(company_name_normalized = ? and city = ?)")
            params.extend([row["company_name_normalized"], row["city"]])
        if row.get("company_name") and row.get("city"):
            conditions.append("(company_name = ? and city = ?)")
            params.extend([row["company_name"], row["city"]])
        if row.get("company_name") and row.get("address"):
            conditions.append("(company_name = ? and address = ?)")
            params.extend([row["company_name"], row["address"]])
        if row.get("company_name") and row.get("representative"):
            conditions.append("(company_name = ? and representative = ?)")
            params.extend([row["company_name"], row["representative"]])
        if not conditions:
            return None
        with self.connect() as conn:
            found = conn.execute(
                f"select id from companies where {' or '.join(conditions)} limit 1", params
            ).fetchone()
            return int(found["id"]) if found else None

    def _find_license_id_with_conn(self, conn: sqlite3.Connection, company_id: int, license: dict[str, Any]) -> int | None:
        conditions: list[str] = ["company_id = ?"]
        params: list[Any] = [company_id]
        if license.get("source_record_id"):
            conditions.append("source_record_id = ?")
            params.append(license["source_record_id"])
            found = conn.execute(f"select id from company_licenses where {' and '.join(conditions)} limit 1", params).fetchone()
            if found:
                return int(found["id"])
            conditions.pop()
            params.pop()
        for key in [("license_type", "license_no"), ("license_type", "permit_no"), ("license_type", "registration_no")]:
            lt_key, no_key = key
            if license.get(lt_key) and license.get(no_key):
                c = conditions + [f"license_type = ?", f"{no_key} = ?"]
                p = params + [license[lt_key], license[no_key]]
                found = conn.execute(f"select id from company_licenses where {' and '.join(c)} limit 1", p).fetchone()
                if found:
                    return int(found["id"])
        if license.get("license_type") and license.get("authority"):
            c = conditions + ["license_type = ?", "authority = ?"]
            p = params + [license["license_type"], license["authority"]]
            found = conn.execute(f"select id from company_licenses where {' and '.join(c)} limit 1", p).fetchone()
            if found:
                return int(found["id"])
        return None

    # ---- upsert ----

    def upsert_company(self, company: dict[str, Any], license: dict[str, Any] | None = None, contact: dict[str, Any] | None = None) -> tuple[int, bool]:
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
                    "insert into sales_status (company_id, status, created_at, updated_at) values (?, ?, ?, ?)",
                    (company_id, "未対応", now_iso(), now_iso()),
                )
            if license:
                self._upsert_license_with_conn(conn, company_id, license)
            if contact:
                self.upsert_contact(conn, company_id, contact)
        return company_id, created

    def _upsert_license_with_conn(self, conn: sqlite3.Connection, company_id: int, license: dict[str, Any]) -> tuple[int, bool]:
        existing_id = self._find_license_id_with_conn(conn, company_id, license)
        cols = [c for c in LICENSE_COLUMNS if c != "company_id"]
        if existing_id:
            assignments = ", ".join(f"{col} = coalesce(?, {col})" for col in cols)
            params = [license.get(col) for col in cols] + [existing_id]
            conn.execute(f"update company_licenses set {assignments} where id = ?", params)
            return existing_id, False
        else:
            all_cols = ["company_id"] + cols
            placeholders = ", ".join("?" for _ in all_cols)
            params = [company_id] + [license.get(col) for col in cols]
            cursor = conn.execute(
                f"insert into company_licenses ({', '.join(all_cols)}) values ({placeholders})", params
            )
            return int(cursor.lastrowid), True

    def upsert_license(self, company_id: int, license: dict[str, Any]) -> tuple[int, bool]:
        with self.connect() as conn:
            return self._upsert_license_with_conn(conn, company_id, license)

    def upsert_contact(self, conn: sqlite3.Connection, company_id: int, contact: dict[str, Any]) -> None:
        found = conn.execute("select id from company_contacts where company_id = ? limit 1", (company_id,)).fetchone()
        cols = ["website_url", "email", "contact_form_url", "source_url", "confidence", "checked_at", "is_valid", "memo"]
        if found:
            assignments = ", ".join(f"{col} = coalesce(?, {col})" for col in cols)
            conn.execute(
                f"update company_contacts set {assignments} where id = ?",
                [contact.get(col) for col in cols] + [found["id"]],
            )
        else:
            conn.execute(
                f"insert into company_contacts (company_id, {', '.join(cols)}) values (?, {', '.join('?' for _ in cols)})",
                [company_id] + [contact.get(col) for col in cols],
            )

    # ---- 一覧取得 ----

    def list_companies(
        self,
        filters: dict[str, Any] | None = None,
        limit: int = 500,
        offset: int = 0,
        sort: str = "imported_desc",
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            sort_column, sort_desc = SORT_OPTIONS.get(sort, SORT_OPTIONS["imported_desc"])
            clauses: list[str] = []
            params: list[Any] = []

            # デフォルトで is_active=1 のみ表示
            if not (filters and filters.get("show_inactive")):
                clauses.append("c.is_active = 1")
            # デフォルトで支店・銀行類を非表示
            if not (filters and filters.get("show_branches")):
                clauses.append("(c.is_branch = 0 OR c.is_branch IS NULL)")
            if not (filters and filters.get("show_banks")):
                clauses.append("(c.is_bank_like = 0 OR c.is_bank_like IS NULL)")

            if filters:
                if filters.get("company_name"):
                    clauses.append("c.company_name LIKE ?")
                    params.append(f"%{filters['company_name']}%")
                if filters.get("prefecture"):
                    clauses.append("c.prefecture LIKE ?")
                    params.append(f"%{filters['prefecture']}%")
                if filters.get("city"):
                    clauses.append("c.city LIKE ?")
                    params.append(f"%{filters['city']}%")
                if filters.get("ward"):
                    clauses.append("c.ward LIKE ?")
                    params.append(f"%{filters['ward']}%")
                if filters.get("primary_business_category") and filters["primary_business_category"] != "all":
                    clauses.append("c.primary_business_category = ?")
                    params.append(filters["primary_business_category"])
                if filters.get("status") and filters["status"] != "all":
                    clauses.append("coalesce(ss.status, '未対応') = ?")
                    params.append(filters["status"])
                if filters.get("has_tel") == "1":
                    clauses.append("c.tel IS NOT NULL AND c.tel != ''")
                if filters.get("has_website") == "1":
                    clauses.append("cc.website_url IS NOT NULL AND cc.website_url != ''")
                if filters.get("has_email") == "1":
                    clauses.append("cc.email IS NOT NULL AND cc.email != ''")
                if filters.get("has_form") == "1":
                    clauses.append("cc.contact_form_url IS NOT NULL AND cc.contact_form_url != ''")
                if filters.get("has_contact") == "1":
                    clauses.append("(cc.website_url IS NOT NULL OR cc.email IS NOT NULL OR cc.contact_form_url IS NOT NULL)")
                if filters.get("q"):
                    like = f"%{filters['q']}%"
                    clauses.append("(c.company_name LIKE ? OR c.address LIKE ? OR c.tel LIKE ?)")
                    params.extend([like, like, like])
                # 許認可フィルター（SQLで絞り込み）
                for lt in filters.get("license_types") or []:
                    if lt:
                        clauses.append(
                            "exists (select 1 from company_licenses cl "
                            "where cl.company_id = c.id and cl.license_type = ? and cl.is_active = 1)"
                        )
                        params.append(lt)

            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            rows = conn.execute(
                f"""
                select c.*,
                       cc.website_url, cc.email, cc.contact_form_url, cc.confidence,
                       cc.opt_out,
                       ss.status, ss.last_contacted_at, ss.next_action_at, ss.memo as sales_memo,
                       ss.priority,
                       (select group_concat(cl.license_type)
                        from company_licenses cl
                        where cl.company_id = c.id and cl.is_active = 1) as license_types
                from companies c
                left join company_contacts cc on cc.company_id = c.id
                left join sales_status ss on ss.company_id = c.id
                {where}
                order by c.{sort_column} {'desc' if sort_desc else 'asc'}, c.id desc
                limit ? offset ?
                """,
                params + [limit, offset],
            ).fetchall()
        data = [dict(row) for row in rows]
        # Python側フィルター
        if filters:
            py_keys = ("exclude_q", "radius_km", "license_types")
            py_filters = {k: filters[k] for k in py_keys if filters.get(k)}
            if py_filters:
                data = [r for r in data if passes_filters(r, py_filters)]
        return data

    # ---- 1件取得 ----

    def get_company(self, company_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                select c.*, cc.id as contact_id, cc.website_url, cc.email, cc.contact_form_url,
                       cc.confidence, cc.opt_out, cc.memo as contact_memo,
                       ss.status, ss.priority, ss.last_contacted_at, ss.next_action_at, ss.memo as sales_memo
                from companies c
                left join company_contacts cc on cc.company_id = c.id
                left join sales_status ss on ss.company_id = c.id
                where c.id = ?
                """,
                (company_id,),
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            licenses = conn.execute(
                "select * from company_licenses where company_id = ? order by created_at",
                (company_id,),
            ).fetchall()
            result["licenses"] = [dict(r) for r in licenses]
        return result

    # ---- 手動更新 ----

    def update_company(self, company_id: int, fields: dict[str, Any], updated_by: str, update_note: str | None = None) -> None:
        safe = {k: v for k, v in fields.items() if k in EDITABLE_COMPANY_FIELDS}
        if not safe:
            return
        now = now_iso()
        with self.connect() as conn:
            old = conn.execute("select * from companies where id = ?", (company_id,)).fetchone()
            if not old:
                raise ValueError(f"company {company_id} not found")
            old = dict(old)
            safe["last_manual_updated_at"] = now
            safe["manual_updated_by"] = updated_by
            safe["updated_at"] = now
            assignments = ", ".join(f"{col} = ?" for col in safe)
            conn.execute(f"update companies set {assignments} where id = ?", list(safe.values()) + [company_id])
            for field, new_val in fields.items():
                if field not in EDITABLE_COMPANY_FIELDS:
                    continue
                old_val = old.get(field)
                if str(old_val or "") != str(new_val or ""):
                    conn.execute(
                        """insert into company_update_logs
                        (company_id, update_type, field_name, old_value, new_value, updated_by, update_note, created_at)
                        values (?, 'manual', ?, ?, ?, ?, ?, ?)""",
                        (company_id, field,
                         str(old_val) if old_val is not None else None,
                         str(new_val) if new_val is not None else None,
                         updated_by, update_note, now),
                    )

    def update_contact(self, company_id: int, contact: dict[str, Any]) -> None:
        with self.connect() as conn:
            self.upsert_contact(conn, company_id, contact)

    def get_update_logs(self, company_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "select * from company_update_logs where company_id = ? order by created_at desc limit 100",
                (company_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    # ---- 営業ステータス ----

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

    # ---- 統計 ----

    def sidebar_stats(self) -> dict[str, Any]:
        with self.connect() as conn:
            agg = conn.execute(
                "select count(*) as total, max(imported_at) as last_updated from companies where is_active = 1"
            ).fetchone()
            status_rows = conn.execute(
                """
                select coalesce(ss.status, '未対応') as status, count(*) as cnt
                from companies c
                left join sales_status ss on ss.company_id = c.id
                where c.is_active = 1
                group by coalesce(ss.status, '未対応')
                """
            ).fetchall()
        last_updated = format_jst_datetime(agg["last_updated"])
        status_counts = {r["status"]: r["cnt"] for r in status_rows}
        return {
            "db_name": self.db_path.name,
            "total": agg["total"],
            "last_updated": last_updated,
            "status_counts": {status: status_counts.get(status, 0) for status in SALES_STATUSES},
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
            "last_updated": format_jst_datetime(max((r.get("last_checked_at") or r.get("imported_at") for r in rows if r.get("last_checked_at") or r.get("imported_at")), default="")),
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

    def _find_duplicate_id(self, row: dict[str, Any]) -> str | None:
        # source_record_id がある場合はそれを正とし、name/tel マッチは行わない
        if row.get("source_record_id") and row.get("source_type"):
            r = (self.client.table("companies").select("id")
                 .eq("source_record_id", row["source_record_id"])
                 .eq("source_type", row["source_type"]).limit(1).execute())
            return r.data[0]["id"] if r.data else None

        if row.get("corporate_number"):
            r = self.client.table("companies").select("id").eq("corporate_number", row["corporate_number"]).limit(1).execute()
            if r.data:
                return r.data[0]["id"]
        if row.get("tel"):
            r = self.client.table("companies").select("id").eq("tel", row["tel"]).limit(1).execute()
            if r.data:
                return r.data[0]["id"]
        if row.get("company_name_normalized") and row.get("city"):
            r = (self.client.table("companies").select("id")
                 .eq("company_name_normalized", row["company_name_normalized"])
                 .eq("city", row["city"]).limit(1).execute())
            if r.data:
                return r.data[0]["id"]
        if row.get("company_name") and row.get("city"):
            r = (self.client.table("companies").select("id")
                 .eq("company_name", row["company_name"])
                 .eq("city", row["city"]).limit(1).execute())
            if r.data:
                return r.data[0]["id"]
        if row.get("company_name") and row.get("address"):
            r = (self.client.table("companies").select("id")
                 .eq("company_name", row["company_name"])
                 .eq("address", row["address"]).limit(1).execute())
            if r.data:
                return r.data[0]["id"]
        return None

    def _find_license_id(self, company_id: str, license: dict[str, Any]) -> str | None:
        base = self.client.table("company_licenses").select("id").eq("company_id", company_id)
        if license.get("source_record_id"):
            r = base.eq("source_record_id", license["source_record_id"]).limit(1).execute()
            if r.data:
                return r.data[0]["id"]
        for no_key in ["license_no", "permit_no", "registration_no"]:
            if license.get("license_type") and license.get(no_key):
                r = (base.eq("license_type", license["license_type"])
                     .eq(no_key, license[no_key]).limit(1).execute())
                if r.data:
                    return r.data[0]["id"]
        return None

    def upsert_company(self, company: dict[str, Any], license: dict[str, Any] | None = None, contact: dict[str, Any] | None = None) -> tuple[str, bool]:
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
        if license:
            self.upsert_license(company_id, license)
        if contact:
            contact["company_id"] = company_id
            self.client.table("company_contacts").upsert(contact, on_conflict="company_id").execute()
        return company_id, created

    def upsert_license(self, company_id: str, license: dict[str, Any]) -> tuple[str, bool]:
        existing_id = self._find_license_id(company_id, license)
        if existing_id:
            self.client.table("company_licenses").update({k: v for k, v in license.items() if v is not None}).eq("id", existing_id).execute()
            return existing_id, False
        else:
            data = {**license, "company_id": company_id}
            result = self.client.table("company_licenses").insert(data).execute()
            return result.data[0]["id"], True

    def list_companies(
        self,
        filters: dict[str, Any] | None = None,
        limit: int = 500,
        offset: int = 0,
        sort: str = "imported_desc",
    ) -> list[dict[str, Any]]:
        allowed_ids = self._resolve_related_ids(filters) if filters else None
        if filters and filters.get("license_types") and allowed_ids is None:
            allowed_ids = self._license_company_ids_page(filters["license_types"], offset, limit)
            offset = 0
        if allowed_ids is not None and len(allowed_ids) == 0:
            return []

        companies = self._list_company_rows(limit, offset, sort, filters=filters, allowed_ids=allowed_ids)
        company_ids = [row["id"] for row in companies]
        contacts = self._list_related_rows("company_contacts", company_ids)
        statuses = self._list_related_rows("sales_status", company_ids)
        licenses_map = self._list_licenses(company_ids)

        rows = []
        for company in companies:
            cid = company["id"]
            contact = {k: v for k, v in contacts.get(cid, {}).items() if k not in ["id", "company_id"]}
            merged = {**company, **contact}
            status = statuses.get(cid, {})
            merged["status"] = status.get("status", "未対応")
            merged["last_contacted_at"] = status.get("last_contacted_at")
            merged["next_action_at"] = status.get("next_action_at")
            merged["sales_memo"] = status.get("memo")
            merged["priority"] = status.get("priority", 0)
            license_types = [lic["license_type"] for lic in licenses_map.get(cid, [])]
            merged["license_types"] = ",".join(license_types) if license_types else ""
            rows.append(merged)

        if filters:
            py_keys = ("exclude_q", "radius_km", "license_types", "status",
                       "has_website", "has_email", "has_form", "has_contact",
                       "show_branches", "show_banks")
            py_filters = {k: filters[k] for k in py_keys if k in filters}
            if py_filters:
                rows = [r for r in rows if passes_filters(r, py_filters)]
        return rows

    def _license_company_ids_page(self, license_types: list[str], offset: int, limit: int) -> set[str]:
        wanted = [lt for lt in license_types if lt]
        if not wanted:
            return set()
        ids: list[str] = []
        seen: set[str] = set()
        scan_start = 0
        scan_size = 1000
        target_count = offset + limit
        while len(ids) < target_count:
            result = (
                self.client.table("company_licenses")
                .select("company_id")
                .eq("is_active", True)
                .in_("license_type", wanted)
                .range(scan_start, scan_start + scan_size - 1)
                .execute()
            )
            chunk = result.data or []
            if not chunk:
                break
            for row in chunk:
                cid = row.get("company_id")
                if cid and cid not in seen:
                    seen.add(cid)
                    ids.append(cid)
                    if len(ids) >= target_count:
                        break
            if len(chunk) < scan_size:
                break
            scan_start += scan_size
        return set(ids[offset:offset + limit])

    def _list_licenses(self, company_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        result: dict[str, list[dict]] = {}
        chunk_size = 500
        for start in range(0, len(company_ids), chunk_size):
            chunk = company_ids[start:start + chunk_size]
            if not chunk:
                continue
            r = self.client.table("company_licenses").select("*").in_("company_id", chunk).eq("is_active", True).execute()
            for row in r.data or []:
                result.setdefault(row["company_id"], []).append(row)
        return result

    def _resolve_related_ids(self, filters: dict[str, Any]) -> set | None:
        id_sets: list[set] = []
        if filters.get("status") and filters["status"] != "all":
            r = self.client.table("sales_status").select("company_id").eq("status", filters["status"]).execute()
            ids = {row["company_id"] for row in r.data or []}
            if len(ids) <= 200:
                id_sets.append(ids)
        if filters.get("has_email") == "1":
            r = self.client.table("company_contacts").select("company_id").not_.is_("email", "null").execute()
            ids = {row["company_id"] for row in r.data or []}
            if len(ids) <= 200:
                id_sets.append(ids)
        if filters.get("has_form") == "1":
            r = self.client.table("company_contacts").select("company_id").not_.is_("contact_form_url", "null").execute()
            ids = {row["company_id"] for row in r.data or []}
            if len(ids) <= 200:
                id_sets.append(ids)
        if filters.get("has_website") == "1":
            r = self.client.table("company_contacts").select("company_id").not_.is_("website_url", "null").execute()
            ids = {row["company_id"] for row in r.data or []}
            if len(ids) <= 200:
                id_sets.append(ids)
        if filters.get("has_contact") == "1":
            r = self.client.table("company_contacts").select("company_id").execute()
            ids = {row["company_id"] for row in r.data or []}
            if len(ids) <= 200:
                id_sets.append(ids)
        if filters.get("license_types"):
            for lt in filters["license_types"]:
                if not lt:
                    continue
                r = self.client.table("company_licenses").select("company_id").eq("license_type", lt).eq("is_active", True).execute()
                ids = {row["company_id"] for row in r.data or []}
                if len(ids) <= 200:
                    id_sets.append(ids)
        if not id_sets:
            return None
        result = id_sets[0]
        for s in id_sets[1:]:
            result = result & s
        return result

    def _list_company_rows(self, limit: int, offset: int = 0, sort: str = "imported_desc", filters: dict[str, Any] | None = None, allowed_ids: set | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        sort_column, sort_desc = SORT_OPTIONS.get(sort, SORT_OPTIONS["imported_desc"])
        if sort_column == "established_at" and not self.established_sort_available:
            sort_column, sort_desc = SORT_OPTIONS["imported_desc"]

        query = self.client.table("companies").select("*")

        # デフォルトで is_active のみ
        if not (filters and filters.get("show_inactive")):
            query = query.eq("is_active", True)
        # デフォルトで支店・銀行類を非表示
        if not (filters and filters.get("show_branches")):
            query = query.eq("is_branch", False)
        if not (filters and filters.get("show_banks")):
            query = query.eq("is_bank_like", False)

        if allowed_ids is not None:
            query = query.in_("id", list(allowed_ids))

        if filters:
            if filters.get("company_name"):
                query = query.ilike("company_name", f"%{filters['company_name']}%")
            if filters.get("prefecture"):
                query = query.ilike("prefecture", f"%{filters['prefecture']}%")
            if filters.get("city"):
                query = query.ilike("city", f"%{filters['city']}%")
            if filters.get("ward"):
                query = query.ilike("ward", f"%{filters['ward']}%")
            if filters.get("primary_business_category") and filters["primary_business_category"] != "all":
                query = query.eq("primary_business_category", filters["primary_business_category"])
            if filters.get("has_tel") == "1":
                query = query.not_.is_("tel", "null")
            if filters.get("q"):
                q = filters["q"]
                query = query.or_(f"company_name.ilike.%{q}%,address.ilike.%{q}%,tel.ilike.%{q}%")

        chunk_size = 1000
        for start in range(offset, offset + limit, chunk_size):
            end = min(start + chunk_size - 1, offset + limit - 1)
            result = (
                query
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

    def get_company(self, company_id: int) -> dict[str, Any] | None:
        result = self.client.table("companies").select("*").eq("id", company_id).limit(1).execute()
        if not result.data:
            return None
        company = result.data[0]
        contact_res = self.client.table("company_contacts").select("*").eq("company_id", company_id).limit(1).execute()
        contact = contact_res.data[0] if contact_res.data else {}
        status_res = self.client.table("sales_status").select("*").eq("company_id", company_id).limit(1).execute()
        status = status_res.data[0] if status_res.data else {}
        licenses_res = self.client.table("company_licenses").select("*").eq("company_id", company_id).execute()
        return {
            **company,
            "contact_id": contact.get("id"),
            "website_url": contact.get("website_url"),
            "email": contact.get("email"),
            "contact_form_url": contact.get("contact_form_url"),
            "confidence": contact.get("confidence"),
            "opt_out": contact.get("opt_out", False),
            "contact_memo": contact.get("memo"),
            "status": status.get("status", "未対応"),
            "priority": status.get("priority", 0),
            "last_contacted_at": status.get("last_contacted_at"),
            "next_action_at": status.get("next_action_at"),
            "sales_memo": status.get("memo"),
            "licenses": licenses_res.data or [],
        }

    def update_company(self, company_id: int, fields: dict[str, Any], updated_by: str, update_note: str | None = None) -> None:
        safe = {k: v for k, v in fields.items() if k in EDITABLE_COMPANY_FIELDS}
        if not safe:
            return
        now = now_iso()
        old_res = self.client.table("companies").select("*").eq("id", company_id).limit(1).execute()
        old = old_res.data[0] if old_res.data else {}
        safe["last_manual_updated_at"] = now
        safe["manual_updated_by"] = updated_by
        safe["updated_at"] = now
        self.client.table("companies").update(safe).eq("id", company_id).execute()
        logs = []
        for field, new_val in fields.items():
            if field not in EDITABLE_COMPANY_FIELDS:
                continue
            old_val = old.get(field)
            if str(old_val or "") != str(new_val or ""):
                logs.append({
                    "company_id": company_id,
                    "update_type": "manual",
                    "field_name": field,
                    "old_value": str(old_val) if old_val is not None else None,
                    "new_value": str(new_val) if new_val is not None else None,
                    "updated_by": updated_by,
                    "update_note": update_note,
                    "created_at": now,
                })
        if logs:
            self.client.table("company_update_logs").insert(logs).execute()

    def update_contact(self, company_id: int, contact: dict[str, Any]) -> None:
        contact["company_id"] = company_id
        self.client.table("company_contacts").upsert(contact, on_conflict="company_id").execute()

    def get_update_logs(self, company_id: int) -> list[dict[str, Any]]:
        result = self.client.table("company_update_logs").select("*").eq("company_id", company_id).order("created_at", desc=True).limit(100).execute()
        return result.data or []

    def update_status(self, company_id: str, status: str, memo: str | None, next_action_at: str | None) -> None:
        self.client.table("sales_status").upsert(
            {"company_id": company_id, "status": status, "memo": memo,
             "next_action_at": next_action_at, "updated_at": now_iso()},
            on_conflict="company_id",
        ).execute()

    def sidebar_stats(self) -> dict[str, Any]:
        total_res = self.client.table("companies").select("*", count="exact").eq("is_active", True).limit(0).execute()
        total = total_res.count or 0
        status_counts: dict[str, int] = {}
        for status in SALES_STATUSES:
            r = self.client.table("sales_status").select("*", count="exact").eq("status", status).limit(0).execute()
            status_counts[status] = r.count or 0
        last_res = self.client.table("companies").select("imported_at").order("imported_at", desc=True).limit(1).execute()
        last_updated = format_jst_datetime(last_res.data[0].get("imported_at")) if last_res.data else ""
        return {
            "db_name": "sales.db",
            "total": total,
            "last_updated": last_updated,
            "status_counts": status_counts,
        }

    def stats(self) -> dict[str, Any]:
        total_res = self.client.table("companies").select("*", count="exact").eq("is_active", True).limit(0).execute()
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
        last_updated = format_jst_datetime(last_res.data[0].get("imported_at")) if last_res.data else "-"
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
