from __future__ import annotations

import csv
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlencode

import os

from dotenv import load_dotenv
from flask import Flask, Response, flash, redirect, render_template, request, send_file, session, url_for

from services import (
    BUSINESS_CATEGORIES,
    BUSINESS_CATEGORY_LABELS,
    LICENSE_TYPES,
    LICENSE_TYPE_LABELS,
    SALES_STATUSES,
    SOURCE_TYPES,
    TOKYO_STATION_LAT,
    TOKYO_STATION_LON,
    normalize_company_name,
    normalize_company_row,
    normalize_contact_row,
    normalize_license_row,
    normalize_phone,
    now_iso,
)
from storage import create_store

load_dotenv(override=True)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or "local-sales-leads-dev"
store = create_store()

APP_PASSWORD = os.environ.get("APP_PASSWORD")

PUBLIC_ENDPOINTS = {"login", "static"}
LOCAL_HOSTS = {"127.0.0.1", "localhost"}


@app.before_request
def require_login() -> Response | None:
    if not APP_PASSWORD:
        if request.host.split(":", 1)[0] in LOCAL_HOSTS:
            return None
        return Response("APP_PASSWORD is not configured.", status=503)
    if request.endpoint in PUBLIC_ENDPOINTS:
        return None
    if session.get("authed"):
        return None
    return redirect(url_for("login", next=request.path))


@app.context_processor
def inject_sidebar():
    return {
        "sidebar": store.sidebar_stats(),
        "category_labels": BUSINESS_CATEGORY_LABELS,
        "license_type_labels": LICENSE_TYPE_LABELS,
    }


def parse_positive_int(value: str | None, default: int, maximum: int) -> int:
    try:
        parsed = int(value or default)
    except ValueError:
        return default
    return max(1, min(parsed, maximum))


def read_csv_upload(file_storage) -> list[dict[str, str]]:
    raw = file_storage.read()
    for encoding in ["utf-8-sig", "cp932", "utf-8"]:
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("utf-8", errors="replace")
    return list(csv.DictReader(text.splitlines()))


def current_filters() -> dict:
    def g(key, default=""):
        return request.args.get(key, default).strip()

    license_types = [lt for lt in request.args.getlist("license_types") if lt]
    return {
        "company_name": g("company_name"),
        "prefecture": g("prefecture"),
        "city": g("city"),
        "ward": g("ward"),
        "q": g("q"),
        "exclude_q": g("exclude_q"),
        "has_tel": g("has_tel"),
        "has_website": g("has_website"),
        "has_contact": g("has_contact"),
        "has_email": g("has_email"),
        "has_form": g("has_form"),
        "radius_km": g("radius_km", "50"),
        "primary_business_category": g("primary_business_category", "all"),
        "status": g("status", "all"),
        "sort": g("sort", "imported_desc"),
        "license_types": license_types,
        "show_inactive": g("show_inactive"),
        "show_branches": g("show_branches"),
        "show_banks": g("show_banks"),
    }


def build_query_url(endpoint: str, params: dict) -> str:
    cleaned: dict[str, object] = {}
    for key, value in params.items():
        if key == "license_types":
            if value:
                cleaned[key] = value
        elif value:
            cleaned[key] = value
    query = urlencode(cleaned, doseq=True)
    base = url_for(endpoint)
    return f"{base}?{query}" if query else base


def _extract_company_fields(form) -> dict:
    def v(key):
        return form.get(key, "").strip() or None
    company_name = v("company_name")
    return {
        "company_name": company_name,
        "company_name_normalized": normalize_company_name(company_name),
        "corporate_number": v("corporate_number"),
        "primary_business_category": v("primary_business_category") or "unknown",
        "source_type": v("source_type") or "manual",
        "source_name": v("source_name"),
        "source_record_id": v("source_record_id"),
        "source_url": v("source_url"),
        "prefecture": v("prefecture"),
        "city": v("city"),
        "ward": v("ward"),
        "address": v("address"),
        "tel": normalize_phone(v("tel")),
        "representative": v("representative"),
        "is_active": 1 if form.get("is_active") else 0,
        "needs_review": 1 if form.get("needs_review") else 0,
    }


def _extract_contact_fields(form) -> dict:
    def v(key):
        return form.get(key, "").strip() or None
    return {
        "website_url": v("website_url"),
        "email": v("email"),
        "contact_form_url": v("contact_form_url"),
        "checked_at": now_iso(),
        "is_valid": 1,
    }


@app.route("/login", methods=["GET", "POST"])
def login() -> str | Response:
    if request.method == "POST":
        if request.form.get("password") == APP_PASSWORD:
            session["authed"] = True
            return redirect(request.args.get("next") or url_for("dashboard"))
        flash("パスワードが違います。", "error")
    return render_template("login.html")


@app.get("/logout")
def logout() -> Response:
    session.clear()
    return redirect(url_for("login"))


@app.get("/")
def dashboard() -> str:
    stats = store.stats()
    recent = store.list_companies(limit=20)
    return render_template("dashboard.html", stats=stats, recent=recent, store_name=store.name)


@app.route("/import", methods=["GET", "POST"])
def import_csv() -> str | Response:
    if request.method == "POST":
        uploaded = request.files.get("csv_file")
        if not uploaded or not uploaded.filename:
            flash("CSVファイルを選んでください。", "error")
            return redirect(url_for("import_csv"))
        center_lat = float(request.form.get("center_lat") or TOKYO_STATION_LAT)
        center_lon = float(request.form.get("center_lon") or TOKYO_STATION_LON)
        rows = read_csv_upload(uploaded)
        created = updated = skipped = 0
        for raw in rows:
            company = normalize_company_row(raw, center_lat, center_lon)
            license_data = normalize_license_row(raw)
            contact = normalize_contact_row(raw)
            if not company.get("company_name"):
                skipped += 1
                continue
            _, was_created = store.upsert_company(company, license=license_data, contact=contact)
            if was_created:
                created += 1
            else:
                updated += 1
        flash(f"取込完了: 新規 {created} 件 / 更新 {updated} 件 / スキップ {skipped} 件", "ok")
        return redirect(url_for("companies"))
    return render_template(
        "import.html",
        default_lat=TOKYO_STATION_LAT,
        default_lon=TOKYO_STATION_LON,
        categories=BUSINESS_CATEGORIES,
        sources=SOURCE_TYPES,
        active_log=bool(session.get("fetch_log")),
    )


@app.get("/companies")
def companies() -> str:
    filters = current_filters()
    page = parse_positive_int(request.args.get("page"), 1, 100000)
    per_page = parse_positive_int(request.args.get("per_page"), 100, 500)
    offset = (page - 1) * per_page
    rows = store.list_companies(filters=filters, limit=per_page, offset=offset, sort=filters["sort"])
    export_args = dict(filters)
    prev_args = {**filters, "page": page - 1, "per_page": per_page}
    next_args = {**filters, "page": page + 1, "per_page": per_page}
    return render_template(
        "companies.html",
        rows=rows,
        page=page,
        per_page=per_page,
        has_prev=page > 1,
        has_next=len(rows) == per_page,
        export_url=build_query_url("export_csv", export_args),
        prev_url=build_query_url("companies", prev_args),
        next_url=build_query_url("companies", next_args),
        established_sort_available=getattr(store, "established_sort_available", True),
        filters=filters,
        categories=BUSINESS_CATEGORIES,
        statuses=SALES_STATUSES,
        license_types=LICENSE_TYPES,
    )


@app.get("/companies/new")
def company_new() -> str:
    return render_template(
        "company_detail.html",
        company=None,
        logs=[],
        categories=BUSINESS_CATEGORIES,
        source_types=SOURCE_TYPES,
        statuses=SALES_STATUSES,
        license_types=LICENSE_TYPES,
        is_new=True,
    )


@app.post("/companies/new")
def company_new_post() -> Response:
    fields = _extract_company_fields(request.form)
    if not fields.get("company_name"):
        flash("会社名は必須です。", "error")
        return redirect(url_for("company_new"))
    ts = now_iso()
    company = {**fields, "imported_at": ts, "last_seen_at": ts, "created_at": ts, "updated_at": ts}
    contact = _extract_contact_fields(request.form)
    contact_data = contact if any(v for k, v in contact.items() if k not in ("checked_at", "is_valid") and v) else None
    _, was_created = store.upsert_company(company, license=None, contact=contact_data)
    flash("会社を追加しました。" if was_created else "既存レコードを更新しました。", "ok")
    return redirect(url_for("companies"))


@app.get("/companies/<company_id>")
def company_detail(company_id: str) -> str | Response:
    company = store.get_company(company_id)
    if not company:
        flash("会社が見つかりません。", "error")
        return redirect(url_for("companies"))
    logs = store.get_update_logs(company_id)
    return render_template(
        "company_detail.html",
        company=company,
        logs=logs,
        categories=BUSINESS_CATEGORIES,
        source_types=SOURCE_TYPES,
        statuses=SALES_STATUSES,
        license_types=LICENSE_TYPES,
        is_new=False,
    )


@app.post("/companies/<company_id>")
def company_update(company_id: str) -> Response:
    if not store.get_company(company_id):
        flash("会社が見つかりません。", "error")
        return redirect(url_for("companies"))
    fields = _extract_company_fields(request.form)
    update_note = request.form.get("change_reason", "").strip() or None
    store.update_company(company_id, fields, updated_by="user", update_note=update_note)
    contact = _extract_contact_fields(request.form)
    if any(v for k, v in contact.items() if k not in ("checked_at", "is_valid") and v):
        store.update_contact(company_id, contact)
    status = request.form.get("status")
    if status and status in SALES_STATUSES:
        store.update_status(
            company_id,
            status,
            request.form.get("sales_memo") or None,
            request.form.get("next_action_at") or None,
        )
    flash("会社情報を更新しました。", "ok")
    return redirect(url_for("company_detail", company_id=company_id))


@app.post("/companies/<company_id>/status")
def update_company_status(company_id: str) -> Response:
    store.update_status(
        company_id,
        request.form.get("status", "未対応"),
        request.form.get("memo") or None,
        request.form.get("next_action_at") or None,
    )
    flash("営業ステータスを更新しました。", "ok")
    return redirect(request.referrer or url_for("companies"))


@app.get("/export")
def export_csv() -> Response:
    filters = current_filters()
    export_dir = Path("data/exports")
    export_dir.mkdir(parents=True, exist_ok=True)
    path = export_dir / "sales_leads_export.csv"
    count = store.export_csv(path, filters=filters)
    if count == 0:
        flash("出力対象がありません。条件を変えてください。", "error")
        return redirect(url_for("companies"))
    return send_file(path, as_attachment=True, download_name="sales_leads_export.csv", mimetype="text/csv")


@app.get("/sample-csv")
def sample_csv() -> Response:
    return send_file(Path("data/sample_companies.csv"), as_attachment=True, download_name="sample_companies.csv", mimetype="text/csv")


FETCH_LOG_DIR = Path("data/fetch_logs")


def _tee_subprocess(proc: subprocess.Popen, log_path: Path) -> None:
    """subprocess の stdout をターミナルとログファイルの両方に書き出す"""
    with open(log_path, "wb") as lf:
        for line in iter(proc.stdout.readline, b""):
            try:
                print(line.decode("utf-8", errors="replace"), end="", flush=True)
            except Exception:
                pass
            lf.write(line)
            lf.flush()


@app.post("/admin/fetch-mlit")
def fetch_mlit() -> Response:
    category = request.form.get("category", "takken")
    prefecture = request.form.get("prefecture", "").strip()
    mark_missing = bool(request.form.get("mark_missing"))

    if not prefecture:
        flash("都道府県を入力してください。", "error")
        return redirect(url_for("import_csv"))

    script = Path(__file__).parent / "scripts" / "fetch_mlit_companies.py"
    cmd = [sys.executable, "-u", "-X", "utf8", str(script), "--category", category, "--prefecture", prefecture, "--apply"]
    if mark_missing:
        cmd.append("--mark-missing")

    FETCH_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_name = f"mlit_{category}_{prefecture}_{int(time.time())}.log"
    log_path = FETCH_LOG_DIR / log_name

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent),
        )
        t = threading.Thread(target=_tee_subprocess, args=(proc, log_path), daemon=True)
        t.start()
        session["fetch_log"] = str(log_path)
        flash(
            f"国交省データ取得を開始しました（{category} / {prefecture}）。"
            "下のログで進捗を確認できます。",
            "ok",
        )
    except Exception as e:
        flash(f"起動に失敗しました: {e}", "error")

    return redirect(url_for("import_csv"))


@app.get("/admin/fetch-mlit/log")
def fetch_mlit_log():
    log_path_str = session.get("fetch_log")
    if not log_path_str:
        return {"lines": [], "done": True}
    log_path = Path(log_path_str)
    if not log_path.exists():
        return {"lines": [], "done": False}
    try:
        text = log_path.read_bytes().decode("utf-8", errors="replace")
        lines = text.splitlines()
        completed = "=== 結果 ===" in text
        stale = (time.time() - log_path.stat().st_mtime) > 60
        done = completed or stale
        return {"lines": lines[-300:], "done": done}
    except Exception:
        return {"lines": [], "done": True}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "store": store.name}


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5050)
