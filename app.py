from __future__ import annotations

import csv
from pathlib import Path

from flask import Flask, Response, flash, redirect, render_template, request, send_file, url_for

from services import (
    BUSINESS_CATEGORIES,
    SALES_STATUSES,
    SOURCE_TYPES,
    TOKYO_STATION_LAT,
    TOKYO_STATION_LON,
    normalize_company_row,
    normalize_contact_row,
)
from storage import create_store

app = Flask(__name__)
app.secret_key = "local-sales-leads-dev"
store = create_store()


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


def current_filters() -> dict[str, str]:
    return {
        "company_name": request.args.get("company_name", "").strip(),
        "prefecture": request.args.get("prefecture", "").strip(),
        "city": request.args.get("city", "").strip(),
        "ward": request.args.get("ward", "").strip(),
        "q": request.args.get("q", "").strip(),
        "exclude_q": request.args.get("exclude_q", "").strip(),
        "has_tel": request.args.get("has_tel", "").strip(),
        "has_contact": request.args.get("has_contact", "").strip(),
        "has_email": request.args.get("has_email", "").strip(),
        "has_form": request.args.get("has_form", "").strip(),
        "radius_km": request.args.get("radius_km", "50").strip(),
        "business_category": request.args.get("business_category", "all").strip(),
        "status": request.args.get("status", "all").strip(),
    }


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
        created = 0
        updated = 0
        skipped = 0
        for raw in rows:
            company = normalize_company_row(raw, center_lat, center_lon)
            contact = normalize_contact_row(raw)
            if not company.get("company_name"):
                skipped += 1
                continue
            _, was_created = store.upsert_company(company, contact)
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
    )


@app.get("/companies")
def companies() -> str:
    filters = current_filters()
    all_rows = store.list_companies(filters=filters, limit=100000)
    rows = all_rows[:500]
    return render_template(
        "companies.html",
        rows=rows,
        total_count=len(all_rows),
        display_limit=500,
        filters=filters,
        categories=BUSINESS_CATEGORIES,
        statuses=SALES_STATUSES,
    )


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


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "store": store.name}


if __name__ == "__main__":
    app.run(debug=True, port=5050)
