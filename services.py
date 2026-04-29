from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Any

TOKYO_STATION_LAT = 35.681236
TOKYO_STATION_LON = 139.767125

BUSINESS_CATEGORIES = [
    "real_estate",
    "construction",
    "rental_management",
    "mansion_management",
    "reform",
    "shop",
    "owner",
    "unknown",
]

SOURCE_TYPES = [
    "mlit_takken",
    "mlit_kensetsu",
    "mlit_rental_management",
    "mlit_mansion_management",
    "gbizinfo",
    "google_maps",
    "official_site",
    "csv_import",
    "manual",
]

SALES_STATUSES = [
    "未対応",
    "サイト確認中",
    "メール取得済み",
    "問い合わせフォーム取得済み",
    "送信済み",
    "返信あり",
    "見込みあり",
    "NG",
    "保留",
]

CONTACT_CONFIDENCE = ["high", "medium", "low", "guessed", "invalid"]

FIELD_ALIASES = {
    "company_name": ["company_name", "会社名", "商号", "業者名", "法人名", "名称", "氏名又は名称"],
    "business_category": ["business_category", "業種", "カテゴリ", "営業種別"],
    "source_type": ["source_type", "取得元", "source"],
    "prefecture": ["prefecture", "都道府県", "所在地都道府県"],
    "city": ["city", "市区町村", "市町村", "所在地市区町村"],
    "ward": ["ward", "区"],
    "address": ["address", "住所", "所在地", "本店所在地", "事務所所在地"],
    "tel": ["tel", "電話", "電話番号", "TEL", "代表電話"],
    "license_no": ["license_no", "免許", "免許番号", "宅建免許番号"],
    "permit_no": ["permit_no", "許可番号", "建設業許可番号"],
    "representative": ["representative", "代表者", "代表者名"],
    "office_type": ["office_type", "主・従", "事務所種別", "営業所種別"],
    "latitude": ["latitude", "lat", "緯度"],
    "longitude": ["longitude", "lon", "lng", "経度"],
    "source_url": ["source_url", "URL", "url", "参照URL"],
    "website_url": ["website_url", "公式サイト", "サイトURL", "ホームページ"],
    "email": ["email", "メール", "メールアドレス", "mail"],
    "contact_form_url": ["contact_form_url", "問い合わせフォーム", "問合せURL", "フォームURL"],
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def parse_float(value: Any) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_phone(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    return re.sub(r"[^\d+]", "", text)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def pick(row: dict[str, Any], canonical: str) -> str | None:
    aliases = FIELD_ALIASES.get(canonical, [canonical])
    normalized = {str(k).strip().lower(): v for k, v in row.items()}
    for alias in aliases:
        key = alias.strip().lower()
        if key in normalized:
            return clean_text(normalized[key])
    return None


def infer_category(row: dict[str, Any]) -> str:
    value = pick(row, "business_category") or ""
    source = pick(row, "source_type") or ""
    joined = f"{value} {source}"
    if any(token in joined for token in ["宅建", "不動産", "real_estate", "takken"]):
        return "real_estate"
    if any(token in joined for token in ["建設", "construction", "kensetsu"]):
        return "construction"
    if "賃貸住宅" in joined:
        return "rental_management"
    if "マンション" in joined:
        return "mansion_management"
    return value if value in BUSINESS_CATEGORIES else "unknown"


def normalize_company_row(row: dict[str, Any], center_lat: float, center_lon: float) -> dict[str, Any]:
    latitude = parse_float(pick(row, "latitude"))
    longitude = parse_float(pick(row, "longitude"))
    distance = None
    if latitude is not None and longitude is not None:
        distance = round(haversine_km(center_lat, center_lon, latitude, longitude), 2)

    return {
        "company_name": pick(row, "company_name"),
        "business_category": infer_category(row),
        "source_type": pick(row, "source_type") or "csv_import",
        "prefecture": pick(row, "prefecture"),
        "city": pick(row, "city"),
        "ward": pick(row, "ward"),
        "address": pick(row, "address"),
        "tel": normalize_phone(pick(row, "tel")),
        "license_no": pick(row, "license_no"),
        "permit_no": pick(row, "permit_no"),
        "representative": pick(row, "representative"),
        "office_type": pick(row, "office_type"),
        "latitude": latitude,
        "longitude": longitude,
        "distance_km": distance,
        "source_url": pick(row, "source_url"),
        "source_updated_at": None,
        "imported_at": now_iso(),
        "last_checked_at": None,
        "is_active": True,
    }


def normalize_contact_row(row: dict[str, Any]) -> dict[str, Any] | None:
    website = pick(row, "website_url")
    email = pick(row, "email")
    form_url = pick(row, "contact_form_url")
    if not any([website, email, form_url]):
        return None
    confidence = "high" if email else "medium" if form_url else "low"
    return {
        "website_url": website,
        "email": email,
        "contact_form_url": form_url,
        "source_url": website or form_url,
        "confidence": confidence,
        "checked_at": now_iso(),
        "is_valid": True,
        "memo": None,
    }


def passes_filters(row: dict[str, Any], filters: dict[str, Any]) -> bool:
    company_name = clean_text(filters.get("company_name"))
    if company_name and company_name not in (row.get("company_name") or ""):
        return False
    for key in ["prefecture", "city", "ward"]:
        value = clean_text(filters.get(key))
        if value and value not in (row.get(key) or ""):
            return False
    category = clean_text(filters.get("business_category"))
    if category and category != "all" and row.get("business_category") != category:
        return False
    status = clean_text(filters.get("status"))
    if status and status != "all" and row.get("status") != status:
        return False
    keyword = clean_text(filters.get("q"))
    if keyword:
        target = " ".join(str(row.get(k) or "") for k in ["company_name", "address", "tel", "license_no", "permit_no"])
        if keyword.lower() not in target.lower():
            return False
    exclude_q = clean_text(filters.get("exclude_q"))
    if exclude_q:
        target = " ".join(str(row.get(k) or "") for k in ["company_name", "address", "license_no", "permit_no", "representative"])
        exclude_terms = [term.strip().lower() for term in exclude_q.replace(",", " ").split() if term.strip()]
        if any(term in target.lower() for term in exclude_terms):
            return False
    if clean_text(filters.get("has_tel")) == "1" and not row.get("tel"):
        return False
    if clean_text(filters.get("has_contact")) == "1" and not any([row.get("website_url"), row.get("email"), row.get("contact_form_url")]):
        return False
    if clean_text(filters.get("has_email")) == "1" and not row.get("email"):
        return False
    if clean_text(filters.get("has_form")) == "1" and not row.get("contact_form_url"):
        return False
    radius = parse_float(filters.get("radius_km"))
    if radius is not None and row.get("distance_km") not in [None, ""]:
        try:
            if float(row["distance_km"]) > radius:
                return False
        except (TypeError, ValueError):
            return False
    return True
