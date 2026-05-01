from __future__ import annotations

import math
import re
import unicodedata
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
    "komuten",
    "appraisal",
    "survey",
    "consultant",
    "shop",
    "owner",
    "unknown",
]

BUSINESS_CATEGORY_LABELS = {
    "real_estate": "不動産",
    "construction": "建設",
    "rental_management": "賃貸住宅管理",
    "mansion_management": "マンション管理",
    "reform": "リフォーム",
    "komuten": "工務店",
    "appraisal": "不動産鑑定",
    "survey": "測量・調査",
    "consultant": "コンサルタント",
    "shop": "店舗",
    "owner": "オーナー",
    "unknown": "不明",
}

LICENSE_TYPES = [
    "takken",
    "construction",
    "rental_management",
    "mansion_management",
    "appraisal",
    "survey",
    "consultant",
    "unknown",
]

LICENSE_TYPE_LABELS = {
    "takken": "宅建",
    "construction": "建設",
    "rental_management": "賃貸管理",
    "mansion_management": "マンション管理",
    "appraisal": "鑑定",
    "survey": "測量",
    "consultant": "コンサル",
    "unknown": "その他",
}

SOURCE_TYPES = [
    "csv_import",
    "mlit_takken",
    "mlit_kensetsu",
    "mlit_rental_management",
    "mlit_mansion_management",
    "gbizinfo",
    "official_site",
    "brave_search",
    "serper",
    "google_maps",
    "manual",
]

SALES_STATUSES = [
    "未対応",
    "送信済み",
    "返信あり",
    "要フォロー",
    "バウンス",
    "クローズ",
]

CONTACT_CONFIDENCE = ["high", "medium", "low", "guessed", "invalid"]

FIELD_ALIASES = {
    "company_name": ["company_name", "会社名", "商号", "業者名", "法人名", "名称", "氏名又は名称"],
    "primary_business_category": ["primary_business_category", "business_category", "業種", "カテゴリ", "営業種別"],
    "corporate_number": ["corporate_number", "法人番号"],
    "source_type": ["source_type", "取得元", "source"],
    "source_name": ["source_name", "参照元名", "ソース名"],
    "source_record_id": ["source_record_id", "参照元ID", "原本ID"],
    "prefecture": ["prefecture", "都道府県", "所在地都道府県"],
    "city": ["city", "市区町村", "市町村", "所在地市区町村"],
    "ward": ["ward", "区"],
    "address": ["address", "住所", "所在地", "本店所在地", "事務所所在地"],
    "tel": ["tel", "電話", "電話番号", "TEL", "代表電話"],
    "license_no": ["license_no", "免許", "免許番号", "宅建免許番号"],
    "permit_no": ["permit_no", "許可番号", "建設業許可番号"],
    "registration_no": ["registration_no", "登録番号"],
    "authority": ["authority", "免許行政庁", "許可行政庁", "登録行政庁"],
    "representative": ["representative", "代表者", "代表者名"],
    "office_type": ["office_type", "主・従", "事務所種別", "営業所種別"],
    "established_raw": ["established_raw", "設立年月日", "創業年月日", "設立日", "創業日"],
    "latitude": ["latitude", "lat", "緯度"],
    "longitude": ["longitude", "lon", "lng", "経度"],
    "source_url": ["source_url", "URL", "url", "参照URL"],
    "website_url": ["website_url", "公式サイト", "サイトURL", "ホームページ"],
    "email": ["email", "メール", "メールアドレス", "mail"],
    "contact_form_url": ["contact_form_url", "問い合わせフォーム", "問合せURL", "フォームURL"],
}

ERA_BASE_YEARS = {"R": 2018, "H": 1988, "S": 1925, "T": 1911, "M": 1867}

_CORP_PATTERN = re.compile(
    r"株式会社|有限会社|合同会社|一般社団法人|一般財団法人|特定非営利活動法人|"
    r"医療法人|社会福祉法人|協同組合|農業協同組合|宗教法人|"
    r"[（(]株[）)]|[（(]有[）)]|[（(]合[）)]"
)


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


def parse_japanese_date(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    normalized = text.translate(str.maketrans("０１２３４５６７８９．／－", "0123456789..."))
    normalized = normalized.replace("/", ".").replace("-", ".")
    match = re.match(r"^([RrHhSsTtMm])(\d{1,2})\.(\d{1,2})\.(\d{1,2})$", normalized)
    if match:
        era, year, month, day = match.groups()
        full_year = ERA_BASE_YEARS[era.upper()] + int(year)
        return f"{full_year:04d}-{int(month):02d}-{int(day):02d}"
    match = re.match(r"^(\d{4})\.(\d{1,2})\.(\d{1,2})$", normalized)
    if match:
        year, month, day = match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return None


def normalize_phone(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    return re.sub(r"[^\d+]", "", text)


def normalize_company_name(name: str | None) -> str | None:
    if not name:
        return None
    text = unicodedata.normalize("NFKC", name)
    text = _CORP_PATTERN.sub("", text)
    text = re.sub(r"\s+", "", text).strip()
    return text or None


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
    value = pick(row, "primary_business_category") or ""
    source = pick(row, "source_type") or ""
    joined = f"{value} {source}"
    if any(token in joined for token in ["宅建", "不動産", "real_estate", "takken"]):
        return "real_estate"
    if any(token in joined for token in ["建設", "construction", "kensetsu"]):
        return "construction"
    if "賃貸住宅" in joined or "rental_management" in joined:
        return "rental_management"
    if "マンション" in joined or "mansion_management" in joined:
        return "mansion_management"
    return value if value in BUSINESS_CATEGORIES else "unknown"


def infer_license_type(row: dict[str, Any]) -> str:
    source = pick(row, "source_type") or ""
    category = pick(row, "primary_business_category") or ""
    joined = f"{source} {category}"
    if any(t in joined for t in ["takken", "mlit_takken", "real_estate", "宅建"]):
        return "takken"
    if any(t in joined for t in ["kensetsu", "mlit_kensetsu", "construction", "建設"]):
        return "construction"
    if "rental_management" in joined or "mlit_rental_management" in joined:
        return "rental_management"
    if "mansion_management" in joined or "mlit_mansion_management" in joined:
        return "mansion_management"
    license_no = pick(row, "license_no") or ""
    if license_no:
        return "takken"
    if pick(row, "permit_no"):
        return "construction"
    return "unknown"


def normalize_company_row(row: dict[str, Any], center_lat: float, center_lon: float) -> dict[str, Any]:
    latitude = parse_float(pick(row, "latitude"))
    longitude = parse_float(pick(row, "longitude"))
    established_raw = pick(row, "established_raw")
    distance = None
    if latitude is not None and longitude is not None:
        distance = round(haversine_km(center_lat, center_lon, latitude, longitude), 2)
    ts = now_iso()
    company_name = pick(row, "company_name")

    return {
        "company_name": company_name,
        "company_name_normalized": normalize_company_name(company_name),
        "corporate_number": pick(row, "corporate_number"),
        "primary_business_category": infer_category(row),
        "source_type": pick(row, "source_type") or "csv_import",
        "source_name": pick(row, "source_name"),
        "source_record_id": pick(row, "source_record_id"),
        "prefecture": pick(row, "prefecture"),
        "city": pick(row, "city"),
        "ward": pick(row, "ward"),
        "address": pick(row, "address"),
        "tel": normalize_phone(pick(row, "tel")),
        "representative": pick(row, "representative"),
        "established_at": parse_japanese_date(established_raw),
        "established_raw": established_raw,
        "latitude": latitude,
        "longitude": longitude,
        "distance_km": distance,
        "source_url": pick(row, "source_url"),
        "source_updated_at": None,
        "imported_at": ts,
        "last_checked_at": None,
        "last_seen_at": ts,
        "last_manual_updated_at": None,
        "manual_updated_by": None,
        "is_active": True,
        "needs_review": False,
        "update_note": None,
        "created_at": ts,
        "updated_at": ts,
    }


def normalize_license_row(row: dict[str, Any]) -> dict[str, Any] | None:
    license_no = pick(row, "license_no")
    permit_no = pick(row, "permit_no")
    registration_no = pick(row, "registration_no")
    if not any([license_no, permit_no, registration_no]):
        return None
    ts = now_iso()
    return {
        "license_type": infer_license_type(row),
        "license_no": license_no,
        "permit_no": permit_no,
        "registration_no": registration_no,
        "authority": pick(row, "authority"),
        "office_type": pick(row, "office_type"),
        "source_type": pick(row, "source_type") or "csv_import",
        "source_name": pick(row, "source_name"),
        "source_url": pick(row, "source_url"),
        "source_record_id": pick(row, "source_record_id") or license_no or permit_no or registration_no,
        "last_seen_at": ts,
        "is_active": True,
        "needs_review": False,
        "created_at": ts,
        "updated_at": ts,
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
    category = clean_text(filters.get("primary_business_category"))
    if category and category != "all" and row.get("primary_business_category") != category:
        return False
    status = clean_text(filters.get("status"))
    if status and status != "all" and row.get("status") != status:
        return False
    keyword = clean_text(filters.get("q"))
    if keyword:
        target = " ".join(str(row.get(k) or "") for k in ["company_name", "address", "tel"])
        if keyword.lower() not in target.lower():
            return False
    exclude_q = clean_text(filters.get("exclude_q"))
    if exclude_q:
        target = " ".join(str(row.get(k) or "") for k in ["company_name", "address", "representative"])
        exclude_terms = [term.strip().lower() for term in exclude_q.replace(",", " ").split() if term.strip()]
        if any(term in target.lower() for term in exclude_terms):
            return False
    if clean_text(filters.get("has_tel")) == "1" and not row.get("tel"):
        return False
    if clean_text(filters.get("has_website")) == "1" and not row.get("website_url"):
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
    # 許認可フィルター（Supabase側ではPythonフィルタで処理）
    license_types_str = row.get("license_types") or ""
    license_types = set(license_types_str.split(",")) if license_types_str else set()
    for lt in filters.get("license_types") or []:
        if lt and lt not in license_types:
            return False
    return True
