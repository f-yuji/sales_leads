"""
国交省 etsuran2 スクレイパー

宅建・建設業・賃貸管理・マンション管理の母集団データを
直接スクレイピングして DB に保存する。CSV ダウンロード不要。

使用例:
  python scripts/fetch_mlit_companies.py --category takken --prefecture 神奈川県 --apply
  python scripts/fetch_mlit_companies.py --category construction --prefecture 東京都 --limit 200 --apply
  python scripts/fetch_mlit_companies.py --category takken --prefecture 神奈川県 --apply --mark-missing
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import normalize_company_row, normalize_license_row
from storage import create_store

BASE_URL = "https://etsuran2.mlit.go.jp/TAKKEN/"
CENTER_LAT = 35.681236
CENTER_LON = 139.767125
SLEEP_SEC = 0.7
MAX_PAGES = 2000
REQUEST_TIMEOUT = (10, 90)
EMPTY_PAGE_RETRIES = 3
PROGRESS_DIR = Path("data/import_progress")

PREF_CODES: dict[str, str] = {
    "北海道": "01", "青森県": "02", "岩手県": "03", "宮城県": "04",
    "秋田県": "05", "山形県": "06", "福島県": "07", "茨城県": "08",
    "栃木県": "09", "群馬県": "10", "埼玉県": "11", "千葉県": "12",
    "東京都": "13", "神奈川県": "14", "新潟県": "15", "富山県": "16",
    "石川県": "17", "福井県": "18", "山梨県": "19", "長野県": "20",
    "岐阜県": "21", "静岡県": "22", "愛知県": "23", "三重県": "24",
    "滋賀県": "25", "京都府": "26", "大阪府": "27", "兵庫県": "28",
    "奈良県": "29", "和歌山県": "30", "鳥取県": "31", "島根県": "32",
    "岡山県": "33", "広島県": "34", "山口県": "35", "徳島県": "36",
    "香川県": "37", "愛媛県": "38", "高知県": "39", "福岡県": "40",
    "佐賀県": "41", "長崎県": "42", "熊本県": "43", "大分県": "44",
    "宮崎県": "45", "鹿児島県": "46", "沖縄県": "47",
}

PREF_NAMES = list(PREF_CODES.keys())

CATEGORY_CONFIG: dict[str, dict] = {
    "takken": {
        "url": BASE_URL + "takkenKensaku.do",
        "source_type": "mlit_takken",
        "primary_business_category": "real_estate",
        "license_field": "license_no",
        "extra_params": {},
    },
    "construction": {
        "url": BASE_URL + "kensetuKensaku.do",
        "source_type": "mlit_kensetsu",
        "primary_business_category": "construction",
        "license_field": "permit_no",
        "extra_params": {
            "gyosyu": "", "gyosyuType": "",
            "sv_gyosyu": "", "sv_gyosyuType": "",
        },
    },
    "rental_management": {
        "url": BASE_URL + "chintaiKensaku.do",
        "source_type": "mlit_rental_management",
        "primary_business_category": "rental_management",
        "license_field": "registration_no",
        "extra_params": {},
    },
    "mansion_management": {
        "url": BASE_URL + "mansionKensaku.do",
        "source_type": "mlit_mansion_management",
        "primary_business_category": "mansion_management",
        "license_field": "registration_no",
        "extra_params": {},
    },
}

EXCLUDED_KEYWORDS = {
    "銀行", "信託銀行", "信用金庫", "信用組合", "労働金庫",
    "農業協同組合", "漁業協同組合", "協同組合", "証券",
    "生命保険", "損害保険", "保険", "共済",
    "独立行政法人", "地方公共団体",
}


@dataclass
class FetchResult:
    category: str
    prefecture: str
    total_fetched: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    pages: int = 0
    total_pages: int = 0
    result_count: int = 0
    error: str | None = None

    def summary(self) -> str:
        lines = [
            f"カテゴリ: {self.category} / 都道府県: {self.prefecture}",
            f"総件数: {self.result_count:,} / ページ数: {self.total_pages}",
            f"取得: {self.total_fetched:,} / 新規: {self.created:,} / 更新: {self.updated:,} / スキップ: {self.skipped}",
        ]
        if self.error:
            lines.append(f"エラー: {self.error}")
        return "\n".join(lines)


def progress_path(category: str, pref_name: str) -> Path:
    safe_pref = re.sub(r"[^0-9A-Za-z一-龥ぁ-んァ-ヶー_-]+", "_", pref_name)
    return PROGRESS_DIR / f"mlit_{category}_{safe_pref}.json"


def load_progress(category: str, pref_name: str) -> dict:
    path = progress_path(category, pref_name)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_progress(category: str, pref_name: str, data: dict) -> None:
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    path = progress_path(category, pref_name)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def resolve_pref_code(prefecture: str) -> str:
    stripped = prefecture.strip()
    # 数字コードをそのまま使う
    if stripped.isdigit() and len(stripped) <= 2:
        return stripped.zfill(2)
    # 都道府県名のあいまい一致
    for name, code in PREF_CODES.items():
        if stripped in name or name in stripped:
            return code
    raise ValueError(f"都道府県名が不明: {prefecture!r}  例: 神奈川県 / 東京都 / 埼玉県")


def parse_address(address: str) -> tuple[str, str, str]:
    prefecture = ""
    rest = address
    for p in PREF_NAMES:
        if address.startswith(p):
            prefecture = p
            rest = address[len(p):]
            break

    city = ""
    ward = ""
    if rest:
        m = re.match(r"(.+?[市区町村])", rest)
        if m:
            candidate = m.group(1)
            if candidate.endswith("区") and prefecture != "東京都":
                # 政令市の区が最初にマッチ → 先に市を探す
                city_m = re.match(r"(.+?市)", rest)
                if city_m:
                    city = city_m.group(1)
                    ward_m = re.match(r".+?市(.+?区)", rest)
                    if ward_m:
                        ward = ward_m.group(1)
                else:
                    city = candidate
            else:
                city = candidate
                if prefecture == "東京都" and candidate.endswith("区"):
                    ward = candidate
                elif city.endswith("市"):
                    # 政令市の区（川崎市中原区 など）を取得
                    ward_m = re.match(r".+?市(.+?区)", rest)
                    if ward_m:
                        ward = ward_m.group(1)
    return prefecture, city, ward


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def decode_response(resp: requests.Response) -> str:
    return resp.content.decode("shift_jis", errors="replace")


def build_form_data(soup: BeautifulSoup) -> dict[str, str]:
    fields: dict[str, str] = {}
    form = soup.find("form", attrs={"name": "tkModel"}) or soup.find("form")
    if not form:
        return fields
    for tag in form.find_all(["input", "select"]):
        name = tag.get("name")
        if not name:
            continue
        if tag.name == "input":
            itype = tag.get("type", "text").lower()
            if itype in ("submit", "button", "image", "reset"):
                continue
            if itype == "radio":
                if tag.get("checked"):
                    fields[name] = tag.get("value", "on")
                else:
                    fields.setdefault(name, tag.get("value", ""))
            elif itype == "checkbox":
                if tag.get("checked"):
                    fields[name] = tag.get("value", "on")
            else:
                fields.setdefault(name, tag.get("value", ""))
        elif tag.name == "select":
            opts = tag.find_all("option")
            selected = next((o for o in opts if o.get("selected")), opts[0] if opts else None)
            if selected:
                fields[name] = selected.get("value", "")
    return fields


def extract_data_rows(soup: BeautifulSoup) -> list[Tag]:
    # 全カテゴリ共通: 免許/許可/登録番号セルには title="licenseNo" 属性がある
    # これを使って確実にデータ行を特定する（無効なネスト構造に依存しない）
    rows = []
    seen_ids = set()
    for td in soup.find_all("td", attrs={"title": "licenseNo"}):
        tr = td.parent
        if tr and tr.name == "tr" and id(tr) not in seen_ids:
            seen_ids.add(id(tr))
            rows.append(tr)
    return rows


def parse_row(tr: Tag, config: dict, source_name: str) -> dict | None:
    tds = tr.find_all("td")
    if len(tds) < 5:
        return None

    # 免許/登録番号セルは title="licenseNo" 属性で特定
    license_td = next((td for td in tds if td.get("title") == "licenseNo"), None)

    # 会社名セルは <a> タグを持つセル
    name_td = next((td for td in tds if td.find("a")), None)
    if not name_td:
        return None

    company_name = name_td.get_text(strip=True)
    if not company_name:
        return None
    if any(kw in company_name for kw in EXCLUDED_KEYWORDS):
        return None

    link = name_td.find("a")
    source_record_id = ""
    if link:
        m = re.search(r"js_ShowDetail\('(\w+)'\)", link.get("onclick", ""))
        if m:
            source_record_id = m.group(1)

    # 会社名以降のセル: 代表者 / 事務所名 / 住所（7列構造・6列構造どちらも対応）
    name_idx = tds.index(name_td)
    rep_td = tds[name_idx + 1] if name_idx + 1 < len(tds) else None
    off_td = tds[name_idx + 2] if name_idx + 2 < len(tds) else None
    addr_td = tds[-1]  # 最後のセルが住所

    address = addr_td.get_text(strip=True)
    prefecture, city, ward = parse_address(address)

    # 行政庁は免許番号セルの1つ前（7列構造のみ存在）
    authority = ""
    if license_td:
        lic_idx = tds.index(license_td)
        if lic_idx > 1:
            authority = tds[lic_idx - 1].get_text(strip=True)

    row: dict = {
        "company_name": company_name,
        "address": address,
        "prefecture": prefecture,
        "city": city,
        "ward": ward,
        "representative": rep_td.get_text(strip=True) if rep_td else None,
        "office_type": off_td.get_text(strip=True) if off_td else None,
        "authority": authority or None,
        "source_record_id": source_record_id or None,
        "source_type": config["source_type"],
        "source_name": source_name,
        "source_url": config["url"],
        "primary_business_category": config["primary_business_category"],
    }

    if license_td:
        license_text = license_td.get_text(strip=True)
        if license_text:
            row[config["license_field"]] = license_text

    return row


def _load_existing_source_ids(store, source_type: str) -> set[str]:
    """DBに登録済みの source_record_id を一括取得する（--create-only 用）"""
    from storage import SQLiteStore, SupabaseStore
    if isinstance(store, SQLiteStore):
        with store.connect() as conn:
            rows = conn.execute(
                "SELECT source_record_id FROM companies WHERE source_type = ? AND source_record_id IS NOT NULL",
                (source_type,)
            ).fetchall()
        return {r["source_record_id"] for r in rows}
    elif isinstance(store, SupabaseStore):
        existing: set[str] = set()
        offset = 0
        while True:
            res = (
                store.client.table("companies")
                .select("source_record_id")
                .eq("source_type", source_type)
                .not_.is_("source_record_id", "null")
                .range(offset, offset + 999)
                .execute()
            )
            batch = res.data or []
            existing.update(r["source_record_id"] for r in batch if r.get("source_record_id"))
            if len(batch) < 1000:
                break
            offset += 1000
        return existing
    return set()


def run_fetch(
    category: str,
    prefecture: str,
    apply: bool = False,
    limit: int = 0,
    start_page: int = 1,
    resume: bool = False,
    mark_missing: bool = False,
    create_only: bool = False,
    verbose: bool = True,
) -> FetchResult:
    if category not in CATEGORY_CONFIG:
        raise ValueError(f"未対応カテゴリ: {category}  対応: {list(CATEGORY_CONFIG)}")

    config = CATEGORY_CONFIG[category]
    pref_code = resolve_pref_code(prefecture)
    pref_name = next((n for n, c in PREF_CODES.items() if c == pref_code), prefecture)

    result = FetchResult(category=category, prefecture=pref_name)
    session = make_session()
    store = create_store() if apply else None
    source_name = f"mlit_{category}_{pref_name}_{date.today()}"

    def log(msg: str) -> None:
        if verbose:
            print(msg, flush=True)

    progress = load_progress(category, pref_name) if resume else {}
    if resume and progress.get("last_successful_page"):
        start_page = max(start_page, int(progress["last_successful_page"]) + 1)
        log(f"進捗ファイルから再開します: page {start_page} ({progress_path(category, pref_name)})")
    elif resume:
        log(f"進捗ファイルがないため page {start_page} から開始します: {progress_path(category, pref_name)}")

    # 初期フォーム取得
    try:
        r0 = session.get(config["url"], timeout=REQUEST_TIMEOUT)
        r0.raise_for_status()
        soup0 = BeautifulSoup(decode_response(r0), "html.parser")
        form_data = build_form_data(soup0)
        if not form_data:
            raise RuntimeError("search form was not found")
    except Exception as e:
        result.error = f"初期フォーム取得失敗: {e}"
        return result

    existing_ids: set[str] = set()
    if apply and create_only:
        log("既存 source_record_id を取得中...")
        existing_ids = _load_existing_source_ids(store, config["source_type"])
        log(f"  既存 {len(existing_ids):,} 件をスキップ対象に設定")

    touched_ids: list = []
    start_page = max(1, start_page)
    page = 1

    while page <= MAX_PAGES:
        if page > 1:
            time.sleep(SLEEP_SEC)

        req_data = dict(form_data)
        if page == 1:
            req_data.update({
                "CMD": "search",
                "kenCode": pref_code,
                "choice": "1",  # 本店のみ
                "dispCount": "50",
                "dispPage": "1",
                "resultCount": "0",
                "pageCount": "0",
            })
        else:
            req_data.update({
                "CMD": "next",
                "dispPage": str(page),
            })
        req_data.update({
            "kenCode": pref_code,
            "choice": "1",
            "dispCount": "50",
        })
        req_data.update(config["extra_params"])

        try:
            resp = session.post(config["url"], data=req_data, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            content = decode_response(resp)
            soup = BeautifulSoup(content, "html.parser")
        except Exception as e:
            log(f"  ERROR page {page}: {e}")
            result.error = str(e)
            break

        # 結果ページの hidden fields を更新
        for inp in soup.find_all("input", type="hidden"):
            n = inp.get("name", "")
            if n:
                form_data[n] = inp.get("value", "")

        if page == 1:
            result.total_pages = int(form_data.get("pageCount", "0") or "0")
            result.result_count = int(form_data.get("resultCount", "0") or "0")
            if start_page > 1:
                log(f"\n[{category}] {pref_name}: {result.result_count:,}件 / {result.total_pages}ページ (page {start_page} まで保存スキップ)")
            else:
                log(f"\n[{category}] {pref_name}: {result.result_count:,}件 / {result.total_pages}ページ")
            if result.total_pages == 0:
                result.error = "検索結果ページを取得できませんでした（国交省側が0件/空ページを返しました）"
                log(f"  ERROR: {result.error}")
                break

        data_rows = extract_data_rows(soup)
        empty_retries = 0
        while not data_rows and page > 1 and empty_retries < EMPTY_PAGE_RETRIES:
            empty_retries += 1
            wait_sec = empty_retries * 3
            log(f"  page {page}: データ行なし、{wait_sec}秒後に再試行 ({empty_retries}/{EMPTY_PAGE_RETRIES})")
            time.sleep(wait_sec)
            try:
                resp = session.post(config["url"], data=req_data, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                content = decode_response(resp)
                soup = BeautifulSoup(content, "html.parser")
                data_rows = extract_data_rows(soup)
            except Exception as e:
                log(f"  ERROR retry page {page}: {e}")

        if not data_rows and page > 1:
            log(f"  page {page}: データ行なし、終了")
            break
        for tr in data_rows:
            row = parse_row(tr, config, source_name)
            if not row:
                result.skipped += 1
                continue
            if row.get("prefecture") and row.get("prefecture") != pref_name:
                log(f"  SKIP prefecture mismatch: expected={pref_name} got={row.get('prefecture')} {row.get('company_name')}")
                result.skipped += 1
                continue
            if page < start_page:
                continue

            result.total_fetched += 1

            if limit and not apply and result.total_fetched >= limit:
                break

            if not apply:
                continue

            if limit and (result.created + result.updated) >= limit:
                break

            if create_only and row.get("source_record_id") in existing_ids:
                continue

            try:
                company = normalize_company_row(row, CENTER_LAT, CENTER_LON)
                license_data = normalize_license_row(row)
                company_id, was_created = store.upsert_company(company, license=license_data)
                touched_ids.append(company_id)
                if was_created:
                    result.created += 1
                else:
                    result.updated += 1
            except Exception as e:
                if "ConnectionTerminated" in str(e) or "ConnectionNotAvailable" in str(e):
                    # Supabase HTTP/2 接続リセット → store を再生成してリトライ
                    time.sleep(2)
                    store = create_store()
                    try:
                        company_id, was_created = store.upsert_company(company, license=license_data)
                        touched_ids.append(company_id)
                        if was_created:
                            result.created += 1
                        else:
                            result.updated += 1
                    except Exception as e2:
                        log(f"  ERROR upsert retry {row.get('company_name')}: {e2}")
                        result.skipped += 1
                else:
                    log(f"  ERROR upsert {row.get('company_name')}: {e}")
                    result.skipped += 1

        result.pages += 1

        if apply and page >= start_page:
            save_progress(category, pref_name, {
                "category": category,
                "prefecture": pref_name,
                "last_successful_page": page,
                "next_page": page + 1,
                "total_pages": result.total_pages,
                "result_count": result.result_count,
                "created": result.created,
                "updated": result.updated,
                "skipped": result.skipped,
                "updated_at": date.today().isoformat(),
            })

        if result.pages % 20 == 0:
            log(f"  {page}/{result.total_pages}ページ完了  新規:{result.created} 更新:{result.updated}")

        reached_limit = (
            (limit and not apply and result.total_fetched >= limit) or
            (limit and apply and (result.created + result.updated) >= limit)
        )
        if reached_limit:
            log(f"  --limit {limit}件に達したため終了")
            break

        if page >= result.total_pages:
            if apply and result.total_pages:
                save_progress(category, pref_name, {
                    "category": category,
                    "prefecture": pref_name,
                    "last_successful_page": page,
                    "next_page": None,
                    "total_pages": result.total_pages,
                    "result_count": result.result_count,
                    "created": result.created,
                    "updated": result.updated,
                    "skipped": result.skipped,
                    "completed": True,
                    "updated_at": date.today().isoformat(),
                })
            break

        page += 1

    if mark_missing and apply and touched_ids:
        _mark_missing(store, touched_ids, config["source_type"], source_name)

    return result


def _mark_missing(store, touched_ids: list, source_type: str, source_name: str) -> None:
    print("\n未検出レコードをチェック中...")
    from storage import SQLiteStore, SupabaseStore

    if isinstance(store, SQLiteStore):
        touched_set = {str(i) for i in touched_ids}
        with store.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM companies WHERE source_type = ? AND is_active = 1",
                (source_type,)
            ).fetchall()
            missing = [r["id"] for r in existing if str(r["id"]) not in touched_set]
            if missing:
                conn.execute(
                    f"UPDATE companies SET needs_review = 1, update_note = ? "
                    f"WHERE id IN ({','.join('?' * len(missing))})",
                    [f"再取込時未検出 ({source_name})"] + missing,
                )
                print(f"needs_review にした件数: {len(missing)}")
            else:
                print("未検出レコードなし")

    elif isinstance(store, SupabaseStore):
        touched_set = {str(i) for i in touched_ids}
        existing = (
            store.client.table("companies")
            .select("id").eq("source_type", source_type).eq("is_active", True)
            .execute()
        )
        missing = [r["id"] for r in (existing.data or []) if r["id"] not in touched_set]
        if missing:
            for start in range(0, len(missing), 500):
                chunk = missing[start:start + 500]
                store.client.table("companies").update({
                    "needs_review": True,
                    "update_note": f"再取込時未検出 ({source_name})",
                }).in_("id", chunk).execute()
            print(f"needs_review にした件数: {len(missing)}")
        else:
            print("未検出レコードなし")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="国交省 etsuran2 から会社データをスクレイピングして DB に保存する"
    )
    parser.add_argument(
        "--category",
        required=True,
        choices=list(CATEGORY_CONFIG),
        help="業種: takken / construction / rental_management / mansion_management",
    )
    parser.add_argument("--prefecture", required=True, help="都道府県名 例: 神奈川県 / 東京都")
    parser.add_argument("--apply", action="store_true", help="DB に書き込む（省略時は dry-run）")
    parser.add_argument("--limit", type=int, default=0, help="最大取込件数（テスト用）")
    parser.add_argument("--start-page", type=int, default=1, help="再開するページ番号")
    parser.add_argument("--resume", action="store_true", help="進捗ファイルから自動再開する")
    parser.add_argument("--create-only", action="store_true",
                        help="DBに未登録の新規レコードだけ保存する（既存はスキップ）")
    parser.add_argument("--mark-missing", action="store_true",
                        help="今回取得できなかった既存レコードを needs_review=true にする")
    args = parser.parse_args()

    result = run_fetch(
        category=args.category,
        prefecture=args.prefecture,
        apply=args.apply,
        limit=args.limit,
        start_page=args.start_page,
        resume=args.resume,
        mark_missing=args.mark_missing,
        create_only=args.create_only,
        verbose=True,
    )

    print(f"\n=== 結果 ===")
    print(result.summary())
    if not args.apply:
        print("\n（dry-run: --apply を付けると実際に書き込みます）")


if __name__ == "__main__":
    main()
