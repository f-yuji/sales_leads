# 営業先DB・連絡先収集アプリ

自分用の地域営業先DBツールです。最初の実用機能として、CSV取込、地域・半径フィルター、会社一覧、Supabase保存、営業ステータス管理、CSV出力を実装しています。

## 起動

```powershell
cd C:\Users\f-yuj\dev\sales_leads_app
python app.py
```

ブラウザで `http://127.0.0.1:5050` を開きます。

## Supabaseで使う

1. Supabase SQL Editorで `schema.sql` を実行します。
2. `.env.example` を `.env` にコピーします。
3. `SUPABASE_URL` と `SUPABASE_SERVICE_ROLE_KEY` を入れます。
4. `python app.py` で起動します。

Supabase環境変数が未設定の場合は `data/sales_leads.sqlite3` にローカル保存します。

既存DBに創業年月日ソート用の列を足す場合は、Supabase SQL Editorで `migration_add_established.sql` を実行します。その後、宅建CSVから設立年月日を反映できます。

```powershell
python scripts\backfill_takken_established_dates.py "C:\Users\f-yuj\Downloads\kokudo_all_takken_20240806_merged.csv"
python scripts\backfill_takken_established_dates.py "C:\Users\f-yuj\Downloads\kokudo_all_takken_20240806_merged.csv" --apply
```

## CSV取込ヘッダー

以下のような列名を自動認識します。

- 会社名: `会社名`, `商号`, `業者名`, `法人名`, `名称`
- 住所: `住所`, `所在地`, `本店所在地`, `事務所所在地`
- 電話: `電話`, `電話番号`, `TEL`, `代表電話`
- 免許/許可: `免許番号`, `宅建免許番号`, `許可番号`, `建設業許可番号`
- 位置: `緯度`, `経度`, `latitude`, `longitude`
- 連絡先: `公式サイト`, `メールアドレス`, `問い合わせフォーム`

緯度経度がある場合は東京駅からの距離をハバーサイン式で計算し、半径フィルターに使います。

## 国交省宅建CSVの50km候補取込

国交省の宅建CSVに緯度経度がない場合は、市区町村ホワイトリストで東京駅50km圏の候補だけを抽出します。

```powershell
python scripts\import_takken_csv.py "C:\Users\f-yuj\Downloads\kokudo_all_takken_20240806_merged.csv"
python scripts\import_takken_csv.py "C:\Users\f-yuj\Downloads\kokudo_all_takken_20240806_merged.csv" --apply
```

1行目はドライラン、2行目は実際にDBへ保存します。厳密な距離は、後続で住所ジオコードを入れて `latitude`、`longitude`、`distance_km` を更新します。

銀行、信託銀行、信用金庫、信用組合、保険、証券、協同組合など、営業対象になりにくい宅建免許保有者は取込時に除外します。すでに取り込んだ除外対象を確認・削除する場合は次を使います。

```powershell
python scripts\cleanup_excluded_companies.py
python scripts\cleanup_excluded_companies.py --apply
```

大手企業の支店重複を避けるため、宅建CSV取込では同じ会社名は最初の1件だけ保存します。すでに入った同名重複を確認・削除する場合は次を使います。

```powershell
python scripts\cleanup_duplicate_names.py
python scripts\cleanup_duplicate_names.py --apply
```

## 重複判定

優先順に近い形で、免許番号、許可番号、電話番号、会社名+住所、会社名+市区町村を見て既存レコードを更新します。

## 次に足す場所

公式サイト探索、メール抽出、問い合わせフォーム抽出は `services.py` に抽出ロジック、`app.py` に実行画面を追加していく想定です。
