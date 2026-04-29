update sales_status
set status = case
    when status in ('未対応', 'サイト確認中', 'メール取得済み', '問い合わせフォーム取得済み', '対応中', '保留') then '未対応'
    when status in ('送信済み') then '送信済み'
    when status in ('返信あり', '見込みあり') then '返信あり'
    when status in ('要フォロー', 'フォロー', '要対応') then '要フォロー'
    when status in ('バウンス', '不達') then 'バウンス'
    when status in ('クローズ', 'NG') then 'クローズ'
    else '未対応'
end
where status not in ('未対応', '送信済み', '返信あり', '要フォロー', 'バウンス', 'クローズ');
