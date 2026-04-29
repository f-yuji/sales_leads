alter table companies
    add column if not exists established_at date,
    add column if not exists established_raw text;

create index if not exists idx_companies_name on companies(company_name);
create index if not exists idx_companies_established on companies(established_at);
