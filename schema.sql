create extension if not exists pgcrypto;

create table if not exists companies (
    id uuid primary key default gen_random_uuid(),
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
    latitude double precision,
    longitude double precision,
    distance_km double precision,
    source_url text,
    source_updated_at timestamptz,
    imported_at timestamptz default now(),
    last_checked_at timestamptz,
    is_active boolean not null default true
);

create table if not exists company_contacts (
    id uuid primary key default gen_random_uuid(),
    company_id uuid not null references companies(id) on delete cascade,
    website_url text,
    email text,
    contact_form_url text,
    source_url text,
    confidence text,
    checked_at timestamptz,
    is_valid boolean not null default true,
    memo text,
    unique(company_id)
);

create table if not exists sales_status (
    id uuid primary key default gen_random_uuid(),
    company_id uuid not null references companies(id) on delete cascade,
    status text not null default '未対応',
    last_contacted_at timestamptz,
    next_action_at date,
    memo text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(company_id)
);

create index if not exists idx_companies_license on companies(license_no);
create index if not exists idx_companies_permit on companies(permit_no);
create index if not exists idx_companies_tel on companies(tel);
create index if not exists idx_companies_location on companies(prefecture, city, ward);
create index if not exists idx_companies_distance on companies(distance_km);
create index if not exists idx_sales_status_status on sales_status(status);
