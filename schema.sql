create extension if not exists pgcrypto;

create table if not exists companies (
    id uuid primary key default gen_random_uuid(),
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
    established_at date,
    established_raw text,
    latitude double precision,
    longitude double precision,
    distance_km double precision,
    source_url text,
    source_updated_at timestamptz,
    imported_at timestamptz default now(),
    last_checked_at timestamptz,
    last_seen_at timestamptz,
    last_manual_updated_at timestamptz,
    manual_updated_by text,
    is_active boolean not null default true,
    needs_review boolean not null default false,
    is_branch boolean not null default false,
    is_bank_like boolean not null default false,
    update_note text,
    created_at timestamptz,
    updated_at timestamptz
);

create table if not exists company_licenses (
    id uuid primary key default gen_random_uuid(),
    company_id uuid not null references companies(id) on delete cascade,
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
    issued_at date,
    valid_from date,
    valid_to date,
    last_seen_at timestamptz,
    is_active boolean not null default true,
    needs_review boolean not null default false,
    memo text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists company_contacts (
    id uuid primary key default gen_random_uuid(),
    company_id uuid not null references companies(id) on delete cascade,
    website_url text,
    website_title text,
    email text,
    contact_form_url text,
    source_url text,
    confidence text,
    checked_at timestamptz,
    is_valid boolean not null default true,
    opt_out boolean not null default false,
    opt_out_at timestamptz,
    unsubscribe_token text,
    memo text,
    unique(company_id)
);

create table if not exists sales_status (
    id uuid primary key default gen_random_uuid(),
    company_id uuid not null references companies(id) on delete cascade,
    status text not null default '未対応',
    priority smallint not null default 0,
    last_contacted_at timestamptz,
    next_action_at date,
    memo text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(company_id)
);

create table if not exists import_logs (
    id uuid primary key default gen_random_uuid(),
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
    imported_at timestamptz not null default now(),
    memo text
);

create table if not exists company_update_logs (
    id uuid primary key default gen_random_uuid(),
    company_id uuid not null references companies(id) on delete cascade,
    license_id uuid references company_licenses(id),
    update_type text not null default 'manual',
    field_name text,
    old_value text,
    new_value text,
    updated_by text,
    update_note text,
    created_at timestamptz not null default now()
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
create index if not exists idx_sales_status_status on sales_status(status);
create index if not exists idx_update_logs_company on company_update_logs(company_id);
