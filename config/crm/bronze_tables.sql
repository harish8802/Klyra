-- CRM bronze/source table definitions for batch ingestion.
-- Common metadata columns for all tables:
-- batch_id, buisness_date, source_name, record_version, ingested_at, updated_at
-- These columns are not part of the CSV files; they are added by the pipeline at write time.

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_customers (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    city STRING,
    state STRING,
    country STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_contacts (
    contact_id STRING,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    job_title STRING,
    department STRING,
    city STRING,
    state STRING,
    country STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_accounts (
    account_id STRING,
    account_name STRING,
    industry STRING,
    website STRING,
    phone STRING,
    billing_city STRING,
    billing_state STRING,
    billing_country STRING,
    annual_revenue STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_leads (
    lead_id STRING,
    lead_name STRING,
    company_name STRING,
    email STRING,
    phone STRING,
    source_channel STRING,
    status STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_opportunities (
    opportunity_id STRING,
    customer_id STRING,
    account_id STRING,
    opportunity_name STRING,
    stage STRING,
    amount STRING,
    close_date TIMESTAMP,
    probability STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_campaigns (
    campaign_id STRING,
    campaign_name STRING,
    campaign_type STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    budget STRING,
    status STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_activities (
    activity_id STRING,
    customer_id STRING,
    contact_id STRING,
    activity_type STRING,
    activity_subject STRING,
    activity_date TIMESTAMP,
    owner STRING,
    status STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_products (
    product_id STRING,
    product_name STRING,
    category STRING,
    unit_price STRING,
    currency STRING,
    stock_quantity STRING,
    is_active STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    order_date TIMESTAMP,
    quantity STRING,
    unit_price STRING,
    total_amount STRING,
    status STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.bronze.crm_invoices (
    invoice_id STRING,
    customer_id STRING,
    order_id STRING,
    invoice_date TIMESTAMP,
    due_date TIMESTAMP,
    total_amount STRING,
    status STRING,
    payment_status STRING,
    created_at TIMESTAMP,
    updated_at_raw TIMESTAMP,
    batch_id BIGINT,
    buisness_date TIMESTAMP,
    source_name STRING,
    record_version BIGINT,
    ingested_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;
