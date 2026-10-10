-- Data quality and job control tables
-- One job row per business date, and one DQ failure row per failing record.

CREATE TABLE IF NOT EXISTS klyra.control.job_run_control (
    job_run_id BIGINT,
    job_name STRING,
    source_name STRING,
    business_date TIMESTAMP,
    batch_id BIGINT,
    run_status STRING,
    run_type STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    records_read BIGINT,
    records_valid BIGINT,
    records_invalid BIGINT,
    records_inserted BIGINT,
    records_updated BIGINT,
    records_merged BIGINT,
    error_summary STRING,
    retry_count INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.control.dq_rules (
    rule_id BIGINT,
    job_name STRING,
    source_name STRING,
    table_name STRING,
    column_name STRING,
    rule_name STRING,
    rule_type STRING,
    expression STRING,
    threshold_value STRING,
    severity STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS klyra.audit.dq_failure_audit (
    dq_failure_id BIGINT,
    job_run_id BIGINT,
    job_name STRING,
    source_name STRING,
    business_date TIMESTAMP,
    table_name STRING,
    record_key STRING,
    column_name STRING,
    validation_rule STRING,
    validation_status STRING,
    error_details STRING,
    created_at TIMESTAMP
) USING DELTA;
