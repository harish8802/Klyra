import logging
import time
from datetime import datetime
from typing import Any, Optional

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

JOB_RUN_CONTROL_TABLE = "klyra.control.job_run_control"

JOB_RUN_CONTROL_SCHEMA = StructType(
    [
        StructField("job_run_id", LongType(), nullable=False),
        StructField("job_name", StringType(), nullable=False),
        StructField("source_name", StringType(), nullable=True),
        StructField("business_date", TimestampType(), nullable=True),
        StructField("batch_id", LongType(), nullable=True),
        StructField("run_status", StringType(), nullable=False),
        StructField("run_type", StringType(), nullable=True),
        StructField("started_at", TimestampType(), nullable=False),
        StructField("finished_at", TimestampType(), nullable=True),
        StructField("records_read", LongType(), nullable=True),
        StructField("records_valid", LongType(), nullable=True),
        StructField("records_invalid", LongType(), nullable=True),
        StructField("records_inserted", LongType(), nullable=True),
        StructField("records_updated", LongType(), nullable=True),
        StructField("records_merged", LongType(), nullable=True),
        StructField("error_summary", StringType(), nullable=True),
        StructField("retry_count", IntegerType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)


def start_job_run(spark: Any, job_config: dict, batch_details: dict) -> int:
    job_run_id = time.time_ns()
    started_at = datetime.now()
    business_date = datetime.strptime(batch_details["business_date"], "%Y-%m-%d")
    job_name = job_config["job"]["name"]
    source_name = batch_details.get("source_name", job_config["source"]["name"])
    run_type = job_config["job"].get("mode", job_config["job"].get("type"))

    row = (
        job_run_id,
        job_name,
        source_name,
        business_date,
        batch_details["batch_id"],
        "RUNNING",
        run_type,
        started_at,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        started_at,
        started_at,
    )

    logger.info(
        "Inserting RUNNING job control record: job=%s batch_id=%s job_run_id=%s",
        job_name,
        batch_details["batch_id"],
        job_run_id,
    )
    try:
        spark.createDataFrame([row], JOB_RUN_CONTROL_SCHEMA).write.format("delta").mode(
            "append"
        ).saveAsTable(JOB_RUN_CONTROL_TABLE)
    except Exception:
        logger.exception(
            "Failed to insert job control record: job=%s job_run_id=%s",
            job_name,
            job_run_id,
        )
        raise
    return job_run_id


def finish_job_run(
    spark: Any,
    job_run_id: int,
    run_status: str,
    records_read: Optional[int] = None,
    records_valid: Optional[int] = None,
    records_invalid: Optional[int] = None,
    error_summary: Optional[str] = None,
) -> None:
    def sql_value(value: Optional[int]) -> str:
        return "NULL" if value is None else str(int(value))

    error_value = (
        "NULL"
        if error_summary is None
        else "'" + error_summary.replace("'", "''") + "'"
    )
    update_sql = f"""
        UPDATE {JOB_RUN_CONTROL_TABLE}
        SET run_status = '{run_status}',
            finished_at = current_timestamp(),
            records_read = {sql_value(records_read)},
            records_valid = {sql_value(records_valid)},
            records_invalid = {sql_value(records_invalid)},
            error_summary = {error_value},
            updated_at = current_timestamp()
        WHERE job_run_id = {int(job_run_id)}
    """

    spark.sql(update_sql)
    logger.info(
        "Updated job control record: job_run_id=%s status=%s",
        job_run_id,
        run_status,
    )
