import logging
from pyspark.sql import SparkSession
from src.read import read_source
from src.config import load_config
from src.write import write_target
from src.validate import ValidateEngine
from src.job_control import finish_job_run, start_job_run

logger = logging.getLogger(__name__)

def main(job,batch_details):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s - %(message)s'
    )
    logger.info('Starting job=%s business_date=%s', job, batch_details['business_date'])
    config =load_config(job)
    logger.info('Configuration loaded for job=%s', job)
    spark=SparkSession.builder.getOrCreate()
    print('Spark session ready')
    job_run_id = start_job_run(spark, config, batch_details)
    records_read = None
    records_valid = None

    try:
        df = read_source(spark,config,batch_details['business_date'])
        records_read = df.count()
        logger.info('Source DataFrame prepared; beginning write')
        validate_engine = ValidateEngine()
        validation_results = validate_engine.validate(spark, df, config)
        records_valid = records_read
        write_target(spark,df,config,batch_details)
        finish_job_run(
            spark,
            job_run_id,
            "SUCCEEDED",
            records_read=records_read,
            records_valid=records_valid,
            records_invalid=0,
        )
    except Exception as error:
        logger.exception(
            'Job failed: job=%s business_date=%s',
            job,
            batch_details['business_date'],
        )
        try:
            finish_job_run(
                spark,
                job_run_id,
                "FAILED",
                records_read=records_read,
                records_valid=records_valid,
                records_invalid=(
                    records_read - records_valid
                    if records_read is not None and records_valid is not None
                    else None
                ),
                error_summary=str(error),
            )
        except Exception:
            logger.exception(
                'Failed to update control record for job_run_id=%s',
                job_run_id,
            )
        raise

    logger.info('Job completed: job=%s business_date=%s', job, batch_details['business_date'])

if __name__ == "__main__":
    main("customers",{"business_date": "9999-01-01", "batch_id": 1, "source_name": "crm"})

