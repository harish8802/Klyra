import logging
from pyspark.sql import SparkSession
from read import read_source
from config import load_config
from write import write_target

logger = logging.getLogger(__name__)

def main(job,buisness_date):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s - %(message)s'
    )
    logger.info('Starting job=%s business_date=%s', job, buisness_date)
    config =load_config(job)
    logger.info('Configuration loaded for job=%s', job)
    spark=SparkSession.builder.getOrCreate()
    print('Spark session ready')
    df = read_source(spark,config,buisness_date)
    logger.info('Source DataFrame prepared; beginning write')
    write_target(spark,df,config)
    logger.info('Job completed: job=%s business_date=%s', job, buisness_date)

if __name__ == "__main__":
    main("customers","2026-08-08")


