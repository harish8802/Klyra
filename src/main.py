import logging
from pyspark.sql import SparkSession
from src.read import read_source
from src.config import load_config
from src.write import write_target

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
    df = read_source(spark,config,batch_details['business_date'])
    logger.info('Source DataFrame prepared; beginning write')
    write_target(spark,df,config,batch_details)
    logger.info('Job completed: job=%s business_date=%s', job, batch_details['business_date'])

if __name__ == "__main__":
    main("customers",{"business_date": "9999-01-01", "batch_id": 1, "source_name": "crm"})


