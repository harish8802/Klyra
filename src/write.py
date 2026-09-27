import logging
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utility import find_table

logger = logging.getLogger(__name__)

def overwrite_table(spark,df,table_config):
    table=find_table(table_config)
    logger.info('Overwriting raw table=%s', table)
    result = df.write\
        .mode('overwrite')\
        .format(table_config['format'])\
        .saveAsTable(table)
    logger.info('Raw table overwrite completed: table=%s', table)
    return result

def merge_table(spark,df,target_config):
    raw_table=find_table(target_config['raw'])
    master_table =find_table(target_config['master'])
    join_key =target_config['write']['keys']
    logger.info('Starting merge: source=%s target=%s key=%s', raw_table, master_table, join_key)
    overwrite_table(spark,df,target_config['raw'])
    merge_sql = f'''merge into {master_table} as m using {raw_table} as r
     on m.{join_key} = r.{join_key}
     when matched then update set *
     when not matched then insert *'''
    print(f'Merge SQL to execute:\n{merge_sql}')
    spark.sql(merge_sql)
    logger.info('Merge completed successfully: target=%s', master_table)

def write_target(spark,df,config):
    target_config=config['target']
    logger.info('Preparing metadata columns for write')
    df=df.withColumn('batch_id',lit(None).cast('bigint'))\
        .withColumn('source_name',lit(None).cast('string'))\
        .withColumn('record_version',lit(None).cast('bigint'))\
        .withColumn('ingested_at',lit(None).cast('timestamp'))\
        .withColumn('updated_at',lit(None).cast('timestamp'))
    
    if target_config['write']['mode']=='merge':
        merge_table(spark,df,target_config)
        

    