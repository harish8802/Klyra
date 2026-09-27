import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utility import find_table

logger = logging.getLogger(__name__)

def append_table(df,table_config):
    table=find_table(table_config)
    logger.info('Appending to table=%s', table)
    result = df.write\
        .mode('append')\
        .format(table_config['format'])\
        .saveAsTable(table)
    logger.info('Append completed: table=%s', table)
    return result

def overwrite_table(df,table_config):
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
    update_set=','.join([f'm.{col}=r.{col}' for col in df.columns if col not in ['updated_at','record_version']])
    update_set+=',m.updated_at=current_timestamp(),m.record_version=m.record_version+r.record_version'
    overwrite_table(spark,df,target_config['raw'])
    merge_sql = f'''merge into {master_table} as m using {raw_table} as r
     on m.{join_key} = r.{join_key}
     when matched then update set {update_set}
     when not matched then insert *'''
    print(f'Merge SQL to execute:\n{merge_sql}')
    spark.sql(merge_sql)
    logger.info('Merge completed successfully: target=%s', master_table)

def write_target(spark,df,config,batch_details):
    target_config=config['target']
    batch_id = batch_details['batch_id']
    business_date = batch_details['business_date']
    source_name = batch_details['source_name']
    logger.info('Preparing metadata columns for write')
    df=df.withColumn('batch_id',lit(batch_id).cast('bigint'))\
        .withColumn('buisness_date',lit(business_date).cast('timestamp'))\
        .withColumn('source_name',lit(source_name).cast('string'))\
        .withColumn('record_version',lit(1).cast('bigint'))\
        .withColumn('ingested_at',current_timestamp())\
        .withColumn('updated_at',lit(None).cast('timestamp'))
    
    if target_config['write']['mode']=='merge':
        merge_table(spark,df,target_config)
    elif target_config['write']['mode']=='overwrite':
        overwrite_table(df,target_config['master'])
    elif target_config['write']['mode']=='append':
        append_table(df,target_config['master']) 
        

    