import logging
from src.utility import find_table
from datetime import datetime

logger = logging.getLogger(__name__)

def read_source(spark,config,business_date):
    logger.info('Preparing source read for business_date=%s', business_date)
    source=config['source']
    target_config =config['target']
    dt=datetime.strptime(business_date,'%Y-%m-%d')
    file=source['file_format'].replace('{dd}',dt.strftime('%d'))\
                            .replace('{MM}',dt.strftime('%m'))\
                            .replace('{yyyy}',dt.strftime('%Y'))
    file_path = f"{source['path'].rstrip('/')}/{file}"

    if source['type'] =='file':
        table_name=find_table(target_config['raw'])
        table =spark.table(table_name)
        table = table.drop('batch_id','buisness_date','source_name','record_version','ingested_at','updated_at')
        schema =table.schema
        logger.info('Reading CSV path=%s schema=%s', file_path, schema.simpleString())
        return (spark.read\
                .format(source['format'])\
                .option('header',source['header'])\
                .option('timestampFormat',source['timestamp_format'])\
                .option('dateFormat',source['date_format'])\
                .option('delimiter',source['delimiter'])\
                .schema(schema)\
                .load(file_path)\
                )
    else:
        raise ValueError(f"Unsupported source type: {source['type']}")




