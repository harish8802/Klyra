def find_table(table_config):
    catalog = table_config["catalog"]
    schema = table_config["schema"]
    table = table_config["table"]
    return f"{catalog}.{schema}.{table}"