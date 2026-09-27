import yaml

def load_config(config_name):

    path=f'/Volumes/klyra/datalakecode/raw_layer/yaml_files/{config_name}.yaml'

    with open(path,'r') as file:
        return yaml.safe_load(file)

