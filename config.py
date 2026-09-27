import yaml

def load_config(config_name):

    path=f'config/{config_name}.yaml'

    with open(path,'r') as file:
        return yaml.safe_load(file)

