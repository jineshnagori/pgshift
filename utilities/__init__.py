import yaml

def load_config(config_file):
    with open(config_file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        
        except yaml.YAMLError as e:
            print(e)
            return None