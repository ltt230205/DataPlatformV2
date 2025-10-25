import json


def read_json(json_path):
    with open(json_path) as f:
        conf = json.load(f)
    return conf
