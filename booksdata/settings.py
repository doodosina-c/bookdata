import yaml
from yaml.loader import SafeLoader
from pathlib import Path

def load_config():
    with open(Path(__file__).parent / "config.yaml") as file:
        if isinstance((data :=  yaml.load(file, SafeLoader)), dict):
            return data.values()