import yaml
from yaml.loader import SafeLoader
from pathlib import Path
from typing import Iterable, Any

def load_config() -> Iterable[str | dict[str, Any]]:
    with open(Path(__file__).parent / "config.yaml") as file:
        if isinstance((data :=  yaml.load(file, SafeLoader)), dict):
            return data.values()