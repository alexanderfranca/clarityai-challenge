from pathlib import Path
import sys
import yaml


BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = BASE_DIR / "configs"


def load_yaml(name: str) -> dict:
    """
    Load and validate a YAML config file.
    """
    path = CONFIG_DIR / name
    if not path.exists():
        sys.exit(f"Config file not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        sys.exit(f"YAML syntax error in {name}: {e}")

    if not isinstance(data, dict):
        sys.exit(f"Invalid format in {name}: expected mapping at top level")

    return data
