import sys
import yaml
from pathlib import Path
from utils.log import init_logger

CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs"
logger = init_logger()


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


def main() -> None:
    """
    Entry point of the pipeline.
    """
    logger.info("Loading pipeline contracts and configs.")

    contracts = load_yaml("contracts.yaml")
    mappings = load_yaml("mappings.yaml")
    paths = load_yaml("paths.yaml")

    logger.info(f"- contracts: {len(contracts)} top-level keys")
    logger.info(f"- mappings:  {len(mappings)} top-level keys")
    logger.info(f"- paths:     {len(paths)} top-level keys")
    logger.info("All contracts and configs loadded successfully.")


if __name__ == "__main__":
    main()

