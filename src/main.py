import sys
import yaml
from typing import (
        Dict,
        List,
)
from pathlib import Path
from utils.log import init_logger
from datetime import datetime, timezone


CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs"
DATA_INCOMING = Path(__file__).resolve().parents[1] / "data" / "incoming"
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


def discover_batches(paths_cfg: dict) -> Dict[str, List[str]]:
    """
    Scan provider incoming folders defined in paths.yaml for ready batches.
    A batch is ready if it has a '_READY' marker (or quarantine elapsed).
    """
    result: Dict[str, List[str]] = {}
    for provider, cfg in paths_cfg.get("providers", {}).items():
        incoming_dir = Path(
                cfg.get("incoming_dir", f"data/incoming/{provider}")
                ).resolve()
        readiness = cfg.get("readiness", {}).get("marker_file", "_READY")
        quarantine_sec = cfg.get("readiness", {}).get("quarantine_seconds", 0)

        if not incoming_dir.exists():
            logger.warning(f"Provider path missing: {incoming_dir}")
            continue

        ready_batches: List[str] = []

        for batch_dir in sorted(incoming_dir.iterdir()):
            if not batch_dir.is_dir():
                continue

            marker = batch_dir / readiness
            if marker.exists():
                ready_batches.append(batch_dir.name)
                logger.info(f"Ready batch: {provider}/{batch_dir.name}")
                continue

            if quarantine_sec > 0:
                age = (
                        datetime.now(timezone.utc) - datetime.fromtimestamp(
                            batch_dir.stat().st_mtime, tz=timezone.utc)
                        ).total_seconds()
                if age >= quarantine_sec:
                    logger.info(
                            "Quarantine passed: "
                            f"{provider}/{batch_dir.name}"
                            )
                    ready_batches.append(batch_dir.name)

        if ready_batches:
            result[provider] = ready_batches

    if not result:
        logger.warning("No ready batches found.")
    else:
        total = sum(len(v) for v in result.values())
        logger.info(f"Discovered {total} ready batch(es).")

    return result


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

    batches = discover_batches(paths)
    logger.info(f"Found batches: {batches}")

if __name__ == "__main__":
    main()

