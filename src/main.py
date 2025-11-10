from pprint import pprint
import sys
import fnmatch
import yaml
from typing import (
        Dict,
        List,
        Any,
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
                logger.info(f"Batch ready: {provider}/{batch_dir.name}")
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


def build_plan(batches: Dict[str, List[str]], mappings_cfg: dict) -> List[Dict[str, Any]]:
    """
    Build a plan of execution.
    The plan is a relation between source files and entities/feeds.
    """
    plan = []
    for provider, batch_ids in batches.items():
        provider_cfg = mappings_cfg.get("providers", {}).get(provider)
        if not provider_cfg:
            logger.warning(f"No mapping found for provider: {provider}")
            continue

        feeds = provider_cfg.get("feeds", {})
        if not feeds:
            logger.warning(f"No feeds found for provider: {provider}")
            continue

        for batch_id in batch_ids:
            batch_path = Path("data/incoming") / provider / batch_id
            for file_path in sorted(batch_path.glob("*")):
                if not file_path.is_file() or file_path.name.startswith("_"):
                    continue

                matched_feed = None
                matched_entity = None
                for feed_name, feed_cfg in feeds.items():
                    file_name_pattern = feed_cfg.get("filename_selector", {})
                    if not file_name_pattern:
                        logger.warning(
                                f"Feed {feed_name} "
                                "missing selector pattern."
                        )
                        continue

                    if fnmatch.fnmatch(file_path.name, file_name_pattern):
                        matched_feed = feed_name
                        matched_entity = feed_cfg.get("target_entity")
                        break

                if matched_feed:
                    plan.append(
                            {
                                "provider": provider,
                                "batch_id": batch_id,
                                "file": file_path,
                                "feed": matched_feed,
                                "target_entity": matched_entity,
                            }
                    )
                    logger.info(
                        "Plan created for: "
                        f"{file_path.name} -> {matched_entity}"
                    )
                else:
                    logger.warning(f"File didn't match any: {file_path.name}")

    if not plan:
        logger.warning("No files matched any feed pattern.")
    else:
        logger.info(
                "Planned: "
                f"{len(plan)} file(s) total."
        )

    return plan


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

    plan = build_plan(batches, mappings)
    pprint(plan)

if __name__ == "__main__":
    main()

