from pathlib import Path
from typing import Dict, List, Any
import fnmatch
from datetime import (
        datetime,
        timezone,
)


BASE_DIR = Path(__file__).resolve().parents[2]
INCOMING_DIR = BASE_DIR / "data" / "incoming"


def discover_batches(paths_cfg: dict, logger) -> Dict[str, List[str]]:
    """
    Discover batches to ingest in the incoming directory.
    - paths_cfg = dict containing all the paths available in the pipeline.

    Batch is a folder.
    """
    result: Dict[str, List[str]] = {}
    for provider, cfg in paths_cfg.get("providers", {}).items():
        incoming_dir = Path(
                cfg.get("incoming_dir", INCOMING_DIR / provider)
                ).resolve()

        # It's the mark that defines the files landed completely.
        # For this challenge it's a file named _READY.
        readiness = cfg.get("readiness", {}).get("marker_file", "_READY")

        # In case of big files, wait until they're ready. Optional.
        quarantine_sec = cfg.get("readiness", {}).get("quarantine_seconds", 0)

        if not incoming_dir.exists():
            logger.warning(f"Provider path missing: {incoming_dir}")
            continue

        ready_batches: List[str] = []
        for batch_dir in sorted(
                p for p in incoming_dir.iterdir() if p.is_dir()
                ):
            marker = batch_dir / readiness

            # Check if the the batch is ready to be ingested.
            if marker.exists():
                ready_batches.append(batch_dir.name)
            elif quarantine_sec > 0:
                age = (datetime.now(
                        timezone.utc
                    ) - datetime.fromtimestamp(
                        batch_dir.stat().st_mtime, tz=timezone.utc
                        )).total_seconds()
                if age >= quarantine_sec:
                    ready_batches.append(batch_dir.name)

        if ready_batches:
            result[provider] = ready_batches

    if not result:
        logger.warning("No ready batches found.")
    else:
        total = sum(len(v) for v in result.values())
        logger.info(f"Discovered {total} ready batch(es).")
    return result


def build_plan(
        batches: Dict[str, List[str]],
        mappings_cfg: dict,
        logger) -> List[Dict[str, Any]]:
    """
    Build a datastructure defining the plan to ingest the batch.
    - batches = list of providers and folders to be ingested.
    - mappings_cfg = result from yaml that defines the providers.
    - loger = just the logging object.
    """
    plan: List[Dict[str, Any]] = []
    for provider, batch_ids in batches.items():
        provider_cfg = mappings_cfg.get("providers", {}).get(provider)

        # Just protection to avoid missing configuration about the provider
        if not provider_cfg:
            logger.warning(f"No mapping found for provider: {provider}")
            continue

        feeds = provider_cfg.get("feeds", {})

        # Protection to avoid missing feeds for the current provider.
        if not feeds:
            logger.warning(f"No feeds found for provider: {provider}")
            continue

        for batch_id in batch_ids:
            batch_path = INCOMING_DIR / provider / batch_id
            for file_path in sorted(
                    p for p in batch_path.glob("*")
                    if p.is_file() and not p.name.startswith("_")
                    ):

                matched_feed = matched_entity = None
                for feed_name, feed_cfg in feeds.items():
                    # filename_selector is a glob string (like *.csv)
                    pattern = feed_cfg.get("filename_selector")
                    if pattern and fnmatch.fnmatch(file_path.name, pattern):
                        matched_feed = feed_name
                        matched_entity = feed_cfg.get("target_entity")
                        break
                if matched_feed:
                    # Add meta information
                    plan.append({
                        "provider": provider,
                        "batch_id": batch_id,
                        "file": str(file_path),
                        "feed": matched_feed,
                        "target_entity": matched_entity,
                    })
                    logger.info(f"Plan: {file_path.name} → {matched_entity}")
                else:
                    logger.warning(f"Unmatched file: {file_path.name}")

    if not plan:
        logger.warning("No files matched any feed pattern.")
    else:
        logger.info(f"Planned {len(plan)} file(s).")
    return plan
