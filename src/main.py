from collections import defaultdict
from typing import Dict, List, Tuple
from utils.log import init_logger
from core.loader import load_yaml
from core.planner import (
    discover_batches,
    build_plan,
)
from core.validator import qualify_plan
from core.ingest import ingest_csv, ingest_json, consolidate_bronze_feed
from core.silver import build_silver_movie_metrics
from core.gold import build_gold
from core.audit import write_audit


logger = init_logger()


def required_feeds_map(mappings_cfg: dict) -> Dict[str, List[str]]:
    """
    Feeds marked as required (default True) per provider.
    """
    out: Dict[str, List[str]] = {}
    for provider, pcfg in mappings_cfg.get("providers", {}).items():
        req = [
            fname
            for fname, fcfg in pcfg.get("feeds", {}).items()
            if fcfg.get("required", True)
        ]
        out[provider] = sorted(req)
    return out


def batch_completeness(
    qualified_plan: List[dict], mappings_cfg: dict
) -> Dict[Tuple[str, str], dict]:
    """
    Return {(provider,batch_id): {'present', 'required', 'complete'}}.
    """
    req_map = required_feeds_map(mappings_cfg)
    present = defaultdict(set)
    for it in qualified_plan:
        present[(it["provider"], it["batch_id"])].add(it["feed"])

    result: Dict[Tuple[str, str], dict] = {}
    for (prov, bid), pres in present.items():
        req = set(req_map.get(prov, []))
        complete = req.issubset(pres)
        result[(prov, bid)] = {"present": pres, "required": req, "complete": complete}
    return result


def main() -> None:
    # Load configurations
    contracts = load_yaml("contracts.yaml")
    mappings = load_yaml("mappings.yaml")
    paths = load_yaml("paths.yaml")
    logger.info("Configs loaded.")

    # Discover batches ready to be ingested
    batches = discover_batches(paths, logger)

    # Build plan based on the ready batches
    plan = build_plan(batches, mappings, logger)
    if not plan:
        logger.info("Nothing to do (empty plan).")
        return

    # Validate sources referenced in the plan
    qualified = qualify_plan(plan, mappings, logger)
    if not qualified:
        logger.info("No qualified files after precheck.")
    else:
        logger.info(f"Qualified plan size: {len(qualified)}")

        # Ingest the data
        for item in qualified:
            provider = item["provider"]
            feed_cfg = mappings["providers"][provider]["feeds"][item["feed"]]
            fmt = (feed_cfg.get("input_format") or "csv").lower()

            if fmt == "csv":
                ingest_csv(item, mappings, contracts, logger)
            elif fmt == "json":
                ingest_json(item, mappings, contracts, logger)
            else:
                logger.error(
                    "Unsupported input_format " f"'{fmt}' for {provider}.{item['feed']}"
                )

        # Audit
        summary = batch_completeness(qualified, mappings)
        for (provider, batch_id), info in sorted(summary.items()):
            write_audit(
                provider=provider,
                batch_id=batch_id,
                level="batch",
                completeness=sorted(list(info["present"])),
                required=sorted(list(info["required"])),
                complete=info["complete"],
                status="ok",
            )
            if not info["complete"]:
                logger.warning(
                    "Batch incomplete -> skip in silver: "
                    f"{provider}/{batch_id}."
                    f"present={sorted(info['present'])}, "
                    f"required={sorted(info['required'])}"
                )

        # Consolidate data
        by_batch = defaultdict(set)
        for it in qualified:
            by_batch[(it["provider"], it["batch_id"])].add(it["feed"])

        for (provider, batch_id), feeds in sorted(by_batch.items()):
            for feed in sorted(feeds):
                consolidate_bronze_feed(provider, feed, batch_id, logger)

    curation = load_yaml("curation.yaml")
    silver_out = build_silver_movie_metrics(curation, contracts, logger)
    if silver_out:
        logger.info(f"[silver] built at: {silver_out}")

        gold_out = build_gold(logger)
        if gold_out:
            logger.info(f"Gold built at: {gold_out}")

    logger.info("Done.")


if __name__ == "__main__":
    main()
