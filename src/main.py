from __future__ import annotations
import argparse
import sys
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from utils.log import init_logger
from core.loader import load_yaml
from core.planner import discover_batches, build_plan
from core.validator import qualify_plan
from core.ingest import ingest_csv, ingest_json, consolidate_bronze_feed
from core.silver import build_silver_movie_metrics
from core.gold import build_gold
from core.audit import write_audit
from core.query import (
    load_gold_df,
    as_dict_by_key,
    find_by_title,
    lookup,
    head,
    MovieKey,
)


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
        result[(prov, bid)] = {
                "present": pres,
                "required": req,
                "complete": complete
        }

    return result


def run_etl() -> int:
    """
    Execute the full pipeline end-to-end and build gold.
    Returns 0 on success, non-zero on failure.
    """
    logger = init_logger()

    try:
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
            return 0

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
                        f"Unsupported input_format '{fmt}' "
                        f"for {provider}.{item['feed']}"
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

        # Silver and Gold
        curation = load_yaml("curation.yaml")
        silver_out = build_silver_movie_metrics(curation, contracts, logger)
        if silver_out:
            logger.info(f"[silver] built at: {silver_out}")

            gold_out = build_gold(logger)
            if gold_out:
                logger.info(f"Gold built at: {gold_out}")

        logger.info("Done.")
        return 0

    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        return 1


# Cli arguments/commands
def _cmd_run(_: argparse.Namespace) -> int:
    return run_etl()


def _cmd_query(args: argparse.Namespace) -> int:
    df = load_gold_df()

    if args.list:
        print(head(df, args.list).to_string(index=False))
        return 0

    if args.key:
        rows = lookup(df, MovieKey(movie_key=args.key))
        if rows.empty:
            print("No match for movie_key.", file=sys.stderr)
            return 2
        print(rows.to_string(index=False))
        return 0

    if args.title:
        if args.year is not None:
            rows = lookup(df, MovieKey(title=args.title, year=args.year))
        else:
            rows = find_by_title(df, args.title)
        if rows.empty:
            print("No matches.", file=sys.stderr)
            return 2
        print(rows.to_string(index=False))
        return 0

    if args.dict:
        d = as_dict_by_key(df)
        print(f"Loaded dict with {len(d):,} movies. Example keys (up to 10):")
        for i, k in enumerate(d.keys()):
            if i == 10:
                break
            print("  ", k)
        return 0

    print(
        "No query specified. Try --list 10 or --title 'Inception' "
        "--year 2010 or --key <id>.",
        file=sys.stderr,
    )
    return 2


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="movie-pipeline",
        description="ETL pipeline and gold data query tool.",
    )
    sub = p.add_subparsers(dest="cmd")

    # run_etl
    pr = sub.add_parser("run_etl", help="Run the full ETL to produce gold.")
    pr.set_defaults(func=_cmd_run)

    # query
    pq = sub.add_parser("query", help="Query the gold output.")
    pq.add_argument("--list", type=int, default=0, help="Show N from gold.")
    pq.add_argument("--key", type=str, help="Lookup by movie_key.")
    pq.add_argument(
        "--title",
        type=str,
        help="Lookup by title (case-insensitive contains if year not provided).",
    )
    pq.add_argument("--year", type=int, help="Add year for exact lookup.")
    pq.add_argument(
        "--dict",
        action="store_true",
        help="Load gold as a dict keyed by movie_key (or (title,year)).",
    )
    pq.set_defaults(func=_cmd_query)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

