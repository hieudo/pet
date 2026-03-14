import csv
import argparse
import logging
from pathlib import Path
from collections import defaultdict


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def safe_int(value: str) -> int:
    """
    Convert value to int safely.
    Empty values -> 0
    Invalid values -> raise ValueError
    """
    if value is None or value == "":
        return 0
    return int(value.strip())

def safe_float(value: str) -> float:
    if value is None or value == "":
        return 0.0
    return float(value.strip())

def process_csv(input_file: str):
    """
    Stream CSV file and aggregate metrics by campaign_id
    """

    aggregates = defaultdict(lambda: {
        "impressions": 0,
        "clicks": 0,
        "spend": 0.0,
        "conversions": 0
    })

    processed_rows = 0
    skipped_rows = 0

    with open(input_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            processed_rows += 1

            try:
                campaign_id = (row.get("campaign_id") or "").strip()
                if not campaign_id:
                    skipped_rows += 1
                    continue

                impressions = safe_int(row.get("impressions"))
                clicks = safe_int(row.get("clicks"))
                spend = safe_float(row.get("spend"))
                conversions = safe_int(row.get("conversions"))

                agg = aggregates[campaign_id]
                agg["impressions"] += impressions
                agg["clicks"] += clicks
                agg["spend"] += spend
                agg["conversions"] += conversions

            except ValueError:
                skipped_rows += 1
                continue

            if processed_rows % 10000 == 0:
                logging.info("Processed %d rows", processed_rows)

    logging.info("Finished processing")
    logging.info("Processed rows: %d", processed_rows)
    logging.info("Skipped rows: %d", skipped_rows)

    return aggregates


def compute_metrics(aggregates: dict):
    """
    Compute CTR and CPA for each campaign
    """

    results = []

    for campaign_id, data in aggregates.items():
        impressions = data["impressions"]
        clicks = data["clicks"]
        spend = data["spend"]
        conversions = data["conversions"]

        ctr = clicks / impressions if impressions > 0 else 0
        cpa = spend / conversions if conversions > 0 else None

        results.append({
            "campaign_id": campaign_id,
            "total_impressions": impressions,
            "total_clicks": clicks,
            "total_spend": spend,
            "total_conversions": conversions,
            "ctr": ctr,
            "cpa": cpa
        })

    return results


def write_csv(filepath: Path, rows: list):
    """
    Write output CSV
    """

    fieldnames = [
        "campaign_id",
        "total_impressions",
        "total_clicks",
        "total_spend",
        "total_conversions",
        "ctr",
        "cpa"
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow(row)


def generate_rankings(results: list, output_dir: Path):
    """
    Generate top10 CTR and top10 CPA reports
    """

    # Top CTR (descending)
    top_ctr = sorted(results, key=lambda x: x["ctr"], reverse=True)[:10]

    # Top CPA (ascending) — exclude campaigns with 0 conversions
    cpa_candidates = [r for r in results if r["cpa"] is not None]
    top_cpa = sorted(cpa_candidates, key=lambda x: x["cpa"])[:10]

    write_csv(output_dir / "top10_ctr.csv", top_ctr)
    write_csv(output_dir / "top10_cpa.csv", top_cpa)


def main():
    parser = argparse.ArgumentParser(description="Aggregate campaign metrics from large CSV")
    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--output", required=True, help="Output directory")

    args = parser.parse_args()

    input_file = args.input
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info("Starting CSV processing")

    aggregates = process_csv(input_file)

    results = compute_metrics(aggregates)

    generate_rankings(results, output_dir)

    logging.info("Reports generated in %s", output_dir)


if __name__ == "__main__":
    main()
