import csv
import argparse
import logging
from pathlib import Path
import heapq

import time
import tracemalloc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def parse_row(row, idx_campaign, idx_impressions, idx_clicks, idx_spend, idx_conversions):
    try:
        campaign_id = row[idx_campaign].strip()
        if not campaign_id:
            return None

        impressions = int(row[idx_impressions]) if row[idx_impressions] else 0
        clicks = int(row[idx_clicks]) if row[idx_clicks] else 0
        spend = float(row[idx_spend]) if row[idx_spend] else 0.0
        conversions = int(row[idx_conversions]) if row[idx_conversions] else 0

        return campaign_id, impressions, clicks, spend, conversions

    except (ValueError, IndexError):
        return None


def update_aggregate(aggregates, campaign_id, impressions, clicks, spend, conversions):
    agg = aggregates.setdefault(campaign_id, [0, 0, 0.0, 0])
    agg[0] += impressions
    agg[1] += clicks
    agg[2] += spend
    agg[3] += conversions


def compute_metrics(impr, clk, spend, conv):
    ctr = clk / impr if impr > 0 else 0
    cpa = spend / conv if conv > 0 else None
    return ctr, cpa


# ---------------------------
# Core logic
# ---------------------------
def process_rows(reader):
    aggregates = {}

    processed_rows = 0
    skipped_rows = 0

    header = next(reader)

    idx_campaign = header.index("campaign_id")
    idx_impressions = header.index("impressions")
    idx_clicks = header.index("clicks")
    idx_spend = header.index("spend")
    idx_conversions = header.index("conversions")

    agg_setdefault = aggregates.setdefault

    for row in reader:
        processed_rows += 1

        parsed = parse_row(
            row,
            idx_campaign,
            idx_impressions,
            idx_clicks,
            idx_spend,
            idx_conversions
        )

        if parsed is None:
            skipped_rows += 1
            continue

        campaign_id, impressions, clicks, spend, conversions = parsed

        # inline update (avoid extra call overhead in hot loop if needed)
        agg = agg_setdefault(campaign_id, [0, 0, 0.0, 0])
        agg[0] += impressions
        agg[1] += clicks
        agg[2] += spend
        agg[3] += conversions

        if processed_rows % 100000 == 0:
            logging.info("Processed %d rows", processed_rows)

    logging.info("Finished processing")
    logging.info("Processed rows: %d", processed_rows)
    logging.info("Skipped rows: %d", skipped_rows)

    return aggregates


def process_csv(input_file: str):
    with open(input_file, "r", newline="", buffering=1024 * 1024) as f:
        reader = csv.reader(f)
        return process_rows(reader)


# ---------------------------
# Ranking logic
# ---------------------------
def compute_top_n(aggregates: dict, top_n: int = 10):
    """
    Compute CTR and CPA and maintain top-N rankings using heaps.
    """
    top_ctr = []
    top_cpa = []

    for campaign_id, (impr, clk, spend, conv) in aggregates.items():

        ctr, cpa = compute_metrics(impr, clk, spend, conv)
        data = (campaign_id, impr, clk, spend, conv, ctr, cpa)

        # --- TOP CTR ---
        entry_ctr = (ctr, data)
        if len(top_ctr) < top_n:
            heapq.heappush(top_ctr, entry_ctr)
        else:
            heapq.heappushpop(top_ctr, entry_ctr)

        # --- TOP CPA ---
        if cpa is not None:
            entry_cpa = (-cpa, data)
            if len(top_cpa) < top_n:
                heapq.heappush(top_cpa, entry_cpa)
            else:
                heapq.heappushpop(top_cpa, entry_cpa)

    return top_ctr, top_cpa


def fmt_money(value: float) -> str:
    return f"{value:.2f}"


def fmt_ctr(value: float) -> str:
    return f"{value:.4f}"


def fmt_cpa(value) -> str:
    return f"{value:.2f}" if value != "" else ""


# ---------------------------
# Output (I/O only)
# ---------------------------
def write_ctr(filepath: Path, rows):
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow([
            "campaign_id",
            "total_impressions",
            "total_clicks",
            "total_spend",
            "total_conversions",
            "ctr",
            "cpa"
        ])

        for _, data in sorted(rows, reverse=True):
            cid, impr, clk, spend, conv, ctr, cpa = data

            writer.writerow([
                cid,
                impr,
                clk,
                fmt_money(spend),
                conv,
                fmt_ctr(ctr),
                fmt_cpa(cpa if cpa is not None else "")
            ])


def write_cpa(filepath: Path, rows):
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow([
            "campaign_id",
            "total_impressions",
            "total_clicks",
            "total_spend",
            "total_conversions",
            "ctr",
            "cpa"
        ])

        for _, data in sorted(rows, reverse=True):
            cid, impr, clk, spend, conv, ctr, cpa = data

            writer.writerow([
                cid,
                impr,
                clk,
                fmt_money(spend),
                conv,
                fmt_ctr(ctr),
                fmt_cpa(cpa)
            ])


def main():
    parser = argparse.ArgumentParser(description="Large CSV Campaign Aggregator")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top campaigns to output"
    )
    parser.add_argument(
        "--measure-memory",
        action="store_true",
        help="Enable memory measurement (slower)"
    )

    args = parser.parse_args()

    start_time = time.perf_counter()
    # To measurement memory usage
    if args.measure_memory:
        tracemalloc.start()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info("Starting processing")

    aggregates = process_csv(args.input)

    top_ctr, top_cpa = compute_top_n(aggregates)

    write_ctr(output_dir / "top10_ctr.csv", top_ctr)
    write_cpa(output_dir / "top10_cpa.csv", top_cpa)

    logging.info("Reports written to %s", output_dir)

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    logging.info("Processing time: %.2f seconds", elapsed)
    # To measurement memory usage
    if args.measure_memory:
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        logging.info("Peak memory usage: %.2f MB", peak / (1024 * 1024))


if __name__ == "__main__":
    main()
