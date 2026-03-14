# Requirement: https://github.com/flinters-vietnam/recruitment/tree/main/fv-sec-001-software-engineer-challenge
# CSV Campaign Aggregator

## Setup Instructions

### 1. Requirements

* **Python 3.12** or newer
* No external dependencies are required (only Python standard library)

### 2. Project Structure

Example project layout:

```text
project/
├─ prompts
├─ results/
├─ test/test_aggregator.py
├─ ad_data.csv
└─ aggregator.py
```

The `results/` directory will be created automatically if it does not exist.

---

## How to Run the Program

Run the script using the command line:

```bash
python aggregator.py --input ad_data.csv --output results/
```

### Parameters

| Argument   | Description                                  |
| ---------- | -------------------------------------------- |
| `--input`  | Path to the input CSV file                   |
| `--output` | Directory where result files will be written |

Example:

```bash
python aggregator.py --input ad_data.csv --output results/
```

### Output

The program generates two files:

```
results/
├─ top10_ctr.csv
└─ top10_cpa.csv
```

Each output file follows the format:

```
campaign_id,total_impressions,total_clicks,total_spend,total_conversions,ctr,cpa
```

Where:

* **CTR** = total_clicks / total_impressions
* **CPA** = total_spend / total_conversions

Rules applied:

* If `total_impressions = 0` → CTR = 0
* Campaigns with `total_conversions = 0` are excluded from CPA ranking

---

## Libraries Used

The implementation intentionally uses only the **Python Standard Library** to keep the program lightweight and suitable for processing large CSV files.

### csv

Used for efficient streaming CSV parsing.

```
csv.reader
```

Advantages:

* Processes rows sequentially
* Avoids loading the entire dataset into memory

---

### heapq

Used to maintain **top-10 rankings incrementally**.

```
heapq.heappush()
heapq.heappushpop()
```

Benefits:

* Avoids sorting the entire dataset
* Keeps only the top 10 elements in memory
* Reduces complexity from **O(n log n)** to **O(n log 10)**

---

### argparse
Used to implement a clean command-line interface.
```
python aggregator.py --input ad_data.csv --output results/
```

---

### logging

Used for progress reporting during large file processing.

Example output:

```
2026-03-14 10:12:01 [INFO] Processed 100000 rows
2026-03-14 10:12:05 [INFO] Processed 200000 rows
...
2026-03-14 16:45:40,732 [INFO] Finished processing
2026-03-14 16:45:40,732 [INFO] Processed rows: 26843544
2026-03-14 16:45:40,732 [INFO] Skipped rows: 0
2026-03-14 16:45:40,732 [INFO] Reports written to results
2026-03-14 16:45:40,732 [INFO] Processing time: 72.15 seconds
```

---

### pathlib

Used for safe and portable filesystem path handling.

---
## Processing time for the 1GB file
[INFO] Processing time: 72.15 seconds

---
### Peak memory usage
Used to implement a clean command-line interface.
```
python aggregator.py --input ad_data.csv --output results --measure-memory
```
Result: 1.06 MB
