# CSV Campaign Aggregator

A high-performance tool for aggregating ad campaign data from large-scale CSV files, optimized for minimal memory footprint and streaming processing.

---

## 1. Requirements
* **Python 3.12** or newer.
* **No external dependencies** (Uses only the Python Standard Library).
* **pytest** (Only required for running the test suite).

## 2. Project Structure
```text
project/
├─ aggregator.py      # Main processing script
├─ ad_data.csv        # Input CSV file (example)
├─ results/           # Output directory (auto-created)
└─ test/              # Unit tests directory
```

## 3. Usage
Run the script via the command line:

```bash
python aggregator.py --input ad_data.csv --output results/
```

### Command Line Arguments:
| Argument | Description |
| :--- | :--- |
| `--input` | Path to the input CSV file |
| `--output` | Directory where result files will be saved |
| `--measure-memory` | (Optional) Measure peak RAM usage during execution |

## 4. Output Data
The program generates two primary reports in the output directory:
* `top10_ctr.csv`: Top 10 campaigns with the highest CTR.
* `top10_cpa.csv`: Top 10 campaigns with the lowest CPA.

**Calculations & Rules:**
* **CTR** = total_clicks / total_impressions (CTR = 0 if impressions = 0).
* **CPA** = total_spend / total_conversions (Campaigns with 0 conversions are excluded from ranking).

## 5. Implementation Details
Designed to process multi-gigabyte datasets with minimal system resources:
* **`csv`**: Sequential row-by-row streaming to avoid loading the entire dataset into memory.
* **`heapq`**: Maintains top-10 rankings using a Min-Heap. Reduces complexity from $O(n \log n)$ to $O(n \log 10)$.
* **`argparse` & `pathlib`**: Provides a clean CLI and safe cross-platform path handling.
* **`logging`**: Real-time progress reporting for large file processing.

## 6. Testing
To run the automated test suite, ensure `pytest` is installed and run:

```bash
pytest test/
```

The test suite validates:
* **Data Parsing:** Correct handling of various CSV formats and headers.
* **Logic Accuracy:** Proper calculation of CTR and CPA, including edge cases (division by zero).
* **Ranking:** Verification that the Heap correctly identifies the Top 10 items.

## 7. Performance Benchmarks
Tested with a **1GB** dataset (~26.8 million rows):

**Sample Execution Logs:**
```text
[INFO] Processed 100,000 rows
[INFO] Processed 200,000 rows
...
[INFO] Finished processing
[INFO] Processed rows: 26,843,544
[INFO] Skipped rows: 0
[INFO] Reports written to results
[INFO] Processing time: 72.15 seconds
[INFO] Peak memory usage: 1.06 MB
```

* **Processing Time:** ~72.15 seconds.
* **Peak Memory Usage:** ~1.06 MB.
