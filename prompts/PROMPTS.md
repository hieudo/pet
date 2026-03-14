## Prompt 1 – Initial Implementation

Write Python 3.12 code to process a very large CSV file (~3GB) using a streaming approach without loading the entire file into memory.

Requirements:

- Read the CSV file using streaming with the built-in open() function.
- Use csv.DictReader to parse the CSV safely (do not manually split lines).
- Do not load the entire dataset into RAM.
  Do not use pandas because the dataset is large and pandas typically loads the entire file into memory.
- Do not use asyncio because the file is read from local disk and sequential streaming is sufficient and more efficient.
- Log progress every 10,000 processed rows.
- The code should be optimized for memory usage and simplicity.
- Provide a complete runnable example in a file named `aggregator.py`.
- CLI usage example:
  python aggregator.py --input ad_data.csv --output results/

Schema file ad_data.csv:

campaign_id (string)
date (string, yyyy-mm-dd)
impressions (int)
clicks (int)
spend (float)
conversions (int)

Data quality assumptions:

- Some CSV rows may contain missing or empty values.
- Missing numeric fields (impressions, clicks, spend, conversions) should default to 0.
- Rows with missing campaign_id should be skipped.
- Invalid numeric values should not crash the program; skip the row and continue processing.

Aggregation logic:

For each campaign_id compute:

- total_impressions
- total_clicks
- total_spend
- total_conversions
- CTR = total_clicks / total_impressions
- CPA = total_spend / total_conversions

Rules:

- If total_impressions = 0 → CTR = 0
- Exclude campaigns with total_conversions = 0 from CPA ranking

Output CSV format:

campaign_id,total_impressions,total_clicks,total_spend,total_conversions,ctr,cpa

Generate:

- results/top10_ctr.csv → top 10 campaigns with highest CTR (descending)
- results/top10_cpa.csv → top 10 campaigns with lowest CPA (ascending)


## Prompt 2 – Performance Review
Review the implementation for performance and memory usage when processing a ~3GB CSV file.

Suggest improvements for:
- CPU efficiency
- reducing object allocations
- avoiding full dataset sorting
- minimizing memory footprint
- improving streaming throughput


## Prompt 3 – Optimized Implementation

Rewrite the implementation applying the performance improvements suggested earlier.

Focus on:

- maintaining top-10 results incrementally
- minimizing object allocation
- keeping memory usage low for ~3GB datasets
- keeping the code readable and production-ready

## Prompt 4 - Unit test

Refactor the code with minimal changes to make it easier to write unit tests.

Goals:

- Separate file I/O from business logic.
- Extract pure functions that operate on in-memory data structures.
- Ensure the core logic can be tested without accessing files.

Performance constraints:

- Preserve the streaming processing model.
- Do not introduce intermediate lists or load the entire dataset into memory.
- Avoid additional object allocations inside the main processing loop.
- Keep the row processing logic efficient for large datasets (~3GB CSV).

Constraints:

- Do not change the external behavior or CLI interface.
- Prefer small refactoring steps instead of rewriting the entire architecture.

Testing:

- Focus tests on the core computation logic.
- File I/O can be tested with a small integration test if needed.
- Use pytest for test examples.

Output:

- Refactored code
- Example pytest test file
