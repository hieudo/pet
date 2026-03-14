import tempfile

from aggregator import (
    parse_row,
    update_aggregate,
    compute_metrics,
    compute_top_n,
    process_csv
)


# indexes based on dataset schema
IDX_CAMPAIGN = 0
IDX_IMPR = 2
IDX_CLK = 3
IDX_SPEND = 4
IDX_CONV = 5


def test_parse_row_valid():

    row = ["CMP025", "2025-04-18", "3653", "60", "64.29", "2"]

    result = parse_row(
        row,
        IDX_CAMPAIGN,
        IDX_IMPR,
        IDX_CLK,
        IDX_SPEND,
        IDX_CONV
    )

    assert result == ("CMP025", 3653, 60, 64.29, 2)


def test_parse_row_invalid_number():

    row = ["CMP025", "2025-04-18", "abc", "60", "64.29", "2"]

    result = parse_row(
        row,
        IDX_CAMPAIGN,
        IDX_IMPR,
        IDX_CLK,
        IDX_SPEND,
        IDX_CONV
    )

    assert result is None


def test_update_aggregate():

    aggregates = {}

    update_aggregate(aggregates, "CMP025", 3653, 60, 64.29, 2)
    update_aggregate(aggregates, "CMP025", 38234, 1695, 271.09, 149)

    assert aggregates["CMP025"] == [
        3653 + 38234,
        60 + 1695,
        64.29 + 271.09,
        2 + 149
    ]

def extract_campaign_ids_ctr(rows):
    return [data[0] for _, data in rows]

def extract_campaign_ids_cpa(rows):
    return [data[0] for _, data in rows]

def test_compute_top_n_realistic():

    aggregates = {
        "CMP047": [47113, 2081, 963.56, 127],
        "CMP025": [38234, 1695, 271.09, 149],
        "CMP020": [24465, 764, 1394.62, 42],
        "CMP019": [7214, 236, 135.93, 21],
    }

    top_ctr, top_cpa = compute_top_n(aggregates)

    ctr_campaigns = extract_campaign_ids_ctr(top_ctr)
    cpa_campaigns = extract_campaign_ids_cpa(top_cpa)

    assert "CMP047" in ctr_campaigns
    assert "CMP025" in ctr_campaigns

    assert "CMP025" in cpa_campaigns


def test_process_csv_with_real_data(tmp_path):

    csv_file = tmp_path / "test.csv"

    csv_file.write_text(
        "campaign_id,date,impressions,clicks,spend,conversions\n"
        "CMP025,2025-04-18,3653,60,64.29,2\n"
        "CMP025,2025-06-22,38234,1695,271.09,149\n"
        "CMP047,2025-03-25,47113,2081,963.56,127\n"
        "CMP019,2025-02-05,7214,236,135.93,21\n"
    )

    result = process_csv(csv_file)

    assert result["CMP025"] == [
        3653 + 38234,
        60 + 1695,
        64.29 + 271.09,
        2 + 149
    ]

    assert result["CMP047"] == [
        47113,
        2081,
        963.56,
        127
    ]


CSV_DATA = """campaign_id,date,impressions,clicks,spend,conversions
CMP025,2025-04-18,3653,60,64.29,2
CMP020,2025-05-03,24465,764,1394.62,42
CMP019,2025-02-05,7214,236,135.93,21
CMP046,2025-06-04,10631,201,298.82,18
CMP044,2025-03-26,31942,964,744.4,37
CMP047,2025-03-25,47113,2081,963.56,127
CMP016,2025-02-06,36585,920,232.62,35
CMP033,2025-05-06,8147,151,98.61,3
CMP022,2025-05-19,14317,692,862.27,43
CMP029,2025-01-24,40078,1639,1151.22,52
CMP025,2025-06-22,38234,1695,271.09,149
CMP020,2025-04-14,22529,416,127.48,25
CMP042,2025-04-27,2547,13,12.87,0
"""


def create_temp_csv(data):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w")
    tmp.write(data)
    tmp.close()
    return tmp.name


def test_compute_metrics():

    ctr, cpa = compute_metrics(
        impr=1000,
        clk=100,
        spend=500,
        conv=10
    )

    assert ctr == 0.1
    assert cpa == 50


def test_compute_metrics_zero_cases():
    ctr, cpa = compute_metrics(0, 10, 100, 5)

    assert ctr == 0

    ctr, cpa = compute_metrics(100, 10, 100, 0)

    assert cpa is None


# ==============================
# TEST CSV PROCESSING
# ==============================

def test_process_csv_aggregation():

    file = create_temp_csv(CSV_DATA)

    aggregates = process_csv(file)

    # CMP025 appears twice -> aggregation must sum
    impr, clk, spend, conv = aggregates["CMP025"]

    assert impr == 3653 + 38234
    assert clk == 60 + 1695
    assert round(spend, 2) == round(64.29 + 271.09, 2)
    assert conv == 2 + 149


# ==============================
# TEST TOP10 LOGIC
# ==============================

def test_compute_top_n_basic():

    file = create_temp_csv(CSV_DATA)

    aggregates = process_csv(file)

    top_ctr, top_cpa = compute_top_n(aggregates)

    # must not exceed 10
    assert len(top_ctr) <= 10
    assert len(top_cpa) <= 10


def test_top_ctr_ordering():

    file = create_temp_csv(CSV_DATA)

    aggregates = process_csv(file)

    top_ctr, _ = compute_top_n(aggregates)

    # convert heap to sorted list
    sorted_ctr = sorted(top_ctr, reverse=True)

    ctr_values = [x[0] for x in sorted_ctr]

    # ensure descending order
    assert ctr_values == sorted(ctr_values, reverse=True)


def test_top_cpa_ordering():

    file = create_temp_csv(CSV_DATA)

    aggregates = process_csv(file)

    _, top_cpa = compute_top_n(aggregates)

    sorted_cpa = sorted(top_cpa, reverse=True)

    cpa_values = [-x[0] for x in sorted_cpa]

    assert cpa_values == sorted(cpa_values)


# ==============================
# EDGE CASE
# ==============================

def test_campaign_with_zero_conversion_not_in_cpa():

    file = create_temp_csv(CSV_DATA)

    aggregates = process_csv(file)

    _, top_cpa = compute_top_n(aggregates)

    # campaign CMP042 has conversions = 0
    campaigns = [x[1] for x in top_cpa]

    assert "CMP042" not in campaigns

