# UberPulse — Data Quality Findings

## 1. Dataset

* **Source:** NYC TLC Yellow Taxi Trip Records
* **File:** `yellow_tripdata_2025-01.parquet`
* **Format:** Parquet
* **Rows:** 3,475,226
* **Columns:** 20

## 2. Initial Profiling Results

| Check                            | Finding |
| -------------------------------- | ------: |
| Exact duplicate rows             |       0 |
| Missing passenger_count          | 540,149 |
| Missing RatecodeID               | 540,149 |
| Missing store_and_fwd_flag       | 540,149 |
| Missing congestion_surcharge     | 540,149 |
| Missing Airport_fee              | 540,149 |
| Zero-distance trips              |  90,893 |
| Negative-distance trips          |       0 |
| Negative-fare trips              | 144,118 |
| Fares above $500                 |      55 |
| Trips above 100 miles            |     162 |
| Zero or negative-duration trips  |   2,051 |
| Trips lasting more than 24 hours |      20 |

## 3. Initial Observations

* Several columns contain missing values.
* Some records contain unusually large distances or fare amounts.
* Some trips have zero distance or non-positive duration.
* Negative fare amounts occur in the source data and require further interpretation.

## 4. Handling Principles

1. Preserve the original raw Parquet file unchanged.
2. Do not automatically delete records solely because they contain missing values or negative fares.
3. Apply documented validation rules when building the Silver layer.
4. Separate records that fail defined quality rules for investigation rather than silently discarding them.
5. Preserve source values and document any transformations.
6. Investigate extreme values before deciding whether to reject them.

## 5. Status

These findings are exploratory profiling results, not final rejection rules. Final Silver-layer validation rules must be defined and tested before implementation.
