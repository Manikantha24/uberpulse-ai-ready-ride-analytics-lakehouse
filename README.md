# UberPulse --- End-to-End Ride Analytics Lakehouse on Azure

UberPulse is a data-engineering portfolio project that builds a reliable
analytics pipeline for NYC Yellow Taxi trip data. The current
implementation runs locally and uses Parquet files for ingestion,
validation, quarantine, and analytics-ready transformation. The roadmap
is to evolve it into an Azure-based lakehouse with orchestration,
monitoring, and cloud storage.

> **Current status:** The local pipeline and automated tests are
> implemented. Azure deployment is planned and has not yet been
> completed.

## Project Goals

-   Build a maintainable end-to-end data pipeline using real-world trip
    data.
-   Make data-quality rules explicit, testable, and auditable.
-   Quarantine records that fail hard validation without silently
    dropping them.
-   Preserve unusual-but-potentially-meaningful records for
    investigation.
-   Produce analytics-ready data with useful time and trip features.
-   Demonstrate engineering discipline through automated tests, Git
    hygiene, and documented results.

## Architecture

### Current local pipeline

``` mermaid
flowchart TD
    A[NYC TLC Yellow Taxi Parquet] --> B[Inspect raw data]
    B --> C[Validate records]
    C --> D{Hard validation}
    D -->|Pass| E[Validated Parquet]
    D -->|Fail| F[Quarantine Parquet + reasons]
    E --> G[Transform and derive features]
    G --> H[Analytics-ready Parquet]
```

### Planned Azure architecture

``` mermaid
flowchart TD
    A[NYC TLC source data] --> B[Azure Data Lake Storage]
    B --> C[Azure Databricks]
    C --> D[Data quality and quarantine]
    D --> E[Delta Lake tables]
    E --> F[Analytics and reporting]
    C --> G[Monitoring, retries, failure handling]
```

The Azure architecture is a target design, not a deployed environment.
Cloud services, orchestration, and monitoring will be implemented and
verified in later project stages.

## Verified Results

The current dataset is the NYC TLC Yellow Taxi trip-data Parquet file
for January 2025.

  Metric                                Verified result
  ----------------------- -----------------------------
  Input records                               3,475,226
  Input columns                                      20
  Validated records                           3,473,175
  Quarantined records                             2,051
  Record reconciliation     Valid + quarantined = input
  Automated tests                             36 passed

The validation run reconciles all input records: **3,473,175 valid +
2,051 quarantined = 3,475,226 input records**.

These results describe the local implementation and the dataset used
during verification. They do not indicate an Azure deployment.

## Data Quality

### Hard validation rules

Records are quarantined when they have one or more of the following
conditions:

-   Missing pickup timestamp
-   Missing drop-off timestamp
-   Non-positive trip duration
-   Negative trip distance
-   Missing fare amount
-   Missing total amount

The quarantine output retains failure reasons to support troubleshooting
and further investigation.

### Warning checks

Warnings are tracked separately from hard validation failures. They do
not automatically cause a record to be quarantined.

  Warning                           Observed records
  ------------------------------- ------------------
  Zero trip distance                          90,893
  Negative fare amount                       144,118
  Negative total amount                       63,037
  Fare amount above \$500                         55
  Trip distance above 100 miles                  162
  Trip duration above 24 hours                    20

Negative fare and total amounts can represent reversals or adjustments,
so they are treated as warnings rather than automatically discarded.
Unusual values should be investigated using business context before
changing the rules.

Additional source profiling found: - Exact duplicate rows: 0 - Missing
values in each of `passenger_count`, `RatecodeID`, `store_and_fwd_flag`,
`congestion_surcharge`, and `Airport_fee`: 540,149

Missing optional fields and unusual values are documented as
data-quality findings; not every anomaly is a hard failure.

See [`docs/data_quality_findings.md`](docs/data_quality_findings.md) and
[`docs/data_quality_rules.md`](docs/data_quality_rules.md) for the
detailed findings and rules.

## Technology Stack

  Technology                             Purpose
  -------------------------------------- -------------------------------------------------
  Python 3.11                            Pipeline implementation
  pandas                                 Data inspection, validation, and transformation
  PyArrow / Parquet                      Columnar data storage and file I/O
  pytest                                 Automated tests
  Git                                    Version control
  Azure Databricks                       Planned cloud processing platform
  Azure Data Lake Storage / Delta Lake   Planned lakehouse storage

## Repository Structure

``` text
UberPulse/
├── data/
│   ├── raw/                       # Downloaded source data (Git-ignored)
│   └── processed/                 # Generated Parquet outputs (Git-ignored)
├── docs/
│   ├── data_quality_findings.md
│   └── data_quality_rules.md
├── logs/                          # Local logs (Git-ignored)
├── src/
│   ├── ingestion/
│   │   └── inspect_raw_data.py
│   ├── transformation/
│   │   └── transform_trips.py
│   ├── validation/
│   │   └── validate_trips.py
│   └── pipeline/
│       └── run_pipeline.py
├── tests/
│   ├── test_raw_data.py
│   ├── test_validation.py
│   ├── test_transformation.py
│   └── test_pipeline.py
├── .gitignore
├── requirements.txt
└── README.md
```

## Dataset

The project uses the NYC Taxi & Limousine Commission (TLC) Yellow Taxi
trip records.

-   Dataset period: January 2025
-   Format: Parquet
-   Source: [NYC TLC Trip Record
    Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
-   Expected local source path:
    `data/raw/yellow_tripdata_2025-01.parquet`

Download the January 2025 Yellow Taxi Parquet file from the official TLC
page and place it at the expected path before running the full pipeline.

The raw dataset is excluded from Git because it is large. Generated
processed data and logs are also excluded.

## Getting Started

### Prerequisites

-   Python 3.11
-   Git
-   The January 2025 Yellow Taxi Parquet file from the official TLC
    source

### 1. Clone the repository

``` powershell
git clone <your-repository-url>
cd UberPulse
```

Replace `<your-repository-url>` with the URL of your Git repository.

### 2. Create and activate a virtual environment

``` powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

``` powershell
python -m pip install -r requirements.txt
```

### 4. Place the source data

Ensure the file exists at:

``` text
data/raw/yellow_tripdata_2025-01.parquet
```

### 5. Run the pipeline

From the project root:

``` powershell
python .\src\pipeline\run_pipeline.py
```

The pipeline inspects the input, validates records into valid and
quarantine outputs, and transforms validated records into an
analytics-ready output.

The scripts support command-line paths for using alternate input and
output locations. Run a script with `--help` to see its available
arguments.

### 6. Run the automated tests

``` powershell
python -m pytest -v
```

**Latest verified result: 36 tests passed.** Tests cover raw-data schema
checks using isolated fixtures, validation rules and edge cases,
transformation behavior, and pipeline execution behavior. The full test
suite is designed to run without requiring the full production-sized
dataset for every test.

## Generated Outputs

The pipeline writes these local Parquet files under `data/processed/`:

  ----------------------------------------------------------------------------------
  File                                           Description
  ---------------------------------------------- -----------------------------------
  `yellow_tripdata_2025-01_valid.parquet`        Records passing hard validation

  `yellow_tripdata_2025-01_quarantine.parquet`   Records failing hard validation,
                                                 including failure reasons

  `yellow_tripdata_2025-01_analytics.parquet`    Analytics-ready records enriched
                                                 with derived features
  ----------------------------------------------------------------------------------

Derived features include pickup date, pickup hour, day of week, weekend
indicator, average speed where calculable, and tip percentage where
meaningful. The transformation preserves source columns and does not
silently drop records.

## Demo and Evidence

### Automated testing

The latest verified full test run completed with **36 passed**.
Reproduce it from the repository root with:

``` powershell
python -m pytest -v
```

### Pipeline evidence

The local run produced 3,475,226 input records, with 3,473,175 valid
records and 2,051 quarantined records. Counts reconcile exactly.

Screenshots, representative output samples, and business-focused
analytics will be added as they are captured and verified.

## Roadmap

-   [x] Implement local inspection, validation, and transformation
-   [x] Separate invalid records into a quarantine dataset
-   [x] Add automated tests and record reconciliation
-   [x] Pin dependencies and configure Git hygiene
-   [x] Add edge-case tests for validation behavior
-   [ ] Add richer pipeline metrics and sample outputs
-   [ ] Capture real execution and testing demonstrations
-   [ ] Build and verify the Azure-based lakehouse pipeline
-   [ ] Add cloud orchestration, monitoring, retries, and failure
    handling
-   [ ] Add analytics queries and business-focused insights
-   [ ] Explore demand forecasting after the core data-engineering
    pipeline is complete

## Engineering Principles

-   Make data-quality rules explicit and testable.
-   Preserve invalid records for investigation rather than silently
    dropping them.
-   Separate hard validation failures from warnings.
-   Reconcile input, valid, and quarantined record counts.
-   Keep secrets and large generated datasets out of source control.
-   Test behavior with small, isolated fixtures wherever possible.
-   Clearly distinguish implemented functionality from planned cloud
    architecture.

## License and Data Attribution

The source dataset is provided by the New York City Taxi & Limousine
Commission. Consult the [official TLC trip record data
page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) for
dataset details and applicable terms.

A project-specific software license has not yet been added.
