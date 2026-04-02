# Apache Impala Performance Test and Comparison

## Overview
This repository provides a lightweight framework to run Apache Impala query performance tests and compare saved results.

It supports:
- executing query workloads against an Impala cluster,
- measuring query runtime across multiple iterations,
- saving results as CSV reports under `test_result/`, and
- loading report CSVs to derive comparison metrics.

## Repository Structure
```
main.py
requirements.txt
src/
    __init__.py
    test_query.py
    test_performance.py
    compare_performance.py
test_result/
    apache_impalad_version_4.3.0_performance.csv
    apache_impalad_version_4.5.0_performance.csv
```

## How it Works
- `main.py` is the entrypoint and supports two modes: `test` and `compare`.
- `--mode test` loads query definitions from `src/test_query.py` and runs each SQL query repeatedly.
- `src/test_performance.py` connects to Impala, validates SQL via `EXPLAIN`, runs a warm-up execution, then times the configured iterations.
- Results are written to CSV files under `test_result/` in the format `apache_<IMPALA_VERSION>_performance.csv`.
- `--mode compare` loads report files from `test_result/` and computes summary metrics for each loaded file.

## Prerequisites
- Python 3.9 or newer
- Access to an Impala cluster
- Network connectivity from the test machine to the Impala host

## Installation
```bash
git clone <repo-url>
cd <repo_name>
pip install -r requirements.txt
```

## Configuration
### Add or modify test queries
Open `src/test_query.py` and edit the `sql_improvement()` function or add additional query set functions.
The repository uses the `QueryList` dataclass:
```python
ql.add("SELECT ...", mt_dop=4)
```
If `mt_dop` is omitted, it defaults to `0`.

### Available CLI arguments
- `--mode`: `test` or `compare`
- `--iteration`: number of timing iterations per query (default `10`)
- `--host`: Impala host address (default `10.168.49.12`)
- `--port`: Impala port (default `21050`)

## Usage
### Run performance tests
```bash
python main.py --mode test
```

### Compare saved performance reports
```bash
python main.py --mode compare
```

### Example with custom settings
```bash
python main.py --mode test --host 192.168.1.100 --port 21050 --iteration 20
```

## Output
- Test runs produce CSV files in `test_result/`.
- File names follow the pattern `apache_<IMPALA_VERSION>_performance.csv`.
- Each CSV has one column per query and one row per measurement iteration.

## Notes
- `src/test_query.py` currently returns a single query set from `get_queries()`.
- `src/compare_performance.py` reads CSV files from `test_result/` and computes per-column statistics.
- Make sure the `test_result/` directory exists and is writable before running tests.

## Dependencies
- `impyla`
- `pandas`
- `tqdm`
