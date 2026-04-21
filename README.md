# Performance Testing Framework for Apache Impala and ClickHouse

## Overview
This repository provides a comprehensive framework for running query performance tests on both Apache Impala and ClickHouse, comparing results between different versions, and generating detailed PDF reports with visual charts.

Key features:
- Execute query workloads against Impala and ClickHouse clusters with configurable iterations
- Test ClickHouse queries **without query result cache** for accurate performance measurements
- Measure and record query execution times with high precision
- Perform statistical A/B testing comparison between versions
- Generate professional PDF reports with box plots, summary charts, and detailed performance statistics
- Support for dynamic version labeling and flexible query sets

## Repository Structure
```
main.py                              # Main entry point script
README.md                            
CLICKHOUSE_TESTING.md                # ClickHouse testing documentation
requirements.txt                     # Python dependencies
src/
    __init__.py
    compare_performance.py           # Statistical comparison logic
    plot_charts.py                   # PDF report generation with charts
    test_performance.py              # Impala query execution and timing
    test_clickhouse_performance.py    # ClickHouse query execution and timing
    test_query.py                    # Impala query definitions
    clickhouse_query.py              # ClickHouse query definitions
test_result/
```

## How It Works

### Test Mode for Impala (`--mode test`)
- Loads query definitions from `src/test_query.py`
- Connects to the specified Impala cluster
- Executes each query multiple times (configurable iterations)
- Records execution times and saves results to CSV files in `test_result/`
- Supports different MT_DOP (multi-threading degree of parallelism) settings

### Test Mode for ClickHouse (`--mode test_clickhouse`)
- Loads query definitions from `src/clickhouse_query.py`
- Connects to the specified ClickHouse server
- **Clears query result cache before each execution** to measure true performance
- Executes each query multiple times (configurable iterations)
- Records execution times with high precision timing
- Saves results to CSV files in `test_result/`

See [CLICKHOUSE_TESTING.md](CLICKHOUSE_TESTING.md) for detailed ClickHouse documentation.

### Compare Mode (`--mode compare`)
- Loads performance CSV files from `test_result/`
- Performs paired t-test statistical analysis on query execution times
- Generates comprehensive PDF report with:
  - Summary statistics and charts
  - Individual query analysis with box plots
  - Performance metrics (mean, median, std, min, max)
  - Statistical significance testing
  - Detailed explanations and recommendations

### PDF Report Features
- **Visual Charts**: Box plots for each query, summary bar charts, and pie charts
- **Statistical Analysis**: t-values, p-values, significance testing
- **Performance Metrics**: Detailed execution time statistics for each sample
- **Dynamic Content**: Automatically adapts to different version names and query sets
- **Professional Layout**: Tables, headings, and structured analysis

## Prerequisites
- Python 3.9 or newer
- Access to Apache Impala cluster (optional)
- Access to ClickHouse server (optional)
- Network connectivity to the database host
- Required Python packages (installed via `requirements.txt`)

## Installation
```bash
git clone <repository-url>
cd impala_performance_test
pip install -r requirements.txt
```

## Configuration

### Impala Connection
Update the connection parameters in `main.py` or pass them as command-line arguments:
- Host: Default `10.168.49.12`
- Port: Default `21050` (native protocol)

### ClickHouse Connection
Update the connection parameters for ClickHouse:
- Host: Default `localhost`
- Port: Default `9000` (native protocol)
- Database: Default `default`
- User: Default `default`
- Password: Optional

### Test Queries

**Impala** - Modify `src/test_query.py`:
```python
def sql_improvement(ql: QueryList):
    ql.add("SELECT status, COUNT(*) FROM table GROUP BY status;", mt_dop=0)
    # Add more queries...
```

**ClickHouse** - Modify `src/clickhouse_query.py`:
```python
def get_clickhouse_queries():
    ql = ClickHouseQueryList()
    ql.add("SELECT COUNT(*) FROM table", description="Count query")
    # Add more queries...
    return [ql]
```

## Usage

### Running Performance Tests

#### Impala Performance Test
```bash
# Run tests with default settings (10 iterations)
python main.py --mode test

# Run tests with custom iterations
python main.py --mode test --iteration 20

# Run tests with custom connection
python main.py --mode test --host your-impala-host --port 21050
```

#### ClickHouse Performance Test (No Cache)
```bash
# Run tests with default settings (localhost:9000)
python main.py --mode test_clickhouse

# Run tests with custom iterations
python main.py --mode test_clickhouse --iteration 15

# Run tests with custom connection
python main.py --mode test_clickhouse \
  --ch-host ch-server.example.com \
  --ch-port 9000 \
  --ch-database default \
  --ch-user default \
  --ch-password password123
```

### Comparing Results and Generating PDF Report
```bash
# Compare results and generate PDF report
python main.py --mode compare
```

This will:
1. Load CSV files from `test_result/`
2. Perform statistical analysis
3. Generate `performance_comparison_report.pdf` with comprehensive analysis

## Command-Line Arguments

### General Arguments
| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `-M, --mode` | string | - | Mode: 'test', 'test_clickhouse', or 'compare' |
| `-I, --iteration` | int | 10 | Number of iterations per query |

### Impala Connection
| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `-H, --host` | string | 10.168.49.12 | Impala host |
| `-P, --port` | int | 21050 | Impala port |

### ClickHouse Connection
| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--ch-host` | string | localhost | ClickHouse host |
| `--ch-port` | int | 9000 | ClickHouse port (native) |
| `--ch-database` | string | default | ClickHouse database |
| `--ch-user` | string | default | ClickHouse user |
| `--ch-password` | string | - | ClickHouse password |

## Output Files

### CSV Results (`test_result/`)
- **Impala**: `apache_impala_version_X_X_X_performance.csv`
- **ClickHouse**: `clickhouse_version_X_X_X_no_cache_performance.csv`
- Columns: SQL queries (full text as headers)
- Rows: Execution times for each iteration
- Format: CSV with numeric timing data

### PDF Report
- Comprehensive analysis with charts
- Statistical significance testing
- Performance comparisons
- Recommendations for version selection

## ClickHouse Testing Features

### Query Cache Handling
The ClickHouse testing mode includes special handling for query result cache:
- Executes `SYSTEM DROP QUERY_RESULT_CACHE` before each query
- Ensures measurements reflect true query execution performance
- Gracefully handles older ClickHouse versions without cache support

### Query Categories
Pre-configured queries test:
1. Basic aggregation and GROUP BY
2. Complex multi-column aggregation
3. Filtering and predicates
4. JOIN operations
5. Subqueries
6. String operations
7. Advanced features (arrays, window functions, CASE)
8. Set operations (UNION)
9. DISTINCT operations
10. Sorting and LIMIT

See [CLICKHOUSE_TESTING.md](CLICKHOUSE_TESTING.md) for full details.

## Statistical Analysis

The comparison uses:
- **Paired t-test**: Compares means of execution times between versions
- **Significance level**: α = 0.05
- **Null hypothesis**: No performance difference between versions
- **Alternative hypothesis**: Significant performance difference exists

Results include:
- t-statistic and p-value for each query
- Rejection of null hypothesis decision
- Performance improvement direction
- Confidence intervals and effect sizes

## Troubleshooting

### Common Issues
- **Connection errors**: Verify host/port accessibility
- **Query failures**: Check SQL syntax and table permissions
- **Missing dependencies**: Run `pip install -r requirements.txt`
- **Cache clearing fails**: Some older ClickHouse versions may not support SYSTEM DROP QUERY_RESULT_CACHE
- **File not found**: Ensure CSV files exist in `test_result/` for comparison

### Performance Considerations
- Increase iterations for more reliable statistics
- Use appropriate MT_DOP settings for Impala
- Monitor resource usage during testing
- Consider query complexity and data volume
- ClickHouse cache clearing adds minimal overhead

## Related Documentation

- [CLICKHOUSE_TESTING.md](CLICKHOUSE_TESTING.md) - Detailed ClickHouse testing guide
- [src/test_clickhouse_performance.py](src/test_clickhouse_performance.py) - ClickHouse implementation
- [src/clickhouse_query.py](src/clickhouse_query.py) - ClickHouse query definitions
 