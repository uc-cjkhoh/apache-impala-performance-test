# ClickHouse Query Performance Testing (No Cache)

## Overview

This feature enables performance testing of ClickHouse queries **without query result caching**. This is essential for measuring true query execution performance, unclouded by cache effects.

## Key Features

- **Query Cache Disabled**: Clears query result cache before each execution using `SYSTEM DROP QUERY_RESULT_CACHE`
- **Precise Timing**: Measures query execution time using Python's high-resolution `perf_counter()`
- **Multiple Iterations**: Runs each query multiple times to collect statistically meaningful data
- **CSV Output**: Saves results in CSV format for comparison and analysis
- **System Table Queries**: Pre-defined query set using ClickHouse system tables for benchmarking
- **Error Handling**: Graceful handling of cache clearing failures in older ClickHouse versions

## Query Categories

The pre-defined query set (`src/clickhouse_query.py`) includes 10 categories:

1. **Basic Aggregation** - COUNT, simple GROUP BY
2. **Multi-Column Aggregation** - Complex GROUP BY with multiple functions
3. **Filtering & Predicates** - Complex WHERE clauses, range queries
4. **Joins** - INNER JOIN, LEFT JOIN performance
5. **Subqueries** - WHERE subqueries, IN operators
6. **String Operations** - LIKE patterns, string functions
7. **Advanced Features** - Array functions, CASE expressions, window functions
8. **Set Operations** - UNION queries
9. **Deduplication** - DISTINCT operations
10. **Sorting & Limits** - ORDER BY performance

Each query tests a specific aspect of ClickHouse query performance.

## Usage

### Basic Command

```bash
python main.py --mode test_clickhouse \
  --ch-host localhost \
  --ch-port 9000 \
  --iteration 10
```

### Advanced Options

```bash
python main.py --mode test_clickhouse \
  --ch-host your-clickhouse-server \
  --ch-port 9000 \
  --ch-database default \
  --ch-user default \
  --ch-password your-password \
  --iteration 20
```

### Connection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--ch-host` | `localhost` | ClickHouse server hostname/IP |
| `--ch-port` | `9000` | ClickHouse native protocol port |
| `--ch-database` | `default` | Database to use |
| `--ch-user` | `default` | Database user |
| `--ch-password` | empty | Database password |
| `--iteration` | `10` | Number of times to run each query |

## How It Works

### 1. Connection Establishment
```python
client = Client(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password,
    settings={'log_queries': 1}
)
```

### 2. Query Validation
All queries are validated using `EXPLAIN` before performance testing begins.

### 3. Warm-up
Each query is executed once with cache cleared to warm up caches and prepare the query engine.

### 4. Performance Testing
For each query:
- Clear query result cache: `SYSTEM DROP QUERY_RESULT_CACHE`
- Measure precise start time using `perf_counter()`
- Execute query
- Measure precise end time
- Record execution time
- Repeat for N iterations

### 5. Result Storage
Results are saved to CSV:
```
test_result/clickhouse_version_X_X_X_no_cache_performance.csv
```

## Output Format

The CSV file contains one column per query with timing data:

```
SELECT COUNT(*) as total_rows FROM system.query_log,SELECT COUNT(*) as query_count FROM system.query_log WHERE type = 'QueryStart',...
0.0234,0.0189,...
0.0241,0.0195,...
0.0237,0.0192,...
...
```

Each row represents one iteration with all query execution times.

## Implementation Details

### Test Class: `TestClickHousePerformance`

Located in [src/test_clickhouse_performance.py](../src/test_clickhouse_performance.py)

Key methods:
- `__init__()`: Initialize with connection parameters
- `get_clickhouse_version()`: Extract ClickHouse version
- `clear_query_cache()`: Clear query result cache
- `get_performance()`: Execute all queries and measure timing
- `save_result()`: Save results to CSV file

### Query Definition: `ClickHouseQueryList`

Located in [src/clickhouse_query.py](../src/clickhouse_query.py)

- Container class for queries and descriptions
- `add()` method to register queries
- `get_clickhouse_queries()` function returns predefined query set

## Performance Considerations

### Cache Clearing Overhead
Query result cache clearing adds minimal overhead but is necessary for accurate measurements.

### System Table Queries
The default query set uses `system.query_log` table for consistency and availability.

### Timing Resolution
Uses `perf_counter()` for nanosecond-level precision on most systems.

## Troubleshooting

### Connection Refused
```
Ensure ClickHouse is running and accessible on the specified host:port
```

### Query Cache Not Available
```
If SYSTEM DROP QUERY_RESULT_CACHE fails, the system will log a warning but continue testing.
Some older ClickHouse versions may not support this command.
```

### Port Mismatch
- Native protocol: `9000` (TCP)
- HTTP protocol: `8123` (HTTP)
- HTTPS protocol: `8443` (HTTPS)

Always use port `9000` for native protocol connections.

## Example Workflow

### 1. Run Performance Test
```bash
python main.py --mode test_clickhouse \
  --ch-host ch-server.example.com \
  --iteration 15
```

Output:
```
Testing queries: 100%|████████████| 10/10
Iterations: 100%|████████████| 15/15
Performance test completed. Results saved to: test_result/clickhouse_version_23_8_1_no_cache_performance.csv
```

### 2. Analyze Results
Use pandas or your preferred tool:
```python
import pandas as pd
df = pd.read_csv('test_result/clickhouse_version_23_8_1_no_cache_performance.csv')
print(df.describe())
```

### 3. Compare Versions
Run tests on multiple ClickHouse versions and use the compare mode to analyze differences.

## Advanced Usage

### Custom Queries

To test custom queries, modify `src/clickhouse_query.py`:

```python
def get_clickhouse_queries():
    ql = ClickHouseQueryList()
    
    # Add your custom query
    ql.add(
        '''
        SELECT your_column, COUNT(*)
        FROM your_table
        GROUP BY your_column
        ''',
        description="Your query description"
    )
    
    return [ql]
```

### Multiple Query Sets

Return multiple `ClickHouseQueryList` objects for different benchmarks:

```python
def get_clickhouse_queries():
    ql1 = ClickHouseQueryList()
    ql1.add("SELECT COUNT(*) FROM table1", "Benchmark set 1")
    
    ql2 = ClickHouseQueryList()
    ql2.add("SELECT COUNT(*) FROM table2", "Benchmark set 2")
    
    return [ql1, ql2]  # Both will be tested
```

## Related Features

- **Impala Testing**: Use `--mode test` for Apache Impala performance testing
- **Performance Comparison**: Use `--mode compare` to compare multiple test runs
- **PDF Reports**: Generates detailed PDF reports with statistical analysis and charts

## Dependencies

- `clickhouse-driver`: ClickHouse client library
- `pandas`: Data frame manipulation
- `tqdm`: Progress bars

Install with:
```bash
pip install -r requirements.txt
```
