# Quick Start Guide - ClickHouse Performance Testing

## 5-Minute Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run ClickHouse Performance Test
```bash
# Test with default settings (localhost:9000, 10 iterations)
python main.py --mode test_clickhouse

# Test with custom iterations
python main.py --mode test_clickhouse --iteration 20

# Test against remote server
python main.py --mode test_clickhouse \
  --ch-host 192.168.1.100 \
  --ch-port 9000 \
  --ch-user admin \
  --ch-password secret \
  --iteration 15
```

### 3. View Results
Results are saved to:
```
test_result/clickhouse_version_X_X_X_no_cache_performance.csv
```

## Understanding the Output

### Console Output
```
Testing queries: 100%|████████| 10/10
Iterations: 100%|████████| 15/15
Performance test completed.
Results saved to: test_result/clickhouse_version_23_8_1_no_cache_performance.csv
```

### CSV Output
A file with execution times (in seconds) for each query:
- **Columns**: Each SQL query
- **Rows**: Execution time for each iteration
- **Values**: Floating point seconds (e.g., 0.0234)

Example:
```
SELECT COUNT(*) as total_rows FROM system.query_log,SELECT type, COUNT(*) FROM system.query_log GROUP BY type,...
0.0234,0.0125,...
0.0241,0.0118,...
```

## Common Tasks

### Run Multiple Test Iterations
```bash
# 50 iterations for statistical significance
python main.py --mode test_clickhouse --iteration 50
```

### Test Against Production Server
```bash
python main.py --mode test_clickhouse \
  --ch-host clickhouse.prod.example.com \
  --ch-port 9000 \
  --ch-user readonly \
  --ch-password $(cat ~/.ch_password)
```

### Compare Multiple Versions
1. Run test on ClickHouse 23.3:
```bash
python main.py --mode test_clickhouse --ch-host ch-23-3.local --iteration 20
```

2. Run test on ClickHouse 23.8:
```bash
python main.py --mode test_clickhouse --ch-host ch-23-8.local --iteration 20
```

3. Compare results:
```bash
python main.py --mode compare
```

### Analyze Results With Python
```python
import pandas as pd

# Load results
df = pd.read_csv('test_result/clickhouse_version_23_8_1_no_cache_performance.csv')

# Show summary statistics
print(df.describe())

# Get average execution time per query
print("Average execution times (seconds):")
print(df.mean())

# Find slowest queries
print("\nSlowest queries (max execution time):")
print(df.max().sort_values(ascending=False).head(10))
```

## Features Tested

The default query set includes 10 categories testing:

| Category | Examples |
|----------|----------|
| 1. **Aggregation** | COUNT(), simple GROUP BY |
| 2. **Multi-Column Aggregation** | Complex GROUP BY, multiple aggregates |
| 3. **Filtering** | Complex WHERE, range queries |
| 4. **Joins** | INNER JOIN, LEFT JOIN |
| 5. **Subqueries** | WHERE subqueries, IN operators |
| 6. **String Operations** | LIKE patterns, string functions |
| 7. **Advanced** | Arrays, window functions, CASE |
| 8. **Set Operations** | UNION queries |
| 9. **Deduplication** | DISTINCT |
| 10. **Sorting** | ORDER BY, LIMIT |

## Troubleshooting

### "Connection refused"
```bash
# Check if ClickHouse is running
telnet localhost 9000

# Try with explicit host
python main.py --mode test_clickhouse --ch-host 127.0.0.1
```

### "Authentication failed"
```bash
# Verify credentials
python main.py --mode test_clickhouse \
  --ch-user default \
  --ch-password ""
```

### "Query error"
The default queries use `system.query_log` table. Ensure it's available:
```bash
# Connect to ClickHouse and check
clickhouse-client
> SELECT * FROM system.query_log LIMIT 1;
```

### "Cache clear warning"
```
Warning: Could not clear query cache: ...
```
This is normal on older ClickHouse versions. Testing will continue, but may include cached results.

## Next Steps

1. **Analyze Results**: Use Python/pandas to analyze the CSV output
2. **Compare Versions**: Run tests on multiple ClickHouse versions
3. **Generate Reports**: Use `--mode compare` to generate PDF reports
4. **Custom Queries**: Edit `src/clickhouse_query.py` to test your own queries
5. **Automate**: Create scripts to run tests on a schedule

## Full Documentation

For detailed documentation, see:
- [CLICKHOUSE_TESTING.md](CLICKHOUSE_TESTING.md) - Complete guide
- [README.md](README.md) - Project overview
- [src/test_clickhouse_performance.py](src/test_clickhouse_performance.py) - Implementation details

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review CLICKHOUSE_TESTING.md for advanced options
3. Examine the code in `src/test_clickhouse_performance.py`
4. Run with increased verbosity for debugging
