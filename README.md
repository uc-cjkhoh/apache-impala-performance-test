# Apache Impala Performance Test and Comparison

## Overview
This repository provides a comprehensive framework for running Apache Impala query performance tests, comparing results between different versions, and generating detailed PDF reports with visual charts.

Key features:
- Execute query workloads against an Impala cluster with configurable iterations
- Measure and record query execution times
- Perform statistical A/B testing comparison between two Impala versions
- Generate professional PDF reports with box plots, summary charts, and detailed performance statistics
- Support for dynamic version labeling and flexible query sets

## Repository Structure
```
impala_4_3_0_config.txt          # Configuration file for Impala 4.3.0
impala_4_5_0_config.txt          # Configuration file for Impala 4.5.0
main.py                          # Main entry point script
README.md                        # This file
requirements.txt                 # Python dependencies
src/
    __init__.py
    compare_performance.py       # Statistical comparison logic
    plot_charts.py               # PDF report generation with charts
    test_performance.py          # Query execution and timing
    test_query.py                # Query definitions and configurations
    __pycache__/                 # Python cache files
test_result/
    apache_impalad_version_4.3.0_performance.csv  # Performance results for version 4.3.0
    apache_impalad_version_4.5.0_performance.csv  # Performance results for version 4.5.0
```

## How It Works

### Test Mode (`--mode test`)
- Loads query definitions from `src/test_query.py`
- Connects to the specified Impala cluster
- Executes each query multiple times (configurable iterations)
- Records execution times and saves results to CSV files in `test_result/`
- Supports different MT_DOP (multi-threading degree of parallelism) settings

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
- Access to an Apache Impala cluster
- Network connectivity to the Impala host
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
- Port: Default `21050`

### Test Queries
Modify `src/test_query.py` to add or change query sets:
```python
def get_queries():
    ql = QueryList()
    ql.add("SELECT status, COUNT(*) FROM table GROUP BY status;", mt_dop=0)
    # Add more queries...
    return [ql]
```

### Configuration Files
- `impala_4_3_0_config.txt`: Configuration for Impala 4.3.0
- `impala_4_5_0_config.txt`: Configuration for Impala 4.5.0

## Usage

### Running Performance Tests
```bash
# Run tests with default settings (10 iterations)
python main.py --mode test

# Run tests with custom iterations
python main.py --mode test --iteration 20

# Run tests with custom connection
python main.py --mode test --host your-impala-host --port 21050
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

### Direct PDF Generation
```bash
# Generate PDF from existing results
python src/plot_charts.py --data-folder test_result --output custom_report.pdf
```

## Output Files

### CSV Results (`test_result/`)
- Columns: SQL queries (full text as headers)
- Rows: Execution times for each iteration
- Format: `apache_<version>_performance.csv`

### PDF Report
- Comprehensive analysis with charts
- Statistical significance testing
- Performance comparisons
- Recommendations for version selection

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
- **Connection errors**: Verify Impala host/port accessibility
- **Query failures**: Check SQL syntax and table permissions
- **Missing dependencies**: Run `pip install -r requirements.txt`
- **File not found**: Ensure CSV files exist in `test_result/` for comparison

### Performance Considerations
- Increase iterations for more reliable statistics
- Use appropriate MT_DOP settings for your cluster
- Monitor cluster resources during testing
- Consider query complexity and data volume

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

