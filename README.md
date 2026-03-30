# Apache Impala - Performance Test and Comparison

## Description
This project have two purposes: 
1. Generate Apache Impala performance report of targeted version.
2. Compare performance between two or more Impala version (based on reports).

## Structure
```
src/
    __init__.py
    test_query.py
    test_performance.py
    compare_performance.py
main.py
requirements.txt
```

## How it Works
```
```

## How to Use
### Prerequisite
- python 3.9+
- git

### Installation Guide
Clone this repo
```bash
git clone <repo-url>

cd <repo_name>

pip install -r requirements.txt
```

Insert queries in file `src/test_query.py`
```
Eg. querylist.add("ANY QUERY")
```

For performance testing:
```bash
python main.py --mode "test"
```

For compare performance:
```bash
python main.py --mode "compare" <report_1> <report_2> ... <report_n>
```