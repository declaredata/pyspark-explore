# PySpark API Usage Analyzer

Analyzes PySpark API usage in projects to identify which PySpark functions and methods are being actively used across your Spark codebase. The DeclareData Fuse team uses this tool to understand the PySpark functions used in our codebase and identify the most commonly used functions.

Note: Results are specific to your project and are gitignored. They are not to be committed to the repository and should be sent to the DeclareData Fuse team for review.

## Setup and Usage

```bash
git clone https://github.com/declaredata/pyspark-explore.git
cd pyspark-explore
pip3 install -r requirements.txt
```

```bash
# analyze your PySpark code for api functions usage
# -d: project directory to analyze
# -f: PySpark functions file must be a .json file (already generated or can be generated using the optional script below)
# -o: output directory (default: pyspark_api_usage)
python3 find_pyspark_api_usage.py \
    -d /path/to/pyspark/code/project \
    -f pyspark_api_metadata/pyspark_functions_latest.json \
    -o pyspark_api_usage

# actual example
python3 find_pyspark_api_usage.py -d /path/to/code/ -f pyspark_api_metadata/pyspark_functions_latest.json
```

```bash
# OPTIONAL: if you don't have the latest PySpark api functions metadata
# this will generate a .json and text file with all the functions
# you can use the .json file for running the find_pyspark_api_usage.py script
# this is not required if you already have the latest functions list in the pyspark_api_metadata directory
python3 generate_pyspark_api_functions.py
```

## Command Arguments

```bash
python3 find_pyspark_api_usage.py -h
  -d DIRECTORY, --directory DIRECTORY
                        Project directory to analyze
  -f FUNCTIONS_FILE, --functions-file FUNCTIONS_FILE
                        PySpark functions file (.json or .txt) from optional previous step
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory (default: pyspark_api_usage)
  -w WORKERS, --workers WORKERS
                        Number of parallel workers (default: 4)
```

## Output and Summary Files
#### The two output files should be sent to the DeclareData Fuse team for review.

The tool generates two files in your specified output directory:

1. `pyspark_usage_report.json`: Detailed analysis with function locations
```json
    {
      "function": "sum",
      "module": "pyspark.sql.functions",
      "file": "../utils/fuse_bench/src/fuse_bench/sorts.py",
      "line": 76,
      "column": 18,
      "context": "aliased_col = F.sum(<redacted>).alias(<redacted>)",
      "args": [
        "str"
      ]
    },
    {
      "function": "agg",
      "module": "pyspark.sql.dataframe",
      "file": "../utils/fuse_bench/src/fuse_bench/sorts.py",
      "line": 77,
      "column": 20,
      "context": "ordered_group = grouped.agg(<redacted>).sort(<redacted>)",
      "args": [
        "aliased_col"
      ]
    }
```

2. `pyspark_usage_summary.txt`: Simple summary of usage
```text
Total files analyzed: 8
Total PySpark function matches: 43

Functions used:
pyspark.sql.column.over: 1 occurrences
pyspark.sql.dataframe.agg: 3 occurrences
pyspark.sql.dataframe.alias: 3 occurrences
pyspark.sql.dataframe.groupBy: 3 occurrences
pyspark.sql.dataframe.limit: 1 occurrences
pyspark.sql.dataframe.orderBy: 2 occurrences
pyspark.sql.dataframe.sort: 1 occurrences
```

## Contact

[DeclareData](https://declaredata.com/)
