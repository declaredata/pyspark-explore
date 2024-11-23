# PySpark API Usage Analyzer

Analyzes PySpark API usage in projects to identify which PySpark functions and methods are being actively used across your Spark codebase. The DeclareData Fuse team uses this tool to understand the PySpark functions used in our codebase and identify the most commonly used functions.

Note: Results are specific to your project and are automatically gitignored. They are not to be committed to the repository. The should only be shared with the DeclareData Fuse team.

## Setup and Usage

```bash
git clone https://github.com/declaredata/pyspark-explore.git
cd pyspark-explore
pip3 install -r requirements.txt
```

```bash
# generate PySpark latest api functions available list
# this will generate a .json and text file with all the functions
# this is not required if you already have the latest functions list from the repository
python3 generate_pyspark_api_functions.py
```

```bash
# analyze your PySpark api functions usage
# -d: project directory to analyze
# -f: PySpark functions file (.json or .txt) from the previous step
# -o: output directory (default: pyspark_api_usage)
# this will generate a report and summary in the output directory to be sent to the DeclareData Fuse team
python3 find_pyspark_api_usage.py \
    -d /path/to/pyspark/code/project \
    -f pyspark_api_metadata/pyspark_functions_latest.json \
    -o pyspark_api_usage
```

## Command Arguments

```bash
python3 find_pyspark_api_usage.py -h
  -d DIRECTORY, --directory DIRECTORY
                        Project directory to analyze
  -f FUNCTIONS_FILE, --functions-file FUNCTIONS_FILE
                        PySpark functions file (.json or .txt)
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory (default: pyspark_api_usage)
  -w WORKERS, --workers WORKERS
                        Number of parallel workers (default: 4)
```

## Output Files and Summary
### to be sent to DeclareData Fuse team

The tool generates two files in your specified output directory:

1. `pyspark_usage_report.json`: Detailed analysis with function locations
```json
{
  "total_files_analyzed": 42,
  "total_matches": 156,
  "matches": [
    {
      "function": "collect",
      "file": "src/processor.py",
      "line": 25,
      "column": 8,
      "context": "    # Get all results\n    results = df.collect()\n    process_results(results)"
    }
  ]
}
```

2. `pyspark_usage_summary.txt`: Simple summary of usage
```text
Total files analyzed: 42
Total PySpark function matches: 156

Functions used:
collect: 15 occurrences
filter: 23 occurrences
groupBy: 12 occurrences
```

## Issues

If you see `json.decoder.JSONDecodeError`, ensure you're using the correct file:
```bash
# Try using the text file instead
python find_pyspark_api_usage.py \
    -d /path/to/pyspark/code/project \
    -f pyspark_api_metadata/pyspark_functions_latest.txt \
    -o pyspark_api_usage
```
