import ast
import logging
import json
from pathlib import Path
from typing import Set, List, Optional
from dataclasses import dataclass
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class FunctionMatch:
    """Represents a matched function with its location information."""
    name: str
    file_path: str
    line_number: int
    column: int
    context: str

def load_pyspark_functions(functions_file: Path) -> Set[str]:
    """
    Load PySpark functions from either JSON or text file.
    
    Args:
        functions_file: Path to the functions file (.json or .txt)
        
    Returns:
        Set[str]: Set of PySpark function names
    """
    try:
        if functions_file.suffix == '.json':
            with open(functions_file) as f:
                data = json.load(f)
                return set(data["functions"])
        else:  # Assume text file with one function per line
            with open(functions_file) as f:
                return set(line.strip() for line in f if line.strip())
    except Exception as e:
        logging.error(f"Error loading functions file: {e}")
        raise

class PySparkUsageAnalyzer:
    """Analyzes Python files for PySpark function usage."""
    
    def __init__(self, pyspark_functions: Set[str]):
        self.pyspark_functions = pyspark_functions
        self.matches: List[FunctionMatch] = []

    def analyze_file(self, file_path: Path) -> List[FunctionMatch]:
        """
        Analyze a single Python file for PySpark function usage.
        
        Args:
            file_path: Path to the Python file to analyze
            
        Returns:
            List[FunctionMatch]: List of matched functions with their locations
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                tree = ast.parse(content)
                file_matches = []
                
                for node in ast.walk(tree):
                    if isinstance(node, ast.Call):
                        func_name = self._get_func_name(node.func)
                        if func_name in self.pyspark_functions:
                            context = self._get_context(content, node.lineno)
                            match = FunctionMatch(
                                name=func_name,
                                file_path=str(file_path),
                                line_number=node.lineno,
                                column=node.col_offset,
                                context=context
                            )
                            file_matches.append(match)
                            
                return file_matches
                
        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")
            return []

    def _get_func_name(self, node: ast.AST) -> Optional[str]:
        """Extract the function name from an AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return node.attr
        return None

    def _get_context(self, content: str, line_number: int, context_lines: int = 2) -> str:
        """Get the context around a specific line in the file."""
        lines = content.splitlines()
        start = max(0, line_number - context_lines - 1)
        end = min(len(lines), line_number + context_lines)
        return "\n".join(lines[start:end])

def analyze_directory(
    directory: Path,
    pyspark_functions: Set[str],
    output_dir: Path,
    max_workers: int = 4
) -> None:
    """
    Analyze all Python files in a directory for PySpark function usage.
    
    Args:
        directory: Directory to analyze
        pyspark_functions: Set of PySpark function names to match against
        output_dir: Directory to save the results
        max_workers: Maximum number of parallel workers
    """
    analyzer = PySparkUsageAnalyzer(pyspark_functions)
    python_files = list(directory.rglob("*.py"))
    matches: List[FunctionMatch] = []
    
    logging.info(f"Found {len(python_files)} Python files to analyze")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(analyzer.analyze_file, file_path): file_path 
            for file_path in python_files
        }
        
        for future in as_completed(future_to_file):
            file_matches = future.result()
            matches.extend(file_matches)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    report = {
        "total_files_analyzed": len(python_files),
        "total_matches": len(matches),
        "matches": [
            {
                "function": match.name,
                "file": match.file_path,
                "line": match.line_number,
                "column": match.column,
                "context": match.context
            }
            for match in matches
        ]
    }
    
    with open(output_dir / "pyspark_usage_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    with open(output_dir / "pyspark_usage_summary.txt", "w") as f:
        f.write(f"Total files analyzed: {len(python_files)}\n")
        f.write(f"Total PySpark function matches: {len(matches)}\n\n")
        f.write("Functions used:\n")
        for func_name in sorted({match.name for match in matches}):
            count = sum(1 for match in matches if match.name == func_name)
            f.write(f"{func_name}: {count} occurrences\n")

def main():
    parser = argparse.ArgumentParser(description="Analyze PySpark function usage in a project")
    parser.add_argument("-d", "--directory", required=True, type=Path, help="Project directory to analyze")
    parser.add_argument("-f", "--functions-file", required=True, type=Path, help="PySpark functions file (.json or .txt)")
    parser.add_argument("-o", "--output-dir", type=Path, default=Path("pyspark_api_usage"), help="Output directory")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Number of parallel workers")
    args = parser.parse_args()

    pyspark_functions = load_pyspark_functions(args.functions_file)
    analyze_directory(args.directory, pyspark_functions, args.output_dir, args.workers)
    logging.info(f"Analysis complete. Results saved to {args.output_dir}")

if __name__ == "__main__":
    main()
