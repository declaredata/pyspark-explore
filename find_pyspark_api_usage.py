import ast
import logging
import json
import re
from pathlib import Path
from typing import Set, Dict, List, Optional, Tuple
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
    module: str
    file_path: str
    line_number: int
    column: int
    context: str
    args: List[str]

def load_pyspark_functions(functions_file: Path) -> Set[Tuple[str, str]]:
    """
    Load PySpark functions from JSON file.
    
    Args:
        functions_file: Path to the functions JSON file
        
    Returns:
        Set[Tuple[str, str]]: Set of (function_name, module_name) tuples
    """
    try:
        with open(functions_file) as f:
            data = json.load(f)
            return {(func["name"], func["module"]) for func in data["functions"]}
    except Exception as e:
        logging.error(f"Error loading functions file: {e}")
        raise

class PySparkUsageAnalyzer:
    """Analyzes Python files for PySpark function usage."""
    
    def __init__(self, pyspark_functions: Set[Tuple[str, str]]):
        self.function_modules: Dict[str, Set[str]] = {}
        for name, module in pyspark_functions:
            if name not in self.function_modules:
                self.function_modules[name] = set()
            self.function_modules[name].add(module)
        
        # functions that need context verification
        self.verify_functions = {
            'join',   # os.path.join vs pyspark join
            'split',  # string split vs pyspark split
            'get',    # dict get vs pyspark get
            'append', # list append vs pyspark append
            'count',  # len vs pyspark count
            'sum',    # built-in sum vs pyspark sum
        }
        
        self.ignored_paths = {
            '.venv',
            'site-packages',
            '__pycache__',
            'tests',
            'test_',
            'venv',
        }

    def _should_ignore_path(self, file_path: str) -> bool:
        """Check if file path should be ignored."""
        return any(pattern in file_path for pattern in self.ignored_paths)

    def _get_arg_types(self, node: ast.Call) -> List[str]:
        """Extract argument types from a function call."""
        arg_types = []
        for arg in node.args:
            if isinstance(arg, ast.Name):
                arg_types.append(arg.id)
            elif isinstance(arg, ast.Attribute):
                full_name = []
                current = arg
                while isinstance(current, ast.Attribute):
                    full_name.insert(0, current.attr)
                    current = current.value
                if isinstance(current, ast.Name):
                    full_name.insert(0, current.id)
                arg_types.append('.'.join(full_name))
            elif isinstance(arg, ast.Constant):
                # Handle literals
                arg_types.append(type(arg.value).__name__)
            elif isinstance(arg, ast.List):
                arg_types.append('list')
            elif isinstance(arg, ast.Dict):
                arg_types.append('dict')
            elif isinstance(arg, ast.Call):
                arg_types.append('Call')
            else:
                arg_types.append(type(arg).__name__)
        return arg_types

    def _get_context(self, content: str, line_number: int) -> str:
        """Get minimal redacted context for a function call."""
        lines = content.splitlines()
        if 0 <= line_number - 1 < len(lines):
            line = lines[line_number - 1].strip()
            redacted = re.sub(r'"[^"]*"', '"<redacted>"', line)
            redacted = re.sub(r'\'[^\']*\'', '\'<redacted>\'', redacted)
            redacted = re.sub(r'\([^)]+\)', '(<redacted>)', redacted)
            return redacted
        return ""

    def analyze_file(self, file_path: Path) -> List[FunctionMatch]:
        """
        Analyze a single Python file for PySpark function usage.
        
        Args:
            file_path: Path to the Python file to analyze
            
        Returns:
            List[FunctionMatch]: List of matched functions with their locations
        """
        if self._should_ignore_path(str(file_path)):
            return []

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                tree = ast.parse(content)
                file_matches = []
                
                for node in ast.walk(tree):
                    if isinstance(node, ast.Call):
                        func_name = self._get_func_name(node.func)
                        if func_name and func_name in self.function_modules:
                            module = self._determine_module(node, func_name)
                            if module:
                                match = FunctionMatch(
                                    name=func_name,
                                    module=module,
                                    file_path=str(file_path),
                                    line_number=node.lineno,
                                    column=node.col_offset,
                                    context=self._get_context(content, node.lineno),
                                    args=self._get_arg_types(node)
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

    def _determine_module(self, node: ast.Call, func_name: str) -> Optional[str]:
        """Determine the most likely module for a function."""
        available_modules = self.function_modules.get(func_name, set())
        
        if not available_modules:
            return None
            
        if len(available_modules) == 1:
            return available_modules.pop()

        sql_modules = [m for m in available_modules if 'sql' in m]
        if sql_modules:
            for prefix in ['sql.functions', 'sql.dataframe', 'sql.column']:
                specific = [m for m in sql_modules if prefix in m]
                if specific:
                    return specific[0]
            return sql_modules[0]

        return available_modules.pop()

def analyze_directory(
    directory: Path,
    pyspark_functions: Set[Tuple[str, str]],
    output_dir: Path,
    max_workers: int = 4
) -> None:
    """
    Analyze all Python files in a directory for PySpark function usage.
    
    Args:
        directory: Directory to analyze
        pyspark_functions: Set of function names and modules to match against
        output_dir: Directory to save the results
        max_workers: Maximum number of parallel workers
    """
    analyzer = PySparkUsageAnalyzer(pyspark_functions)
    python_files = list(directory.rglob("*.py"))
    matches: List[FunctionMatch] = []
    ignored_files: List[str] = []
    
    logging.info(f"Found {len(python_files)} Python files to analyze")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(analyzer.analyze_file, file_path): file_path 
            for file_path in python_files
        }
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            if analyzer._should_ignore_path(str(file_path)):
                ignored_files.append(str(file_path))
                continue
            
            file_matches = future.result()
            matches.extend(file_matches)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    report = {
        "total_files_analyzed": len(python_files) - len(ignored_files),
        "total_matches": len(matches),
        "ignored_files": ignored_files,
        "matches": [
            {
                "function": match.name,
                "module": match.module,
                "file": match.file_path,
                "line": match.line_number,
                "column": match.column,
                "context": match.context,
                "args": match.args
            }
            for match in matches
        ]
    }
    
    with open(output_dir / "pyspark_usage_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    with open(output_dir / "pyspark_usage_summary.txt", "w") as f:
        f.write(f"Total files analyzed: {len(python_files) - len(ignored_files)}\n")
        f.write(f"Total PySpark function matches: {len(matches)}\n\n")
        f.write("Functions used:\n")
        function_counts = {}
        for match in matches:
            key = f"{match.module}.{match.name}"
            function_counts[key] = function_counts.get(key, 0) + 1
        for func, count in sorted(function_counts.items()):
            f.write(f"{func}: {count} occurrences\n")

def main():
    parser = argparse.ArgumentParser(description="Analyze PySpark function usage in a project")
    parser.add_argument("-d", "--directory", required=True, type=Path, 
                       help="Project directory to analyze")
    parser.add_argument("-f", "--functions-file", required=True, type=Path, 
                       help="PySpark functions JSON file (pyspark_functions_latest.json)")
    parser.add_argument("-o", "--output-dir", type=Path, default=Path("pyspark_api_usage"), 
                       help="Output directory")
    parser.add_argument("-w", "--workers", type=int, default=4, 
                       help="Number of parallel workers")
    args = parser.parse_args()

    if args.functions_file.suffix != '.json':
        parser.error("The functions file must be a JSON file (pyspark_functions_latest.json)")

    pyspark_functions = load_pyspark_functions(args.functions_file)
    analyze_directory(args.directory, pyspark_functions, args.output_dir, args.workers)
    logging.info(f"Analysis complete. Results saved to {args.output_dir}")

if __name__ == "__main__":
    main()
