import pyspark
import inspect
import logging
from typing import Set
from pathlib import Path
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class PySparkFunctionCollector:
    """Collects all callable functions and methods from the PySpark library."""
    
    def __init__(self):
        self.functions_and_methods: Set[str] = set()
        self.processed_objects = set()

    def recurse_members(self, obj) -> None:
        """
        Recursively inspect all members of a module or class to find callable functions/methods.
        
        Args:
            obj: The object to inspect (module, class, or function)
        """
        obj_id = id(obj)
        if obj_id in self.processed_objects:
            return
        self.processed_objects.add(obj_id)

        try:
            for name, member in inspect.getmembers(obj):
                if name.startswith('_'):
                    continue
                
                if inspect.isfunction(member) or inspect.ismethod(member) or inspect.isbuiltin(member):
                    self.functions_and_methods.add(name)
                    if hasattr(member, '__module__'):
                        full_path = f"{member.__module__}.{name}"
                        self.functions_and_methods.add(full_path)
                
                elif inspect.isclass(member):
                    if member.__module__ and member.__module__.startswith('pyspark'):
                        self.recurse_members(member)
                
                elif inspect.ismodule(member) and member.__name__.startswith('pyspark'):
                    self.recurse_members(member)
        except Exception as e:
            logging.warning(f"Error processing member {obj}: {str(e)}")

    def collect(self) -> Set[str]:
        """
        Collect all PySpark functions and methods.
        
        Returns:
            Set[str]: Set of function and method names
        """
        logging.info("Starting PySpark function collection...")
        self.recurse_members(pyspark)
        logging.info(f"Collected {len(self.functions_and_methods)} functions and methods")
        return self.functions_and_methods

def save_function_list(functions: Set[str], output_dir: Path) -> None:
    """
    Save the collected functions to both JSON and text formats.
    
    Args:
        functions: Set of function names to save
        output_dir: Directory to save the output files
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    json_data = {
        "timestamp": datetime.now().isoformat(),
        "pyspark_version": pyspark.__version__,
        "function_count": len(functions),
        "functions": sorted(list(functions))
    }
    
    json_path = output_dir / "pyspark_functions_latest.json"
    txt_path = output_dir / "pyspark_functions_latest.txt"
    
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)
    
    with open(txt_path, "w") as f:
        f.write("\n".join(sorted(functions)))
    
    logging.info(f"Saved functions to {json_path} and {txt_path}")

def main():
    output_dir = Path("pyspark_api_metadata")
    collector = PySparkFunctionCollector()
    functions = collector.collect()
    save_function_list(functions, output_dir)

if __name__ == "__main__":
    main()
