import os
import json
from pathlib import Path
from datetime import datetime


KEYS = [
    "pandas-dev/pandas",
    "numpy/numpy",
    "scikit-learn/scikit-learn",
    "apache/airflow",
    "mlflow/mlflow"
]


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")


def save_data(data, filename):
    """Save data to JSON file in the data/raw directory"""
    raw_data_dir = Path("data/raw")
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    filepath = raw_data_dir / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Saved to {filepath}")


def main():
    """Main execution function"""

    print("="*60)
    print("Combining All JSON Files into Single File")
    print("="*60)

    raw_data_dir = Path("data/raw")
    if not raw_data_dir.exists():
        print("No data/raw directory found. Exiting.")
        return

    # Find all JSON files in data/raw
    json_files = list(raw_data_dir.glob("*.json"))

    if not json_files:
        print("No JSON files found in data/raw. Exiting.")
        return

    print(f"\nFound {len(json_files)} JSON files:")
    for file in json_files:
        print(f"  - {file.name}")

    # Combine all JSON data
    combined_data = {}

    for json_file in json_files:
        print(f"\nReading {json_file.name}...")
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for key in KEYS:
                    if key in data:
                        data[key]['repository'] = key
                combined_data[key] = data
                print(f"  ✓ Successfully loaded")
        except Exception as e:
            print(f"  ✗ Error loading {json_file.name}: {e}")

    # Save combined data
    output_filename = f"combined_data_{timestamp}.json"
    save_data(combined_data, output_filename)

    print(f"\n{'='*60}")
    print(f"Successfully combined {len(combined_data)} files")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
