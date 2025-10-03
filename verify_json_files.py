import json
from pathlib import Path

json_files = list(Path('raw_data').glob('*.json'))
print(f"JSON files found: {len(json_files)}")

for file in json_files:
    with open(file, 'r') as f:
        data = json.load(f)
        time_series = data.get('Time Series (Daily)', {})
        print(f"{file.name}: {len(time_series)} records")