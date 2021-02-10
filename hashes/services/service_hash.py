import csv
from pathlib import Path


base_path = Path(__file__).parent
file_path = (base_path / "dhl_service_hash.csv").resolve()
with open(file_path, mode='r') as infile:
    reader = csv.reader(infile)
    dhl_services = {rows[0]: rows[1] for rows in reader}
