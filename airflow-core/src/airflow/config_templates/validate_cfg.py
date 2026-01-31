"""
This file is for validation only.

This script is OPTIONAL and intended for:
- Developers
- CI pipelines
- Local validation

NOTE:
- Not used by Airflow at runtime
- Does not change config parsing behavior
"""


import json
import configparser
from pathlib import Path
import jsonschema


schema_path = Path(
    "airflow-core/src/airflow/config_templates/schema.json"
)
schema = json.loads(schema_path.read_text())
cfg = configparser.ConfigParser()
cfg.read("airflow.cfg")
data = {section: dict(cfg[section]) for section in cfg.sections()}
jsonschema.validate(instance=data, schema=schema)

print(" airflow.cfg is valid")
