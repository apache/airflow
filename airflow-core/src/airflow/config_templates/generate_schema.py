# Generate JSON Schema for airflow.cfg from config.yml.

from pathlib import Path
from typing import Dict, Any
from collections import OrderedDict
import json

import yaml


def map_type_to_json_schema(
    yaml_type: str, option_data: Dict[str, Any]
) -> OrderedDict:
    schema = OrderedDict()

    if yaml_type == "boolean":
        schema["oneOf"] = [
            {"type": "boolean"},
            {
                "type": "string",
                "enum": [
                    "true", "false",
                    "True", "False",
                    "1", "0",
                    "yes", "no",
                    "Yes", "No",
                ],
            },
        ]
    else:
        type_mapping = {
            "string": "string",
            "integer": "integer",
            "float": "number",
        }
        schema["type"] = type_mapping.get(yaml_type, "string")

    if option_data.get("description"):
        schema["description"] = option_data["description"]

    if option_data.get("default") not in (None, ""):
        default = option_data["default"]
        try:
            if yaml_type == "integer":
                default = int(default)
            elif yaml_type == "float":
                default = float(default)
        except (ValueError, TypeError):
            pass
        schema["default"] = default

    if option_data.get("example"):
        schema["examples"] = [option_data["example"]]

    return schema


def generate_json_schema(config: Dict[str, Any]) -> OrderedDict:
    schema = OrderedDict(
        [
            ("$schema", "http://json-schema.org/draft-07/schema#"),
            ("title", "Apache Airflow Configuration"),
            (
                "description",
                "JSON Schema for Apache Airflow configuration file (airflow.cfg)",
            ),
            ("type", "object"),
            ("properties", OrderedDict()),
            ("additionalProperties", False),
        ]
    )

    for section_name in sorted(config.keys()):
        section = config[section_name]
        options = section.get("options")

        if not options:
            continue

        section_properties = OrderedDict()

        for option_name in sorted(options.keys()):
            option_data = options[option_name]
            option_schema = map_type_to_json_schema(
                option_data.get("type", "string"),
                option_data,
            )
            section_properties[option_name] = option_schema

        section_schema = OrderedDict(
            [
                ("type", "object"),
                ("properties", section_properties),
                ("additionalProperties", False),
            ]
        )

        if section.get("description"):
            section_schema["description"] = section["description"]

        schema["properties"][section_name] = section_schema

    return schema


def main() -> None:
    base_dir = Path(__file__).parent
    config_path = base_dir / "config.yml"
    output_path = base_dir / "schema.json"

    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    schema = generate_json_schema(config)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    print(f"Generated JSON Schema at {output_path}")


if __name__ == "__main__":
    main()
