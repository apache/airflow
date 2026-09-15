#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Regenerate the checked-in JSON Schema for a materialized Airflow configuration.

Requires apache-airflow-core to be importable, e.g.:

    uv run --project airflow-core python dev/generate_config_json_schema.py
    uv run --project airflow-core python dev/generate_config_json_schema.py --check

The schema-building logic itself lives in
``airflow.config_templates.schema.build_airflow_cfg_json_schema`` (tested in
``airflow-core/tests/unit/config_templates/test_schema.py``); this script is only the
CLI wrapper that wires it up to config.yml and writes/checks the artifact file.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_PATH = (
    ROOT_DIR / "airflow-core" / "src" / "airflow" / "config_templates" / "airflow.cfg.schema.json"
)


def _render_schema() -> dict:
    from airflow.config_templates.schema import build_airflow_cfg_json_schema
    from airflow.configuration import AirflowConfigParser, retrieve_configuration_description

    # Providers are intentionally excluded: which providers are installed (and therefore which
    # extra config sections exist) varies per-deployment, so a schema shipped statically inside
    # apache-airflow-core can only describe the core options config.yml declares. Callers who want
    # a schema that also covers their installed providers can call
    # `build_airflow_cfg_json_schema(retrieve_configuration_description(include_providers=True), ...)`
    # themselves; the top-level schema already allows unrecognized sections for exactly this reason.
    config_descriptions = retrieve_configuration_description(include_providers=False)
    return build_airflow_cfg_json_schema(config_descriptions, AirflowConfigParser.enums_options)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Where to write the schema (default: %(default)s)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Do not write the file; exit non-zero if it would differ from what's on disk.",
    )
    args = parser.parse_args()

    schema = _render_schema()
    rendered = json.dumps(schema, indent=2, sort_keys=True) + "\n"

    if args.check:
        current = args.output.read_text() if args.output.exists() else None
        if current != rendered:
            print(
                f"{args.output} is out of date. Regenerate it with "
                f"`uv run --project airflow-core python dev/generate_config_json_schema.py`.",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"{args.output} is up to date.")
        return

    args.output.write_text(rendered)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
