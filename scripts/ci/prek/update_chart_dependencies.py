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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "pyyaml>=6.0.2",
#   "requests>=2.31.0",
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import json
import sys
from pathlib import Path

import requests
import yaml

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_ROOT_PATH, console

VALUES_YAML_FILE = AIRFLOW_ROOT_PATH / "chart" / "values.yaml"
VALUES_SCHEMA_FILE = AIRFLOW_ROOT_PATH / "chart" / "values.schema.json"


def get_latest_prometheus_statsd_exporter_version() -> str:
    quay_data = requests.get("https://quay.io/api/v1/repository/prometheus/statsd-exporter/tag/").json()
    for version in quay_data["tags"]:
        if version["name"].startswith("v"):
            return version["name"]
    raise RuntimeError("ERROR! No version found")


if __name__ == "__main__":
    yaml_content = yaml.safe_load(VALUES_YAML_FILE.read_text())
    chart_prometheus_statsd_exporter_version = yaml_content["images"]["statsd"]["tag"]
    latest_prometheus_statsd_exporter_version = get_latest_prometheus_statsd_exporter_version()
    if chart_prometheus_statsd_exporter_version != latest_prometheus_statsd_exporter_version:
        console.print(
            f"[yellow]Updating prometheus statsd exporter version "
            f"from {chart_prometheus_statsd_exporter_version} "
            f"to {latest_prometheus_statsd_exporter_version}"
        )
    else:
        console.print("[green]Prometheus statsd exporter version is up to date")
        sys.exit(0)
    yaml_lines = VALUES_YAML_FILE.read_text().splitlines()
    result_lines = []
    replace = False
    for index, line in enumerate(yaml_lines):
        if "repository: quay.io/prometheus/statsd-exporter" in line:
            replace = True
            next_line = line
        elif replace:
            if line.startswith("    tag:"):
                next_line = f"    tag: {latest_prometheus_statsd_exporter_version}"
                replace = False
            else:
                raise ValueError(
                    f"ERROR! The next line after repository: should be tag: - "
                    f"index {index} in {VALUES_YAML_FILE}"
                )
        else:
            next_line = line
        result_lines.append(next_line)
    result_lines.append("")
    VALUES_YAML_FILE.write_text("\n".join(result_lines))
    schema_file = json.loads(VALUES_SCHEMA_FILE.read_text())
    schema_file["properties"]["images"]["properties"]["statsd"]["properties"]["tag"]["default"] = (
        latest_prometheus_statsd_exporter_version
    )
    VALUES_SCHEMA_FILE.write_text(json.dumps(schema_file, indent=4) + "\n")
