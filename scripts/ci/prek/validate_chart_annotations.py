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
#   "rich>=13.6.0",
# ]
# ///

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_ROOT_PATH, console

CHART_YAML_FILE = AIRFLOW_ROOT_PATH / "chart" / "Chart.yaml"

# List of artifacthub.io annotations that should contain valid YAML strings
ARTIFACTHUB_ANNOTATIONS_TO_CHECK = [
    "artifacthub.io/links",
    "artifacthub.io/screenshots",
    "artifacthub.io/changes",
]


def validate_chart_annotations():
    """Validate that chart annotations are valid - e.g. strings that hold yaml are valid yaml"""
    errors = []

    try:
        chart_content = yaml.safe_load(CHART_YAML_FILE.read_text())
    except yaml.YAMLError as e:
        console.print(f"[red]Error parsing Chart.yaml: {e}")
        return False

    annotations = chart_content.get("annotations", {})

    for annotation_key in ARTIFACTHUB_ANNOTATIONS_TO_CHECK:
        if annotation_key in annotations:
            annotation_value = annotations[annotation_key]
            try:
                # Try to parse the string value as YAML
                yaml.safe_load(annotation_value)
                console.print(f"[green]✓[/] {annotation_key} contains valid YAML")
            except yaml.YAMLError as e:
                error_msg = f"Invalid YAML in annotation '{annotation_key}': {e}"
                errors.append(error_msg)
                console.print(f"[red]✗ {error_msg}[/]")

    if errors:
        console.print("\n[red]Chart.yaml validation failed![/]")
        console.print("[red]The following artifacthub.io annotations contain invalid YAML:[/]")
        for error in errors:
            console.print(f"[red]  - {error}[/]")
        return False

    console.print("\n[green]All artifacthub.io annotations contain valid YAML![/]")
    return True


if __name__ == "__main__":
    if not validate_chart_annotations():
        sys.exit(1)
