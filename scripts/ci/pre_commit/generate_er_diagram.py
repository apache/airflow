#!/usr/bin/env python3
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
Module to generate Entity Relationship Diagram by reading SQLAlchemy models.
The settings are saved in the pyproject.toml, section [tool.paracelsus]
and you can use it to set excludes, includes, and column sorting.
"""

from __future__ import annotations

import os
from pathlib import Path

import subprocess
from rich.console import Console

SVG_FILE = Path("docs") / "apache-airflow" / "img" / "airflow_erd.svg"
MARKDOWN_FILE = "README.md"

def inject_mermaid_diagram():
    console = Console(width=400, color_system="standard")
    try:
        paracelsus_process = subprocess.run(
            ['paracelsus', 'inject', MARKDOWN_FILE, 'airflow.models.base:Base', '--import-module', 'airflow.models:*'],
            capture_output=True,
            text=True
        )
        if paracelsus_process.returncode != 0:
            console.print("[red]Error running paracelsus command:", paracelsus_process.stderr)
            return
        
        console.print(f"[green]The mermaid diagram was injecte in {MARKDOWN_FILE}. Please commit the changes!")
    except FileNotFoundError as fnf_error:
        console.print("[red]Command not found:", fnf_error)
    except Exception as e:
        console.print("An error occurred:", str(e))


def generate_svg():
    console = Console(width=400, color_system="standard")
    console.print("[bright_blue]Preparing diagram for Airflow ERD")
    try:
        paracelsus_process = subprocess.run(
            ['paracelsus', 'graph', '--format', 'dot'],
            capture_output=True,
            text=True
        )

        if paracelsus_process.returncode != 0:
            console.print("[red]Error running paracelsus command:", paracelsus_process.stderr)
            return

        output_without_warnings = '\n'.join(
            line for line in paracelsus_process.stdout.splitlines() if "WARNING" not in line
        )

        dot_process = subprocess.run(
            ['dot', '-Tsvg'],
            input=output_without_warnings,
            capture_output=True,
            text=True
        )

        if dot_process.returncode != 0:
            console.print("[red]Error running dot command:", dot_process.stderr)
            return

        with open(SVG_FILE, 'w') as f:
            f.write(dot_process.stdout)

        console.print(f"[green]The diagram has been generated in {SVG_FILE}. Please commit the changes!")

    except FileNotFoundError as fnf_error:
        console.print("[red]Command not found:", fnf_error)
    except Exception as e:
        console.print("An error occurred:", str(e))

if __name__ == "__main__":
    #generate_svg()
    inject_mermaid_diagram()
