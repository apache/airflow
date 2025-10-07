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
# requires-python = ">=3.10"
# dependencies = [
#    "rich>=13.6.0",
#    "diagrams>=0.23.4",
# ]
# ///
from __future__ import annotations

from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom
from diagrams.onprem.client import User
from diagrams.programming.language import Python
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name
AIRFLOW_SOURCES_ROOT = MY_DIR.parents[2]
DIAGRAMS_DIR = AIRFLOW_SOURCES_ROOT / "devel-common" / "src" / "docs" / "diagrams"
PYTHON_MULTIPROCESS_LOGO = DIAGRAMS_DIR / "python_multiprocess_logo.png"
PACKAGES_IMAGE = DIAGRAMS_DIR / "packages.png"
DATABASE_IMAGE = DIAGRAMS_DIR / "database.png"
MULTIPLE_FILES_IMAGE = DIAGRAMS_DIR / "multiple_files.png"
CONFIG_FILE = DIAGRAMS_DIR / "config_file.png"

console = Console(width=400, color_system="standard")

graph_attr = {
    "concentrate": "false",
    "splines": "splines",
}

edge_attr = {
    "minlen": "2",
}


def generate_basic_airflow_diagram():
    image_file = (MY_DIR / MY_FILENAME).with_suffix(".png")

    console.print(f"[bright_blue]Generating architecture image {image_file}")
    with Diagram(
        name="",
        show=False,
        direction="LR",
        filename=MY_FILENAME,
        outformat="png",
        graph_attr=graph_attr,
        edge_attr=edge_attr,
    ):
        user = User("Airflow User")

        dag_files = Custom("DAG files", MULTIPLE_FILES_IMAGE.as_posix())
        user >> Edge(color="brown", style="solid", reverse=False, label="author\n\n") >> dag_files

        with Cluster("Parsing, Scheduling & Executing"):
            scheduler = Python("Scheduler")

        metadata_db = Custom("Metadata DB", DATABASE_IMAGE.as_posix())
        scheduler >> Edge(color="red", style="dotted", reverse=True) >> metadata_db

        plugins_and_packages = Custom(
            "Plugin folder\n& installed packages", PACKAGES_IMAGE.as_posix(), color="transparent"
        )

        user >> Edge(color="blue", style="solid", reverse=False, label="install\n\n") >> plugins_and_packages

        with Cluster("UI"):
            webserver = Python("API Server")

        webserver >> Edge(color="black", style="solid", reverse=True, label="operate\n\n") >> user

        metadata_db >> Edge(color="red", style="dotted", reverse=True) >> webserver

        dag_files >> Edge(color="brown", style="solid", label="read\n\n") >> scheduler

        plugins_and_packages >> Edge(color="blue", style="solid", label="install\n\n") >> scheduler
        plugins_and_packages >> Edge(color="blue", style="solid", label="install\n\n") >> webserver

    console.print(f"[green]Generating architecture image {image_file}")


if __name__ == "__main__":
    generate_basic_airflow_diagram()
