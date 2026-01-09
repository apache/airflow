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
    "splines": "line",
}

edge_attr = {
    "minlen": "2",
}


def generate_dag_processor_airflow_diagram():
    dag_processor_architecture_image_file = (MY_DIR / MY_FILENAME).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {dag_processor_architecture_image_file}")
    with Diagram(
        name="",
        show=False,
        direction="LR",
        filename=MY_FILENAME,
        outformat="png",
        graph_attr=graph_attr,
        edge_attr=edge_attr,
    ):
        operations_user = User("Operations User")
        deployment_manager = User("Deployment Manager")

        with Cluster("Security perimeter with no DAG code execution", graph_attr={"bgcolor": "lightgrey"}):
            with Cluster("Scheduling\n\n"):
                schedulers = Custom("Scheduler(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

            with Cluster("UI"):
                webservers = Custom("API Server(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

        webservers >> Edge(color="black", style="solid", reverse=True, label="operate\n\n") >> operations_user

        metadata_db = Custom("Metadata DB", DATABASE_IMAGE.as_posix())

        dag_author = User("DAG Author")

        with Cluster("Security perimeter with DAG code execution"):
            with Cluster("Execution"):
                workers = Custom("Worker(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
                triggerer = Custom("Triggerer(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
            with Cluster("Parsing"):
                dag_processors = Custom("DAG\nProcessor(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
            dag_files = Custom("DAG files", MULTIPLE_FILES_IMAGE.as_posix())

        plugins_and_packages = Custom("Plugin folder\n& installed packages", PACKAGES_IMAGE.as_posix())

        dag_author >> Edge(color="brown", style="dashed", reverse=False, label="author\n\n") >> dag_files
        (
            deployment_manager
            >> Edge(color="blue", style="solid", reverse=False, label="install\n\n")
            >> plugins_and_packages
        )

        workers - Edge(color="black", style="dashed", headlabel="[Executor]") - schedulers

        plugins_and_packages >> Edge(color="blue", style="solid", label="install") >> workers
        plugins_and_packages >> Edge(color="blue", style="solid", label="install") >> dag_processors
        plugins_and_packages >> Edge(color="blue", style="solid", label="install") >> triggerer

        plugins_and_packages >> Edge(color="blue", style="solid", label="install") >> schedulers
        plugins_and_packages >> Edge(color="blue", style="solid", label="install") >> webservers

        metadata_db >> Edge(color="red", style="dotted", reverse=True) >> webservers
        metadata_db >> Edge(color="red", style="dotted", reverse=True) >> schedulers
        dag_processors >> Edge(color="red", style="dotted", reverse=True) >> metadata_db
        workers >> Edge(color="red", style="dotted", reverse=True) >> metadata_db
        triggerer >> Edge(color="red", style="dotted", reverse=True) >> metadata_db

        dag_files >> Edge(color="brown", style="solid", label="sync\n\n") >> workers
        dag_files >> Edge(color="brown", style="solid", label="sync\n\n") >> dag_processors
        dag_files >> Edge(color="brown", style="solid", label="sync\n\n") >> triggerer
    console.print(f"[green]Generating architecture image {dag_processor_architecture_image_file}")


if __name__ == "__main__":
    generate_dag_processor_airflow_diagram()
