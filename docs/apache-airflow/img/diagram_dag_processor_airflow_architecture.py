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
from __future__ import annotations

from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom
from diagrams.onprem.client import User
from diagrams.onprem.database import PostgreSQL
from diagrams.programming.flowchart import MultipleDocuments
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name
PYTHON_MULTIPROCESS_LOGO = MY_DIR.parents[1] / "diagrams" / "python_multiprocess_logo.png"

console = Console(width=400, color_system="standard")


def generate_dag_processor_airflow_diagram():
    dag_processor_architecture_image_file = (MY_DIR / MY_FILENAME).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {dag_processor_architecture_image_file}")
    with Diagram(
        name="",
        show=False,
        direction="LR",
        curvestyle="ortho",
        filename=MY_FILENAME,
        outformat="png",
    ):
        operations_user = User("Operations User")
        with Cluster("No DAG Python Code Execution", graph_attr={"bgcolor": "lightgrey"}):
            with Cluster("Scheduling"):
                schedulers = Custom("Scheduler(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

            with Cluster("UI"):
                webservers = Custom("Webserver(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

        webservers >> Edge(color="black", style="dashed", reverse=True) >> operations_user

        metadata_db = PostgreSQL("Metadata DB")

        dag_author = User("DAG Author")
        with Cluster("DAG Python Code Execution"):
            with Cluster("Execution"):
                workers = Custom("Worker(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
                triggerer = Custom("Triggerer(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
            with Cluster("Parsing"):
                dag_processors = Custom("DAG\nProcessor(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
            dag_files = MultipleDocuments("DAG files")

        dag_author >> Edge(color="black", style="dashed", reverse=False) >> dag_files

        workers - Edge(color="blue", style="dashed", headlabel="Executor") - schedulers

        metadata_db >> Edge(color="red", style="dotted", reverse=True) >> webservers
        metadata_db >> Edge(color="red", style="dotted", reverse=True) >> schedulers
        dag_processors >> Edge(color="red", style="dotted", reverse=True) >> metadata_db
        workers >> Edge(color="red", style="dotted", reverse=True) >> metadata_db
        triggerer >> Edge(color="red", style="dotted", reverse=True) >> metadata_db

        dag_files >> Edge(color="brown", style="solid") >> workers
        dag_files >> Edge(color="brown", style="solid") >> dag_processors
        dag_files >> Edge(color="brown", style="solid") >> triggerer
    console.print(f"[green]Generating architecture image {dag_processor_architecture_image_file}")


if __name__ == "__main__":
    generate_dag_processor_airflow_diagram()
