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
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name
PYTHON_MULTIPROCESS_LOGO = MY_DIR.parents[1] / "diagrams" / "python_multiprocess_logo.png"
PACKAGES_IMAGE = MY_DIR.parents[1] / "diagrams" / "packages.png"
DATABASE_IMAGE = MY_DIR.parents[1] / "diagrams" / "database.png"
MULTIPLE_FILES_IMAGE = MY_DIR.parents[1] / "diagrams" / "multiple_files.png"
CONFIG_FILE = MY_DIR.parents[1] / "diagrams" / "config_file.png"

console = Console(width=400, color_system="standard")

graph_attr = {
    "concentrate": "false",
    "splines": "curve",
}

edge_attr = {
    "minlen": "2",
}


def diagram_component_deployment_airflow_2():
    diagram_component_file = (MY_DIR / MY_FILENAME).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {diagram_component_file}")
    with Diagram(
        name="Diagram Architecture Deployment Airflow 2",
        show=False,
        direction="TB",
        filename=MY_FILENAME,
        outformat="png",
        graph_attr=graph_attr,
        edge_attr=edge_attr,
    ):
        with Cluster("Database Scope", graph_attr={"bgcolor": "lightgray", "fontsize": "10"}):
            database = Custom("Database", DATABASE_IMAGE.as_posix())
            webserver_and_api = Custom("Webserver\n(&API\n& CLI", PYTHON_MULTIPROCESS_LOGO.as_posix())
            scheduler_and_dag_processor = Custom(
                "Scheduler\b& DAG File Processor\n& CLI", PYTHON_MULTIPROCESS_LOGO.as_posix()
            )
            workers_triggerers = Custom("Workers\n& Triggerers\n& CLI", PYTHON_MULTIPROCESS_LOGO.as_posix())

        database >> Edge(color="red", style="solid") >> webserver_and_api
        database >> Edge(color="red", style="solid") >> scheduler_and_dag_processor
        database >> Edge(color="red", style="solid") >> workers_triggerers

        with Cluster("Provider Package Scope", graph_attr={"bgcolor": "lightblue", "fontsize": "10"}):
            auth_backends = Custom("Auth Backends", PACKAGES_IMAGE.as_posix())
            executors = Custom("Executors", PACKAGES_IMAGE.as_posix())
            ui_plugins_and_api_endpoints = Custom("UI Plugins\n& API Endpoints", PACKAGES_IMAGE.as_posix())
            extra_links = Custom("Extra Links", PACKAGES_IMAGE.as_posix())
            hooks_connections = Custom("Hooks\n& Connections", PACKAGES_IMAGE.as_posix())
            operators_and_decorators = Custom("Operators\n& Decorators", PACKAGES_IMAGE.as_posix())
            secret_backends = Custom("Secret Backends", PACKAGES_IMAGE.as_posix())
            task_log_handlers = Custom("Task Log Handlers", PACKAGES_IMAGE.as_posix())

        webserver_and_api >> Edge(color="black", style="solid") >> auth_backends
        webserver_and_api >> Edge(color="black", style="solid") >> ui_plugins_and_api_endpoints
        webserver_and_api >> Edge(color="black", style="solid") >> task_log_handlers
        webserver_and_api >> Edge(color="black", style="solid") >> hooks_connections
        webserver_and_api >> Edge(color="black", style="solid") >> secret_backends
        webserver_and_api >> Edge(color="black", style="solid") >> extra_links
        scheduler_and_dag_processor >> Edge(color="black", style="solid") >> executors
        scheduler_and_dag_processor >> Edge(color="black", style="solid") >> task_log_handlers
        scheduler_and_dag_processor >> Edge(color="black", style="solid") >> hooks_connections
        scheduler_and_dag_processor >> Edge(color="black", style="solid") >> operators_and_decorators
        workers_triggerers >> Edge(color="black", style="solid") >> task_log_handlers
        workers_triggerers >> Edge(color="black", style="solid") >> hooks_connections
        workers_triggerers >> Edge(color="black", style="solid") >> secret_backends
        operators_and_decorators >> Edge(color="black", style="solid") >> extra_links

    console.print(f"[green]Generating architecture image {diagram_component_file}")


if __name__ == "__main__":
    diagram_component_deployment_airflow_2()
