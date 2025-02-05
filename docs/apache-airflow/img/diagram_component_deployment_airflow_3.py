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


def diagram_component_deployment_airflow_3():
    diagram_component_file = (MY_DIR / MY_FILENAME).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {diagram_component_file}")
    with Diagram(
        name="Diagram Architecture Deployment Airflow 3",
        show=False,
        direction="TB",
        filename=MY_FILENAME,
        outformat="png",
        graph_attr=graph_attr,
        edge_attr=edge_attr,
    ):
        with Cluster("CLI Package Scope", graph_attr={"bgcolor": "lightgray", "fontsize": "10"}):
            cli = Custom("CLI", PYTHON_MULTIPROCESS_LOGO.as_posix())

        webserver_and_api = Custom("Webserver\n& API", PYTHON_MULTIPROCESS_LOGO.as_posix())
        scheduler = Custom("Scheduler", PYTHON_MULTIPROCESS_LOGO.as_posix())
        dag_processor = Custom("DAG File Processor", PYTHON_MULTIPROCESS_LOGO.as_posix())
        workers_and_triggerers = Custom("Workers&\n& Triggerers", PYTHON_MULTIPROCESS_LOGO.as_posix())

        cli >> Edge(color="black", style="solid") >> webserver_and_api
        cli >> Edge(color="black", style="solid") >> scheduler
        cli >> Edge(color="black", style="solid") >> dag_processor
        cli >> Edge(color="black", style="solid") >> workers_and_triggerers

        with Cluster("Auth Managers Scope", graph_attr={"bgcolor": "lightgreen", "fontsize": "10"}):
            auth_backends = Custom("Auth Backend", PACKAGES_IMAGE.as_posix())

        with Cluster("UI Plugins Scope", graph_attr={"bgcolor": "lightsalmon", "fontsize": "10"}):
            ui_plugins = Custom("UI Plugins", PACKAGES_IMAGE.as_posix())

        with Cluster("Executors Scope", graph_attr={"bgcolor": "lightgoldenrod", "fontsize": "10"}):
            executors = Custom("Executors", PACKAGES_IMAGE.as_posix())

        with Cluster("Log Readers Scope", graph_attr={"bgcolor": "lightcyan", "fontsize": "10"}):
            log_readers = Custom("Log readers", PACKAGES_IMAGE.as_posix())

        with Cluster("Providers Scope", graph_attr={"bgcolor": "linen", "fontsize": "10"}):
            secret_backends = Custom("Secret Backends", PACKAGES_IMAGE.as_posix())
            log_writers = Custom("Task Log Handlers\n(write)", PACKAGES_IMAGE.as_posix())
            extra_links = Custom("Extra Links", PACKAGES_IMAGE.as_posix())
            hooks_connections = Custom("Hooks\n& Connections", PACKAGES_IMAGE.as_posix())
            operators_and_decorators = Custom("Operators\n& Decorators", PACKAGES_IMAGE.as_posix())

        database = Custom("Database", DATABASE_IMAGE.as_posix())

        webserver_and_api >> Edge(color="black", style="solid") >> auth_backends
        webserver_and_api >> Edge(color="black", style="solid") >> ui_plugins
        webserver_and_api >> Edge(color="black", style="solid") >> log_readers

        scheduler >> Edge(color="black", style="solid") >> executors

        workers_and_triggerers >> Edge(color="black", style="solid") >> log_writers
        workers_and_triggerers >> Edge(color="black", style="solid") >> hooks_connections
        workers_and_triggerers >> Edge(color="black", style="solid") >> secret_backends
        workers_and_triggerers >> Edge(color="black", style="solid") >> operators_and_decorators

        dag_processor >> Edge(color="black", style="solid") >> hooks_connections
        dag_processor >> Edge(color="black", style="solid") >> secret_backends
        dag_processor >> Edge(color="black", style="solid") >> operators_and_decorators

        operators_and_decorators >> Edge(color="black", style="solid") >> extra_links

        scheduler >> Edge(color="red", style="solid", reverse=True) >> database
        webserver_and_api >> Edge(color="red", style="solid", reverse=True) >> database

        workers_and_triggerers >> Edge(color="black", style="dashed", reverse=True) >> webserver_and_api
        dag_processor >> Edge(color="black", style="dashed", reverse=True) >> webserver_and_api

    console.print(f"[green]Generating architecture image {diagram_component_file}")


if __name__ == "__main__":
    diagram_component_deployment_airflow_3()
