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
from diagrams.onprem.client import User
from diagrams.onprem.compute import Server
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

graph_attr = {
    "concentrate": "false",
    "splines": "splines",
}

edge_attr = {
    "minlen": "1",
}


def generate_airflowctl_api_network_diagram():
    image_file = (MY_DIR / MY_FILENAME).with_suffix(".png")

    console.print(f"[bright_blue]Generating network diagram {image_file}")
    with Diagram(
        name="airflowctl<->API Network Diagram",
        show=False,
        direction="LR",
        filename=MY_FILENAME,
        edge_attr=edge_attr,
        graph_attr=graph_attr,
    ):
        # Machine network with client
        with Cluster("Machine Network", graph_attr={"margin": "30", "width": "10"}):
            client = User("Client\n(The machine/host has the airflowctl installed)")

        # Airflow deployment network with API server
        with Cluster("Apache Airflow Deployment Network", graph_attr={"margin": "30"}):
            api_server = Server("Apache Airflow API Server\n(e.g. DNS: https://airflow.internal.api.com)")

        # Edges representing the flows
        (
            client
            >> Edge(
                color="blue",
                style="solid",
                label="Login Request\n(if not manually used in --api-token or env var. Authentication done with username/password)",
            )
            >> api_server
        )

        (
            api_server
            >> Edge(
                color="darkgreen",
                style="solid",
                label="Returns Token",
            )
            >> client
        )

    console.print(f"[green]Generated network diagram {image_file}")


if __name__ == "__main__":
    generate_airflowctl_api_network_diagram()
