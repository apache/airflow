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
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name
PYTHON_MULTIPROCESS_LOGO = MY_DIR.parents[1] / "diagrams" / "python_multiprocess_logo.png"

console = Console(width=400, color_system="standard")


def generate_fab_auth_manager_airflow_diagram():
    image_file = (MY_DIR / MY_FILENAME).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {image_file}")
    with Diagram(
        name="",
        show=False,
        direction="LR",
        curvestyle="ortho",
        filename=MY_FILENAME,
    ):
        user = User("User")
        with Cluster("Airflow environment"):
            webserver = Custom("Webserver(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

            with Cluster("FAB provider"):
                fab_auth_manager = Custom(
                    "FAB auth manager", PYTHON_MULTIPROCESS_LOGO.as_posix()
                )
            with Cluster("Core Airflow"):
                auth_manager_interface = Custom(
                    "Auth manager\ninterface", PYTHON_MULTIPROCESS_LOGO.as_posix()
                )

            db = PostgreSQL("Metadata DB")

        (
            user
            >> Edge(
                color="black", style="solid", reverse=True, label="Access to the console"
            )
            >> webserver
        )
        (
            webserver
            >> Edge(
                color="black", style="solid", reverse=True, label="Is user authorized?"
            )
            >> fab_auth_manager
        )
        (fab_auth_manager >> Edge(color="black", style="solid", reverse=True) >> db)
        (
            fab_auth_manager
            >> Edge(color="black", style="dotted", reverse=False, label="Inherit")
            >> auth_manager_interface
        )

    console.print(f"[green]Generating architecture image {image_file}")


if __name__ == "__main__":
    generate_fab_auth_manager_airflow_diagram()
