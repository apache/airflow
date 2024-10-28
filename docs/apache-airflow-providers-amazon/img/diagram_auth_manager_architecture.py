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
from diagrams.aws.general import Users
from diagrams.aws.security import IAMPermissions
from diagrams.custom import Custom
from diagrams.onprem.client import User
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name
PYTHON_MULTIPROCESS_LOGO = MY_DIR.parents[1] / "diagrams" / "python_multiprocess_logo.png"
IDENTITY_CENTER_LOGO = MY_DIR / "icons" / "idc.png"
AVP_LOGO = MY_DIR / "icons" / "avp.png"

console = Console(width=400, color_system="standard")


def generate_auth_manager_diagram():
    image_file = (MY_DIR / MY_FILENAME).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {image_file}")
    with Diagram(
        show=False,
        filename=MY_FILENAME,
    ):
        user = User("Airflow user")
        admin = User("Admin")
        with Cluster("Airflow environment"):
            webserver = Custom("Webserver(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

            with Cluster("Amazon provider"):
                auth_manager = Custom(
                    "AWS auth manager", PYTHON_MULTIPROCESS_LOGO.as_posix()
                )

        identity_center = Custom(
            "AWS IAM Identity Center", IDENTITY_CENTER_LOGO.as_posix()
        )
        avp = Custom("Amazon Verified Permissions", AVP_LOGO.as_posix())

        user >> Edge(label="Access to the UI/Rest API") >> webserver
        webserver >> auth_manager
        auth_manager >> Edge(label="Authentication") >> identity_center
        auth_manager >> Edge(label="Authorization") >> avp

        Users("Users") >> identity_center
        IAMPermissions("Permissions") >> avp

        identity_center << Edge(label="Manage users/groups") << admin
        avp << Edge(label="Manage permissions") << admin

    console.print(f"[green]Generating architecture image {image_file}")


if __name__ == "__main__":
    generate_auth_manager_diagram()
