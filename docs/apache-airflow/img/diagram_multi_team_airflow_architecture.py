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
from diagrams.onprem.identity import Dex
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
    "splines": "line",
}

edge_attr = {
    "minlen": "2",
}


def generate_dag_processor_airflow_diagram():
    dag_processor_architecture_image_file = (MY_DIR / MY_FILENAME).with_suffix(".png")
    console.print(
        f"[bright_blue]Generating architecture image {dag_processor_architecture_image_file}"
    )
    with Diagram(
        name="",
        show=False,
        direction="LR",
        filename=MY_FILENAME,
        outformat="png",
        graph_attr=graph_attr,
        edge_attr=edge_attr,
    ):
        with Cluster(
            "Common Organization Airflow Deployment",
            graph_attr={"bgcolor": "lightgrey", "fontsize": "22"},
        ):
            with Cluster("Scheduling\n\n"):
                executor_1 = Custom(
                    "Executor\nTeam 1", PYTHON_MULTIPROCESS_LOGO.as_posix()
                )
                executor_2 = Custom(
                    "Executor\nTeam 2", PYTHON_MULTIPROCESS_LOGO.as_posix()
                )
                schedulers = Custom("Scheduler(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
                (
                    executor_1
                    - Edge(color="black", style="dashed", reverse=True)
                    - schedulers
                )
                (
                    executor_2
                    - Edge(color="black", style="dashed", reverse=True)
                    - schedulers
                )

            with Cluster(
                "Organization DB", graph_attr={"bgcolor": "#D0BBCC", "fontsize": "22"}
            ):
                metadata_db = Custom("Metadata DB", DATABASE_IMAGE.as_posix())

            with Cluster("UI"):
                webservers = Custom("Webserver(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
                auth_manager = Custom(
                    "Auth\nManager", PYTHON_MULTIPROCESS_LOGO.as_posix()
                )

            organization_plugins_and_packages = Custom(
                "Common\nOrganization\nPlugins &\nPackages", PACKAGES_IMAGE.as_posix()
            )

            organization_config_file = Custom(
                "Config\nFile\nCommon\nOrganization", CONFIG_FILE.as_posix()
            )

            internal_api = Custom(
                "Task SDK\nGRPC API", PYTHON_MULTIPROCESS_LOGO.as_posix()
            )
            (
                internal_api
                >> Edge(
                    color="red", style="dotted", reverse=True, label="DB Access\n\n\n"
                )
                >> metadata_db
            )

        organization_deployment_manager = User("Deployment Manager\nOrganization")

        with Cluster(
            "Organization UI access Management",
            graph_attr={"bgcolor": "lightgrey", "fontsize": "22"},
        ):
            organization_admin = User("Organization Admin")
            external_organization_identity_system = Dex("Identity System")
            external_organization_identity_system >> auth_manager

        auth_manager >> Edge(color="black", style="solid", reverse=True) >> webservers

        (
            auth_manager
            >> Edge(color="black", style="solid", reverse=True, label="manages\n\n")
            >> organization_admin
        )

        deployment_manager_1 = User("Deployment\nManager\nTeam 1")
        dag_author_1 = User("DAG Author\nTeamt 1")

        with Cluster(
            "Team 1 Airflow Deployment",
            graph_attr={"bgcolor": "#AAAABB", "fontsize": "22"},
        ):
            with Cluster("No DB access"):
                with Cluster("Execution"):
                    workers_1 = Custom("Worker(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
                    triggerer_1 = Custom(
                        "Triggerer(s)", PYTHON_MULTIPROCESS_LOGO.as_posix()
                    )
                with Cluster("Parsing"):
                    dag_processors_1 = Custom(
                        "DAG\nProcessor(s)", PYTHON_MULTIPROCESS_LOGO.as_posix()
                    )
                dag_files_1 = Custom("DAGS/Team 1", MULTIPLE_FILES_IMAGE.as_posix())
                plugins_and_packages_1 = Custom(
                    "Plugins\n& Packages\nTenant 1", PACKAGES_IMAGE.as_posix()
                )
                config_file_1 = Custom("Config\nFile\nTeam 1", CONFIG_FILE.as_posix())
        operations_user_1 = User("Operations User\nTeam 1")

        deployment_manager_2 = User("Deployment\nManager\nTeam 2")
        dag_author_2 = User("DAG Author\nTeam 2")

        with Cluster("Team 2 Airflow Deployment", graph_attr={"fontsize": "22"}):
            with Cluster("No DB access"):
                with Cluster("Execution"):
                    workers_2 = Custom("Worker(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
                    triggerer_2 = Custom(
                        "Triggerer(s)", PYTHON_MULTIPROCESS_LOGO.as_posix()
                    )
                with Cluster("Parsing"):
                    dag_processors_2 = Custom(
                        "DAG\nProcessor(s)", PYTHON_MULTIPROCESS_LOGO.as_posix()
                    )
                dag_files_2 = Custom("DAGS/Team 2", MULTIPLE_FILES_IMAGE.as_posix())
                plugins_and_packages_2 = Custom(
                    "Plugins\n& Packages\nTeam 2", PACKAGES_IMAGE.as_posix()
                )
                config_file_2 = Custom("Config\nFile\nTeam 2", CONFIG_FILE.as_posix())
        operations_user_2 = User("Operations User\nTeam 2")

        (
            operations_user_1
            >> Edge(
                color="black",
                style="solid",
                reverse=True,
                label="operates\nTeam 1 Only\n\n",
            )
            >> auth_manager
        )
        (
            operations_user_2
            >> Edge(
                color="black",
                style="solid",
                reverse=True,
                label="operates\nTeam 2 Only\n\n",
            )
            >> auth_manager
        )

        workers_1 - Edge(color="black", style="dashed", reverse=True) - executor_1
        workers_2 - Edge(color="black", style="dashed", reverse=True) - executor_2

        (
            dag_author_1
            >> Edge(color="brown", style="dashed", reverse=False, label="author\n\n")
            >> dag_files_1
        )
        (
            deployment_manager_1
            >> Edge(color="blue", style="dashed", reverse=False, label="install\n\n")
            >> plugins_and_packages_1
        )

        (
            deployment_manager_1
            >> Edge(color="blue", style="dashed", reverse=False, label="configure\n\n")
            >> config_file_1
        )

        (
            dag_author_2
            >> Edge(color="brown", style="dashed", reverse=False, label="author\n\n")
            >> dag_files_2
        )
        (
            deployment_manager_2
            >> Edge(color="blue", style="solid", reverse=False, label="install\n\n")
            >> plugins_and_packages_2
        )

        (
            deployment_manager_2
            >> Edge(color="blue", style="solid", reverse=False, label="configure\n\n")
            >> config_file_2
        )

        (
            organization_plugins_and_packages
            - Edge(color="blue", style="solid", reverse=True, label="install\n\n")
            - organization_deployment_manager
        )

        (
            organization_config_file
            - Edge(color="blue", style="solid", reverse=True, label="configure\n\n")
            - organization_deployment_manager
        )

        plugins_and_packages_1 >> Edge(style="invis") >> workers_1
        plugins_and_packages_1 >> Edge(style="invis") >> dag_processors_1
        plugins_and_packages_1 >> Edge(style="invis") >> triggerer_1

        plugins_and_packages_2 >> Edge(style="invis") >> workers_2
        plugins_and_packages_2 >> Edge(style="invis") >> dag_processors_2
        plugins_and_packages_2 >> Edge(style="invis") >> triggerer_2

        (
            metadata_db
            >> Edge(color="red", style="dotted", reverse=True, label="DB Access")
            >> webservers
        )
        (
            metadata_db
            >> Edge(color="red", style="dotted", reverse=True, label="DB Access")
            >> schedulers
        )

        (
            dag_processors_1
            >> Edge(color="red", style="dotted", reverse=True, label="GRPC\nHTTPS\n\n")
            >> internal_api
        )
        (
            workers_1
            >> Edge(color="red", style="dotted", reverse=True, label="GRPC\nHTTPS\n\n")
            >> internal_api
        )
        (
            triggerer_1
            >> Edge(color="red", style="dotted", reverse=True, label="GRPC\nHTTPS\n\n")
            >> internal_api
        )

        (
            dag_processors_2
            >> Edge(color="red", style="dotted", reverse=True, label="GRPC\nHTTPS\n\n")
            >> internal_api
        )
        (
            workers_2
            >> Edge(color="red", style="dotted", reverse=True, label="GRPC\nHTTPS\n\n")
            >> internal_api
        )
        (
            triggerer_2
            >> Edge(color="red", style="dotted", reverse=True, label="GRPC\nHTTPS\n\n")
            >> internal_api
        )

        dag_files_1 >> Edge(color="brown", style="solid", label="sync\n\n") >> workers_1
        (
            dag_files_1
            >> Edge(color="brown", style="solid", label="sync\n\n")
            >> dag_processors_1
        )
        dag_files_1 >> Edge(color="brown", style="solid", label="sync\n\n") >> triggerer_1

        dag_files_2 >> Edge(color="brown", style="solid", label="sync\n\n") >> workers_2
        (
            dag_files_2
            >> Edge(color="brown", style="solid", label="sync\n\n")
            >> dag_processors_2
        )
        dag_files_2 >> Edge(color="brown", style="solid", label="sync\n\n") >> triggerer_2

        # This is for better layout. Invisible edges are used to align the nodes better
        schedulers - Edge(style="invis") - organization_config_file
        schedulers - Edge(style="invis") - organization_plugins_and_packages
        metadata_db - Edge(style="invis") - executor_1
        metadata_db - Edge(style="invis") - executor_2
        workers_1 - Edge(style="invis") - operations_user_1
        workers_2 - Edge(style="invis") - operations_user_2

        external_organization_identity_system - Edge(style="invis") - organization_admin

    console.print(
        f"[green]Generating architecture image {dag_processor_architecture_image_file}"
    )


if __name__ == "__main__":
    generate_dag_processor_airflow_diagram()
