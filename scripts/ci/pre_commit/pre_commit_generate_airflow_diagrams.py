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
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom
from diagrams.onprem.client import User
from diagrams.onprem.database import PostgreSQL
from diagrams.programming.flowchart import MultipleDocuments
from rich.console import Console

console = Console(width=400, color_system="standard")

LOCAL_DIR = Path(__file__).parent
AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3]
DOCS_IMAGES_DIR = AIRFLOW_SOURCES_ROOT / "docs" / "apache-airflow" / "img"
FAB_PROVIDER_DOCS_IMAGES_DIR = AIRFLOW_SOURCES_ROOT / "docs" / "apache-airflow-providers-fab" / "img"
PYTHON_MULTIPROCESS_LOGO = AIRFLOW_SOURCES_ROOT / "images" / "diagrams" / "python_multiprocess_logo.png"

BASIC_ARCHITECTURE_IMAGE_NAME = "diagram_basic_airflow_architecture"
DAG_PROCESSOR_AIRFLOW_ARCHITECTURE_IMAGE_NAME = "diagram_dag_processor_airflow_architecture"
AUTH_MANAGER_AIRFLOW_ARCHITECTURE_IMAGE_NAME = "diagram_auth_manager_airflow_architecture"
FAB_AUTH_MANAGER_AIRFLOW_ARCHITECTURE_IMAGE_NAME = "diagram_fab_auth_manager_airflow_architecture"
DIAGRAM_HASH_FILE_NAME = "diagram_hash.txt"


def generate_basic_airflow_diagram():
    basic_architecture_image_file = (DOCS_IMAGES_DIR / BASIC_ARCHITECTURE_IMAGE_NAME).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {basic_architecture_image_file}")
    with Diagram(
        name="", show=False, direction="LR", curvestyle="ortho", filename=BASIC_ARCHITECTURE_IMAGE_NAME
    ):
        with Cluster("Parsing & Scheduling"):
            schedulers = Custom("Scheduler(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

        metadata_db = PostgreSQL("Metadata DB")

        dag_author = User("DAG Author")
        dag_files = MultipleDocuments("DAG files")

        dag_author >> Edge(color="black", style="dashed", reverse=False) >> dag_files

        with Cluster("Execution"):
            workers = Custom("Worker(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())
            triggerer = Custom("Triggerer(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

        schedulers - Edge(color="blue", style="dashed", taillabel="Executor") - workers

        schedulers >> Edge(color="red", style="dotted", reverse=True) >> metadata_db
        workers >> Edge(color="red", style="dotted", reverse=True) >> metadata_db
        triggerer >> Edge(color="red", style="dotted", reverse=True) >> metadata_db

        operations_user = User("Operations User")
        with Cluster("UI"):
            webservers = Custom("Webserver(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

        webservers >> Edge(color="black", style="dashed", reverse=True) >> operations_user

        metadata_db >> Edge(color="red", style="dotted", reverse=True) >> webservers

        dag_files >> Edge(color="brown", style="solid") >> workers
        dag_files >> Edge(color="brown", style="solid") >> schedulers
        dag_files >> Edge(color="brown", style="solid") >> triggerer
    console.print(f"[green]Generating architecture image {basic_architecture_image_file}")


def generate_dag_processor_airflow_diagram():
    dag_processor_architecture_image_file = (
        DOCS_IMAGES_DIR / DAG_PROCESSOR_AIRFLOW_ARCHITECTURE_IMAGE_NAME
    ).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {dag_processor_architecture_image_file}")
    with Diagram(
        name="",
        show=False,
        direction="LR",
        curvestyle="ortho",
        filename=DAG_PROCESSOR_AIRFLOW_ARCHITECTURE_IMAGE_NAME,
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


def generate_auth_manager_airflow_diagram():
    auth_manager_architecture_image_file = (
        DOCS_IMAGES_DIR / AUTH_MANAGER_AIRFLOW_ARCHITECTURE_IMAGE_NAME
    ).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {auth_manager_architecture_image_file}")
    with Diagram(
        name="",
        show=False,
        direction="LR",
        curvestyle="ortho",
        filename=AUTH_MANAGER_AIRFLOW_ARCHITECTURE_IMAGE_NAME,
    ):
        user = User("User")
        with Cluster("Airflow environment"):
            webserver = Custom("Webserver(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

            with Cluster("Provider X"):
                auth_manager = Custom("X auth manager", PYTHON_MULTIPROCESS_LOGO.as_posix())
            with Cluster("Core Airflow"):
                auth_manager_interface = Custom(
                    "Auth manager\ninterface", PYTHON_MULTIPROCESS_LOGO.as_posix()
                )

        (user >> Edge(color="black", style="solid", reverse=True, label="Access to the console") >> webserver)

        (
            webserver
            >> Edge(color="black", style="solid", reverse=True, label="Is user authorized?")
            >> auth_manager
        )

        (
            auth_manager
            >> Edge(color="black", style="dotted", reverse=False, label="Inherit")
            >> auth_manager_interface
        )

    console.print(f"[green]Generating architecture image {auth_manager_architecture_image_file}")


def generate_fab_auth_manager_airflow_diagram():
    auth_manager_architecture_image_file = (
        FAB_PROVIDER_DOCS_IMAGES_DIR / FAB_AUTH_MANAGER_AIRFLOW_ARCHITECTURE_IMAGE_NAME
    ).with_suffix(".png")
    console.print(f"[bright_blue]Generating architecture image {auth_manager_architecture_image_file}")
    with Diagram(
        name="",
        show=False,
        direction="LR",
        curvestyle="ortho",
        filename=FAB_AUTH_MANAGER_AIRFLOW_ARCHITECTURE_IMAGE_NAME,
    ):
        user = User("User")
        with Cluster("Airflow environment"):
            webserver = Custom("Webserver(s)", PYTHON_MULTIPROCESS_LOGO.as_posix())

            with Cluster("FAB provider"):
                fab_auth_manager = Custom("FAB auth manager", PYTHON_MULTIPROCESS_LOGO.as_posix())
            with Cluster("Core Airflow"):
                auth_manager_interface = Custom(
                    "Auth manager\ninterface", PYTHON_MULTIPROCESS_LOGO.as_posix()
                )

            db = PostgreSQL("Metadata DB")

        user >> Edge(color="black", style="solid", reverse=True, label="Access to the console") >> webserver
        (
            webserver
            >> Edge(color="black", style="solid", reverse=True, label="Is user authorized?")
            >> fab_auth_manager
        )
        (fab_auth_manager >> Edge(color="black", style="solid", reverse=True) >> db)
        (
            fab_auth_manager
            >> Edge(color="black", style="dotted", reverse=False, label="Inherit")
            >> auth_manager_interface
        )

    console.print(f"[green]Generating architecture image {auth_manager_architecture_image_file}")


def main():
    hash_md5 = hashlib.md5()
    hash_md5.update(Path(__file__).resolve().read_bytes())
    my_file_hash = hash_md5.hexdigest()
    hash_file = LOCAL_DIR / DIAGRAM_HASH_FILE_NAME
    if not hash_file.exists() or not hash_file.read_text().strip() == str(my_file_hash).strip():
        os.chdir(DOCS_IMAGES_DIR)
        generate_basic_airflow_diagram()
        generate_dag_processor_airflow_diagram()
        generate_auth_manager_airflow_diagram()
        os.chdir(FAB_PROVIDER_DOCS_IMAGES_DIR)
        generate_fab_auth_manager_airflow_diagram()
        os.chdir(DOCS_IMAGES_DIR)
        hash_file.write_text(str(my_file_hash) + "\n")
    else:
        console.print("[bright_blue]No changes to generation script. Not regenerating the images.")


if __name__ == "__main__":
    main()
