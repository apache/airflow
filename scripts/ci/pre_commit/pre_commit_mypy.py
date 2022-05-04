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
import os
import subprocess
import sys
from pathlib import Path

from rich import print

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )


AIRFLOW_SOURCES = Path(__file__).parents[3].absolute()
GITHUB_REPOSITORY = os.environ.get('GITHUB_REPOSITORY', "apache/airflow")

if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).parents[3].absolute() / "dev" / "breeze" / "src"))
    from airflow_breeze.branch_defaults import AIRFLOW_BRANCH

    AIRFLOW_CI_IMAGE = f"ghcr.io/{GITHUB_REPOSITORY}/{AIRFLOW_BRANCH}/ci/python3.7"

    if subprocess.call(args=["docker", "inspect", AIRFLOW_CI_IMAGE], stdout=subprocess.DEVNULL) != 0:
        print(f'[red]The image {AIRFLOW_CI_IMAGE} is not available.[/]\n')
        print("\n[yellow]Please run at the earliest convenience:[/]\n\nbreeze build-image --python 3.7\n\n")
        sys.exit(1)
    return_code = subprocess.call(
        args=[
            "docker",
            "run",
            "-t",
            "-v",
            f"{AIRFLOW_SOURCES}:/opt/airflow/",
            "-e",
            "SKIP_ENVIRONMENT_INITIALIZATION=true",
            "-e",
            "PRINT_INFO_FROM_SCRIPTS=false",
            "-e",
            "BACKEND=sqlite",
            "--pull",
            "never",
            AIRFLOW_CI_IMAGE,
            "/opt/airflow/scripts/in_container/run_mypy.sh",
            *sys.argv[1:],
        ],
    )
    sys.exit(return_code)
