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

import os
import subprocess
import sys

import pytest

from airflowctl_tests.constants import AIRFLOW_ROOT_PATH


def pytest_sessionstart(session):
    """Install airflowctl at the very start of the pytest session."""
    from rich.console import Console

    console = Console(width=400, color_system="standard")

    airflow_ctl_version = os.environ.get("AIRFLOW_CTL_VERSION", "1.0.0")
    console.print(f"[yellow]Installing apache-airflow-ctl=={airflow_ctl_version} via pytest_sessionstart...")

    airflow_ctl_path = AIRFLOW_ROOT_PATH / "airflow-ctl"
    console.print(f"[blue]Installing from: {airflow_ctl_path}")

    # Install directly to current UV environment
    console.print("[blue]Installing to current UV environment...")
    console.print(f"[blue]Current Python: {sys.executable}")

    try:
        cmd = ["uv", "pip", "install", str(airflow_ctl_path)]
        console.print(f"[cyan]Running command: {' '.join(cmd)}")
        subprocess.check_call(cmd)
        console.print("[green]airflowctl installed successfully to UV environment via pytest_sessionstart!")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        console.print(f"[yellow]UV installation failed: {e}")
        raise

    console.print("[yellow]Verifying airflowctl installation via pytest_sessionstart...")
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import airflowctl.api.client; print('✅ airflowctl import successful via pytest_sessionstart')",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        console.print(f"[green]{result.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        console.print("[red]❌ airflowctl import verification failed via pytest_sessionstart:")
        console.print(f"[red]Return code: {e.returncode}")
        console.print(f"[red]Stdout: {e.stdout}")
        console.print(f"[red]Stderr: {e.stderr}")
        raise


@pytest.fixture
def login_command():
    return "auth login --username airflow --password airflow"


@pytest.fixture
def login_output():
    return "Login successful! Welcome to airflowctl!"


@pytest.fixture
def date_param():
    import random
    from datetime import datetime, timedelta

    from dateutil.relativedelta import relativedelta

    # original datetime string
    dt_str = "2025-10-25T00:02:00+00:00"

    # parse to datetime object
    dt = datetime.fromisoformat(dt_str)

    # boundaries
    start = dt - relativedelta(months=1)
    end = dt + relativedelta(months=1)

    # pick random time between start and end
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    random_dt = start + timedelta(seconds=random_seconds)
    return random_dt.isoformat()


@pytest.fixture
def test_commands(login_command, date_param):
    # Define test commands to run with actual running API server
    return [
        login_command,
        "backfill list",
        "config get --section core --option executor",
        "connections --connection-id=test_con --conn-type=mysql --password=TEST_PASS -o json",
        "connections list",
        "connections list -o yaml",
        "connections list -o tabledags list",
        f"dagrun trigger --dag-id=example_bash_operator --logical-date={date_param} --run-after={date_param}",
        "dagrun list --dag-id example_bash_operator --state success --limit=1",
        "jobs list",
        "pools create --name=test_pool --slots=5",
        "pools list",
        "providers list",
        "variables list",
        "variables create --key=test_key --value=test_value",
        "version --remote",
    ]
