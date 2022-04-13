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
import shutil
import subprocess
import sys
from pathlib import Path


def clean_up_airflow_sources(airflow_sources: Path):
    if airflow_sources.exists():
        print(f"Removing {airflow_sources}")
        shutil.rmtree(airflow_sources, ignore_errors=True)


def check_if_in_virtualenv() -> bool:
    return hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)


def pip_install_requirements() -> int:
    """
    install the requirements of the current python version.
    return 0 if success, anything else is an error.
    """
    version = get_python_version()
    constraint = (
        f"https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-{version}.txt"
    )
    pip_install_command = ["pip", "install", "-e", ".[devel-all]", "--constraint", constraint]
    e = subprocess.run(pip_install_command)
    return e.returncode


def get_python_version() -> str:
    """
    return the version of python we are running.
    """
    major = sys.version_info[0]
    minor = sys.version_info[1]
    return f"{major}.{minor}"


def main():
    """
    Setup local virtual environment.
    """
    airflow_home_dir = os.environ.get("AIRFLOW_HOME", Path.home() / "airflow")  # TODO: change to airflow
    airflow_sources = str(Path.cwd())
    if not check_if_in_virtualenv():
        print(
            "Local virtual environment not activated.\nPlease create and activate it "
            "first. (for example using 'python3 -m venv venv && source venv/bin/activate')"
        )
        return

    print("Initializing environment...")
    print(f"This will remove the folder {airflow_home_dir} and reset all the databases!")
    response = input("Are you sure? (y/N/q)")
    if response != "y":
        return

    print(f"\nWiping and recreating {airflow_home_dir}")

    if not airflow_home_dir == airflow_sources:
        print("AIRFLOW_HOME and Source code are in the same path")
        print(
            f"When running this script it will delete all files in path {airflow_home_dir} "
            "to clear dynamic files like config/logs/db"
        )
        print("Please move the airflow source code elsewhere to avoid deletion")
        return

    clean_up_airflow_sources(airflow_home_dir)

    print("\nInstalling requirements...")
    return_code = pip_install_requirements()

    if return_code != 0:
        print("Error installing the requirements")
        print("Try running the command below and rerun virtualenv installation\n")
        os_type = sys.platform
        if os_type == "darwin":
            print("brew install sqlite mysql postgresql openssl")
            print("export LDFLAGS=\"-L/usr/local/opt/openssl/lib\"")
            print("export CPPFLAGS=\"-I/usr/local/opt/openssl/include\"")
        else:
            print(
                "sudo apt install build-essential python3-dev libsqlite3-dev openssl"
                "sqlite default-libmysqlclient-dev libmysqlclient-dev postgresql"
            )
        return

    print("\nResetting AIRFLOW sqlite database...")
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_ENABLED"] = "False"
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{airflow_sources}/empty"
    os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = f"{airflow_sources}/empty"
    subprocess.run(["airflow", "db", "reset", "--yes"])

    print("\nResetting AIRFLOW sqlite unit test database...")
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "True"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_ENABLED"] = "False"
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{airflow_sources}/empty"
    os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = f"{airflow_sources}/empty"
    subprocess.run(["airflow", "db", "reset", "--yes"])

    print("\nInitialization of environment complete! Go ahead and develop Airflow!")


if __name__ == "__main__":
    main()
