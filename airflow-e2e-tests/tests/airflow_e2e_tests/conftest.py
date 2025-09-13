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

import json
import os
from datetime import datetime
from shutil import copyfile, copytree

import pytest
from rich.console import Console
from testcontainers.compose import DockerCompose

from airflow_e2e_tests.constants import (
    AIRFLOW_ROOT_PATH,
    DOCKER_COMPOSE_HOST_PORT,
    DOCKER_IMAGE,
    E2E_DAGS_FOLDER,
    LOGS_FOLDER,
    TEST_REPORT_FILE,
)

console = Console(width=400, color_system="standard")
compose_instance = None
airflow_logs_path = None


def spin_up_airflow_environment(tmp_path_factory):
    global compose_instance
    global airflow_logs_path
    tmp_dir = tmp_path_factory.mktemp("airflow-e2e-tests")

    compose_file_path = (
        AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "howto" / "docker-compose" / "docker-compose.yaml"
    )

    copyfile(compose_file_path, tmp_dir / "docker-compose.yaml")

    subfolders = ("dags", "logs", "plugins", "config")

    console.print(f"[yellow]Creating subfolders:[/ {subfolders}")

    for subdir in subfolders:
        (tmp_dir / subdir).mkdir()

    airflow_logs_path = tmp_dir / "logs"

    console.print(f"[yellow]Copying dags to:[/ {tmp_dir / 'dags'}")
    copytree(E2E_DAGS_FOLDER, tmp_dir / "dags", dirs_exist_ok=True)

    dot_env_file = tmp_dir / ".env"

    console.print(f"[yellow]Creating .env file :[/ {dot_env_file}")

    dot_env_file.write_text(f"AIRFLOW_UID={os.getuid()}\n")
    os.environ["AIRFLOW_IMAGE_NAME"] = DOCKER_IMAGE

    # If we are using the image from ghcr.io/apache/airflow/main we do not pull
    # as it is already available and loaded using prepare_breeze_and_image step in workflow
    pull = False if DOCKER_IMAGE.startswith("ghcr.io/apache/airflow/main/") else True

    compose_instance = DockerCompose(tmp_dir, compose_file_name=["docker-compose.yaml"], pull=pull)

    compose_instance.start()

    compose_instance.wait_for(f"http://{DOCKER_COMPOSE_HOST_PORT}/api/v2/version")
    compose_instance.exec_in_container(
        command=["airflow", "dags", "reserialize"], service_name="airflow-dag-processor"
    )


def pytest_sessionstart(session):
    console.print("[blue]Spinning airflow environment...")

    tmp_path_factory = session.config._tmp_path_factory
    spin_up_airflow_environment(tmp_path_factory)

    console.print("[green]Airflow environment is up and running!")


test_results = []


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Capture test results."""
    output = yield
    report = output.get_result()

    if report.when == "call":
        test_result = {
            "test_name": item.name,
            "test_class": item.cls.__name__ if item.cls else "",
            "status": report.outcome,
            "duration": report.duration,
            "error": str(report.longrepr) if report.failed else None,
            "timestamp": datetime.now().isoformat(),
        }
        test_results.append(test_result)


def pytest_sessionfinish(session, exitstatus):
    """Generate report after all tests complete."""
    generate_test_report(test_results)
    if airflow_logs_path is not None:
        copytree(airflow_logs_path, LOGS_FOLDER, dirs_exist_ok=True)

    if compose_instance:
        if not os.environ.get("SKIP_DOCKER_COMPOSE_DELETION"):
            compose_instance.stop()


def generate_test_report(results):
    """Generate test report with json summary."""
    report = {
        "summary": {
            "total_tests": len(results),
            "passed": len([r for r in results if r["status"] == "passed"]),
            "failed": len([r for r in results if r["status"] == "failed"]),
            "execution_time": sum(r["duration"] for r in results),
        },
        "test_results": results,
    }

    with open(TEST_REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2)

    console.print(f"[blue]\n{'=' * 50}")
    console.print("[blue]TEST EXECUTION SUMMARY")
    console.print(f"[blue]{'=' * 50}")
    console.print(f"[blue]Total Tests: {report['summary']['total_tests']}")
    console.print(f"[blue]Passed: {report['summary']['passed']}")
    console.print(f"[red]Failed: {report['summary']['failed']}")
    console.print(f"[blue]Execution Time: {report['summary']['execution_time']:.2f}s")
    console.print("[blue]Reports generated: test_report.json")
