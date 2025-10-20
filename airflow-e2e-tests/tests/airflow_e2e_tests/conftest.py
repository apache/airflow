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
    AWS_INIT_PATH,
    DOCKER_COMPOSE_HOST_PORT,
    DOCKER_COMPOSE_PATH,
    DOCKER_IMAGE,
    E2E_DAGS_FOLDER,
    E2E_TEST_MODE,
    LOCALSTACK_PATH,
    LOGS_FOLDER,
    TEST_REPORT_FILE,
)

console = Console(width=400, color_system="standard")
compose_instance = None
airflow_logs_path = None


def _setup_s3_integration(dot_env_file, tmp_dir):
    copyfile(LOCALSTACK_PATH, tmp_dir / "localstack.yml")

    copyfile(AWS_INIT_PATH, tmp_dir / "init-aws.sh")
    current_permissions = os.stat(tmp_dir / "init-aws.sh").st_mode
    os.chmod(tmp_dir / "init-aws.sh", current_permissions | 0o111)

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        "AWS_DEFAULT_REGION=us-east-1\n"
        "AWS_ENDPOINT_URL_S3=http://localstack:4566\n"
        "AIRFLOW__LOGGING__REMOTE_LOGGING=true\n"
        "AIRFLOW_CONN_AWS_S3_LOGS=aws://test:test@\n"
        "AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_s3_logs\n"
        "AIRFLOW__LOGGING__DELETE_LOCAL_LOGS=true\n"
        "AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://test-airflow-logs\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


def spin_up_airflow_environment(tmp_path_factory):
    global compose_instance
    global airflow_logs_path
    tmp_dir = tmp_path_factory.mktemp("airflow-e2e-tests")

    console.print(f"[yellow]Using docker compose file: {DOCKER_COMPOSE_PATH}")
    copyfile(DOCKER_COMPOSE_PATH, tmp_dir / "docker-compose.yaml")

    subfolders = ("dags", "logs", "plugins", "config")

    console.print(f"[yellow]Creating subfolders:[/ {subfolders}")

    for subdir in subfolders:
        (tmp_dir / subdir).mkdir()

    airflow_logs_path = tmp_dir / "logs"

    console.print(f"[yellow]Copying dags to:[/ {tmp_dir / 'dags'}")
    copytree(E2E_DAGS_FOLDER, tmp_dir / "dags", dirs_exist_ok=True)

    dot_env_file = tmp_dir / ".env"
    dot_env_file.write_text(f"AIRFLOW_UID={os.getuid()}\n")

    console.print(f"[yellow]Creating .env file :[/ {dot_env_file}")

    os.environ["AIRFLOW_IMAGE_NAME"] = DOCKER_IMAGE
    compose_file_names = ["docker-compose.yaml"]

    if E2E_TEST_MODE == "remote_log":
        compose_file_names.append("localstack.yml")
        _setup_s3_integration(dot_env_file, tmp_dir)

    # If we are using the image from ghcr.io/apache/airflow/main we do not pull
    # as it is already available and loaded using prepare_breeze_and_image step in workflow
    pull = False if DOCKER_IMAGE.startswith("ghcr.io/apache/airflow/main/") else True

    try:
        console.print(f"[blue]Spinning up airflow environment using {DOCKER_IMAGE}")
        compose_instance = DockerCompose(tmp_dir, compose_file_name=compose_file_names, pull=pull)

        compose_instance.start()

        compose_instance.wait_for(f"http://{DOCKER_COMPOSE_HOST_PORT}/api/v2/version")
        compose_instance.exec_in_container(
            command=["airflow", "dags", "reserialize"], service_name="airflow-dag-processor"
        )

    except Exception:
        console.print("[red]Failed to start docker compose")
        _print_logs(compose_instance)
        compose_instance.stop()
        raise


def _print_logs(compose_instance):
    containers = compose_instance.get_containers()
    for container in containers:
        service = container.Service
        stdout, _ = compose_instance.get_logs(service)
        console.print(f"::group:: {service} Logs")
        console.print(f"[red]{stdout}")
        console.print("::endgroup::")


def pytest_sessionstart(session):
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

    # If any test failures lets print the services logs
    if any(r["status"] == "failed" for r in test_results):
        _print_logs(compose_instance=compose_instance)

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
