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
from pathlib import Path
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
    ELASTICSEARCH_PATH,
    LOCALSTACK_PATH,
    LOGS_FOLDER,
    OPENSEARCH_PATH,
    TEST_REPORT_FILE,
    XCOM_BUCKET,
)

from tests_common.test_utils.fernet import generate_fernet_key_string

console = Console(width=400, color_system="standard")


class _E2ETestState:
    compose_instance: DockerCompose | None = None
    airflow_logs_path: Path | None = None


def _copy_localstack_files(tmp_dir):
    """Copy localstack compose file and init script into the temp directory."""
    copyfile(LOCALSTACK_PATH, tmp_dir / "localstack.yml")

    copyfile(AWS_INIT_PATH, tmp_dir / "init-aws.sh")
    current_permissions = os.stat(tmp_dir / "init-aws.sh").st_mode
    os.chmod(tmp_dir / "init-aws.sh", current_permissions | 0o111)


def _copy_elasticsearch_files(tmp_dir):
    """Copy Elasticsearch compose file into the temp directory."""
    copyfile(ELASTICSEARCH_PATH, tmp_dir / "elasticsearch.yml")


def _copy_opensearch_files(tmp_dir):
    """Copy OpenSearch compose file into the temp directory."""
    copyfile(OPENSEARCH_PATH, tmp_dir / "opensearch.yml")


def _setup_s3_integration(dot_env_file, tmp_dir):
    _copy_localstack_files(tmp_dir)

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


def _setup_elasticsearch_integration(dot_env_file, tmp_dir):
    _copy_elasticsearch_files(tmp_dir)

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        "AIRFLOW__LOGGING__REMOTE_LOGGING=true\n"
        "AIRFLOW__ELASTICSEARCH__HOST=http://elasticsearch:9200\n"
        "AIRFLOW__ELASTICSEARCH__WRITE_STDOUT=false\n"
        "AIRFLOW__ELASTICSEARCH__JSON_FORMAT=true\n"
        "AIRFLOW__ELASTICSEARCH__WRITE_TO_ES=true\n"
        "AIRFLOW__ELASTICSEARCH__TARGET_INDEX=airflow-e2e-logs\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


def _setup_opensearch_integration(dot_env_file, tmp_dir):
    _copy_opensearch_files(tmp_dir)

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        "AIRFLOW__LOGGING__REMOTE_LOGGING=true\n"
        "AIRFLOW__OPENSEARCH__HOST=http://opensearch:9200\n"
        "AIRFLOW__OPENSEARCH__PORT=9200\n"
        "AIRFLOW__OPENSEARCH__USERNAME=admin\n"
        "AIRFLOW__OPENSEARCH__PASSWORD=admin\n"
        "AIRFLOW__OPENSEARCH__WRITE_STDOUT=false\n"
        "AIRFLOW__OPENSEARCH__JSON_FORMAT=true\n"
        "AIRFLOW__OPENSEARCH__WRITE_TO_OS=true\n"
        "AIRFLOW__OPENSEARCH__TARGET_INDEX=airflow-e2e-logs\n"
        "AIRFLOW__OPENSEARCH__HOST_FIELD=host\n"
        "AIRFLOW__OPENSEARCH__OFFSET_FIELD=offset\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


def _setup_xcom_object_storage_integration(dot_env_file, tmp_dir):
    _copy_localstack_files(tmp_dir)

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        # XComObjectStorageBackend requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as env vars
        # because `universal-path` uses boto3's native S3 client, which relies on environment variables
        # for authentication rather than parsing credentials from the connection URI
        "AWS_ACCESS_KEY_ID=test\n"
        "AWS_SECRET_ACCESS_KEY=test\n"
        "AWS_DEFAULT_REGION=us-east-1\n"
        "AWS_ENDPOINT_URL_S3=http://localstack:4566\n"
        "AIRFLOW_CONN_AWS_DEFAULT=aws://test:test@\n"
        "AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend\n"
        f"AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=s3://aws_default@{XCOM_BUCKET}/xcom\n"
        "AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=0\n"
        "_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-amazon[s3fs]\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


<<<<<<< HEAD
def spin_up_airflow_environment(tmp_path_factory: pytest.TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("airflow-e2e-tests")
=======
def _download_jdk(dest_dir: Path) -> Path:
    """Download a Linux JDK matching the host/container architecture into dest_dir/openjdk."""
    machine = platform.machine()
    if machine in ("x86_64", "amd64"):
        arch = "x64"
    elif machine in ("aarch64", "arm64"):
        arch = "aarch64"
    else:
        raise RuntimeError(f"Unsupported architecture: {machine}")

    jdk_url = (
        "https://github.com/adoptium/temurin11-binaries/releases/download/"
        f"jdk-11.0.30%2B7/OpenJDK11U-jdk_{arch}_linux_hotspot_11.0.30_7.tar.gz"
    )

    tarball_path = dest_dir / "openjdk-11.tar.gz"
    openjdk_dir = dest_dir / "openjdk"
    openjdk_dir.mkdir(exist_ok=True)

    console.print(f"[yellow]Downloading OpenJDK 11 ({arch}) for containers...")
    result = subprocess.run(
        ["curl", "-fL", "-o", str(tarball_path), jdk_url],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to download OpenJDK: {result.stderr}")

    console.print("[yellow]Extracting OpenJDK...")
    result = subprocess.run(
        ["tar", "-xzf", str(tarball_path), "--strip-components=1", "-C", str(openjdk_dir)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to extract OpenJDK: {result.stderr}")

    tarball_path.unlink()
    console.print("[green]OpenJDK 11 downloaded and extracted successfully")
    return openjdk_dir


def _build_java_sdk_bundles(tmp_dir: Path) -> Path:
    """Build both Java SDK bundles (stub + pure Java) using the shared build script.

    Returns the bundles output directory containing both bundle subdirectories.
    The caller's system Java is used for the Gradle build (not the downloaded Linux JDK).
    """
    build_script = AIRFLOW_ROOT_PATH / "scripts" / "in_container" / "java_sdk_build.sh"
    bundles_dir = tmp_dir / "java-sdk-bundles"

    build_env = {
        **os.environ,
        "JAVA_SDK_SRC_DIR": str(JAVA_SDK_PATH),
        "BUNDLES_OUTPUT_DIR": str(bundles_dir),
        "JAVA_STUB_BUNDLE_NAME": JAVA_STUB_BUNDLE_NAME,
        "JAVA_PURE_BUNDLE_NAME": JAVA_PURE_BUNDLE_NAME,
        "JAVA_STUB_DAG_ID": JAVA_STUB_DAG_ID,
        "JAVA_PURE_DAG_ID": JAVA_PURE_DAG_ID,
        "JAVA_SDK_DAGS_DIR": str(JAVA_SDK_DAGS_FOLDER),
    }

    console.print("[yellow]Building Java SDK bundles via shared build script...")
    result = subprocess.run(
        ["bash", str(build_script)],
        env=build_env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    if result.returncode != 0:
        console.print(f"[red]Java SDK build failed:\n{result.stdout}\n{result.stderr}")
        raise RuntimeError("Failed to build Java SDK bundles")
    console.print(result.stdout)

    return bundles_dir


def _setup_java_sdk_integration(dot_env_file, tmp_dir):
    """Set up Java SDK integration: download JDK, build bundles, write env and compose override."""
    # Download the Linux JDK on the host so containers get it via bind mount
    openjdk_dir = _download_jdk(tmp_dir)

    # Build both bundles (stub + pure Java) using the shared script
    bundles_dir = _build_java_sdk_bundles(tmp_dir)
    stub_bundle_root = bundles_dir / JAVA_STUB_BUNDLE_NAME
    pure_bundle_dir = bundles_dir / JAVA_PURE_BUNDLE_NAME

    # Copy the docker-compose override
    copyfile(JAVA_SDK_COMPOSE_PATH, tmp_dir / "java-sdk.yml")

    # Set host environment variables consumed by docker-compose variable substitution
    os.environ["JAVA_OPENJDK_PATH"] = str(openjdk_dir)
    os.environ["JAVA_STUB_BUNDLE_PATH"] = str(stub_bundle_root)
    os.environ["JAVA_BUNDLES_PATH"] = str(pure_bundle_dir)

    # Write .env file with bundle config for both DAG bundles
    bundle_config = json.dumps(
        [
            {
                "name": JAVA_STUB_BUNDLE_NAME,
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": JAVA_CONTAINER_STUB_DAG_BUNDLE_PATH, "refresh_interval": 20},
            },
            {
                "name": JAVA_PURE_BUNDLE_NAME,
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": JAVA_CONTAINER_PURE_BUNDLE_PATH, "refresh_interval": 20},
            },
        ]
    )
    queue_to_runtime_mapping = json.dumps({"java-queue": "java"})

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        "AIRFLOW__CORE__LOAD_EXAMPLES=false\n"
        "AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG\n"
        f"AIRFLOW__WORKERS__QUEUE_TO_RUNTIME_MAPPING={queue_to_runtime_mapping}\n"
        f"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST={bundle_config}\n"
        f"AIRFLOW__JAVA__BUNDLES_FOLDER={JAVA_CONTAINER_STUB_JAVA_BUNDLES_FOLDER_PATH}\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


def spin_up_airflow_environment():
    # We indent to use explicitly created temp directory instead of pytest's tmp_path fixture because we want the directory to persist after the test run for debugging purposes and pytest's tmp_path is automatically deleted after the test run.
    tmp_dir = Path(tempfile.mkdtemp(prefix="airflow-e2e-tests-"))
    console.print(f"[yellow]Temp directory (persists after test run): {tmp_dir}")
>>>>>>> 1978a719f72 (Add [workers/queue_to_runtime_mapping])

    console.print(f"[yellow]Using docker compose file: {DOCKER_COMPOSE_PATH}")
    copyfile(DOCKER_COMPOSE_PATH, tmp_dir / "docker-compose.yaml")

    subfolders = ("dags", "logs", "plugins", "config")

    console.print(f"[yellow]Creating subfolders:[/ {subfolders}")

    for subdir in subfolders:
        (tmp_dir / subdir).mkdir()

    _E2ETestState.airflow_logs_path = tmp_dir / "logs"

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
    elif E2E_TEST_MODE == "remote_log_elasticsearch":
        compose_file_names.append("elasticsearch.yml")
        _setup_elasticsearch_integration(dot_env_file, tmp_dir)
    elif E2E_TEST_MODE == "remote_log_opensearch":
        compose_file_names.append("opensearch.yml")
        _setup_opensearch_integration(dot_env_file, tmp_dir)
    elif E2E_TEST_MODE == "xcom_object_storage":
        compose_file_names.append("localstack.yml")
        _setup_xcom_object_storage_integration(dot_env_file, tmp_dir)

    #
    # Please Do not use this Fernet key in any deployments! Please generate your own key.
    # This is specifically generated for integration tests and not as default.
    #
    os.environ["FERNET_KEY"] = generate_fernet_key_string()

    # If we are using the image from ghcr.io/apache/airflow we do not pull
    # as it is already available and loaded using prepare_breeze_and_image step in workflow
    pull = False if DOCKER_IMAGE.startswith("ghcr.io/apache/airflow/") else True

    try:
        console.print(f"[blue]Spinning up airflow environment using {DOCKER_IMAGE}")
        _E2ETestState.compose_instance = DockerCompose(
            tmp_dir, compose_file_name=compose_file_names, pull=pull
        )

        _E2ETestState.compose_instance.start()

        _E2ETestState.compose_instance.wait_for(f"http://{DOCKER_COMPOSE_HOST_PORT}/api/v2/monitor/health")
        _E2ETestState.compose_instance.exec_in_container(
            command=["airflow", "dags", "reserialize"], service_name="airflow-dag-processor"
        )

    except Exception:
        console.print("[red]Failed to start docker compose")
        if _E2ETestState.compose_instance:
            _print_logs(_E2ETestState.compose_instance)
            _E2ETestState.compose_instance.stop()
        raise


def _print_logs(compose_instance: DockerCompose):
    containers = compose_instance.get_containers()
    for container in containers:
        service = container.Service
        if service:
            stdout, _ = compose_instance.get_logs(service)
            console.print(f"::group:: {service} Logs")
            console.print(stdout, style="red", soft_wrap=True, markup=False)
            console.print("::endgroup::")


def pytest_sessionstart(session: pytest.Session):
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


def pytest_sessionfinish(session: pytest.Session, exitstatus: int | pytest.ExitCode):
    """Generate report after all tests complete."""
    generate_test_report(test_results)
    if _E2ETestState.airflow_logs_path is not None:
        copytree(_E2ETestState.airflow_logs_path, LOGS_FOLDER, dirs_exist_ok=True)

    if _E2ETestState.compose_instance:
        # If any test failures lets print the services logs
        if any(r["status"] == "failed" for r in test_results):
            _print_logs(_E2ETestState.compose_instance)
        if not os.environ.get("SKIP_DOCKER_COMPOSE_DELETION"):
            _E2ETestState.compose_instance.stop()


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
