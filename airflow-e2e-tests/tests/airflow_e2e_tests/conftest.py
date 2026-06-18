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
import subprocess
from datetime import datetime
from pathlib import Path
from shutil import copyfile, copytree

import pytest
from rich.console import Console
from testcontainers.compose import DockerCompose

from airflow_e2e_tests.constants import (
    AIRFLOW_ROOT_PATH,
    AIRFLOW_SERVICES_FOR_PROVIDER_MOUNT,
    AWS_INIT_PATH,
    DOCKER_COMPOSE_HOST_PORT,
    DOCKER_COMPOSE_PATH,
    DOCKER_IMAGE,
    E2E_DAGS_FOLDER,
    E2E_TEST_MODE,
    ELASTICSEARCH_PATH,
    GO_BUILDER_IMAGE,
    GO_COMPOSE_PATH,
    GO_SDK_BIN_PATH,
    GO_SDK_BUNDLE_NAME,
    GO_SDK_DAGS_PATH,
    GO_SDK_EXAMPLE_BUNDLE_PKG,
    JAVA_COMPOSE_PATH,
    JAVA_DOCKERFILE_PATH,
    JAVA_SDK_EXAMPLE_DAGS_PATH,
    JAVA_SDK_EXAMPLE_LIBS_PATH,
    JAVA_SDK_MAVEN_CACHE_PATH,
    KAFKA_DIR_PATH,
    LOCALSTACK_PATH,
    LOGS_FOLDER,
    OPENSEARCH_PATH,
    PROVIDERS_MOUNT_CONTAINER_PATH,
    PROVIDERS_ROOT_PATH,
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


def _copy_kafka_files(tmp_dir):
    """Copy the Kafka compose file and broker init script into the temp directory."""
    copyfile(KAFKA_DIR_PATH.parent / "kafka.yml", tmp_dir / "kafka.yml")

    kafka_dir = tmp_dir / "kafka"
    kafka_dir.mkdir()
    copyfile(KAFKA_DIR_PATH / "update_run.sh", kafka_dir / "update_run.sh")
    current_permissions = os.stat(kafka_dir / "update_run.sh").st_mode
    os.chmod(kafka_dir / "update_run.sh", current_permissions | 0o111)


def _write_providers_mount_override(tmp_dir: Path, providers: list[str]) -> list[str]:
    """Write a docker-compose override that bind-mounts in-tree provider sources.

    Each entry in ``providers`` is a provider id with dot-separated path segments (e.g.
    ``"apache.kafka"``). The host source ``providers/<dotted/as/slashes>`` is mounted
    read-only into every airflow service at ``<PROVIDERS_MOUNT_CONTAINER_PATH>/<dashed>``.
    Returns the list of in-container paths suitable for ``_PIP_ADDITIONAL_REQUIREMENTS``
    so pip installs the in-tree (latest, possibly unreleased) provider instead of the
    PyPI release.
    """
    in_container_paths: list[str] = []
    volume_entries: list[str] = []
    for provider_id in providers:
        host_path = PROVIDERS_ROOT_PATH / provider_id.replace(".", "/")
        if not host_path.is_dir():
            raise RuntimeError(f"Provider source directory not found: {host_path}")
        container_path = f"{PROVIDERS_MOUNT_CONTAINER_PATH}/{provider_id.replace('.', '-')}"
        in_container_paths.append(container_path)
        volume_entries.append(f"      - {host_path}:{container_path}:ro")

    volumes_block = "\n".join(volume_entries)
    services_block = "\n".join(
        f"  {svc}:\n    volumes:\n{volumes_block}" for svc in AIRFLOW_SERVICES_FOR_PROVIDER_MOUNT
    )
    (tmp_dir / "providers-mount.yml").write_text(f"---\nservices:\n{services_block}\n")
    return in_container_paths


def _setup_event_driven_integration(dot_env_file, tmp_dir):
    _copy_kafka_files(tmp_dir)

    # Install kafka and common-messaging providers from the in-tree sources so the
    # test exercises the latest code even before a PyPI release is cut.
    provider_paths = _write_providers_mount_override(tmp_dir, ["apache.kafka", "common.messaging"])

    kafka_conn = json.dumps(
        {
            "conn_type": "kafka",
            "extra": {
                "bootstrap.servers": "broker:29092",
                "group.id": "kafka_default_group",
                "security.protocol": "PLAINTEXT",
                "enable.auto.commit": False,
                "auto.offset.reset": "latest",
            },
        }
    )

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        f"AIRFLOW_CONN_KAFKA_DEFAULT='{kafka_conn}'\n"
        f"_PIP_ADDITIONAL_REQUIREMENTS={' '.join(provider_paths)}\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


def _create_kafka_topics(compose_instance):
    """Create Kafka topics required by the event-driven Dag."""
    for topic in ("fizz_buzz", "dlq"):
        compose_instance.exec_in_container(
            command=[
                "kafka-topics",
                "--bootstrap-server",
                "broker:29092",
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
                "--if-not-exists",
            ],
            service_name="broker",
        )


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


def _setup_java_sdk_integration(dot_env_file, tmp_dir):
    """Set up the java_sdk E2E test mode.

    Builds the Java example bundle via the Gradle wrapper, then builds a
    Java-capable Airflow worker image, copies the JARs into the temp directory,
    and writes the coordinator configuration.
    """
    # * --user keeps build outputs owned by the current user (not root).
    # * --no-daemon avoids a background JVM that would outlive the container.
    # * GRADLE_USER_HOME persists the Gradle distribution and dependency cache
    #   in java-sdk/.gradle/ so subsequent runs skip straight to compilation.
    # * HOME is set explicitly because --user runs as the host UID which has no
    #   entry in the container's /etc/passwd; Docker would otherwise inherit the
    #   image's HOME (/root) which the non-root process cannot write to.
    # * files/m2 is mounted directly as ~/.m2 so publishToMavenLocal writes
    #   there without nesting, and its contents are visible on the host.
    console.print("[yellow]Publishing Java SDK artifacts to local Maven repository...")
    JAVA_SDK_MAVEN_CACHE_PATH.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-e",
            "GRADLE_USER_HOME=/repo/java-sdk/.gradle",
            "-e",
            "HOME=/workspace-home",
            "-v",
            f"{JAVA_SDK_MAVEN_CACHE_PATH}:/workspace-home/.m2",
            "-v",
            f"{AIRFLOW_ROOT_PATH}:/repo",
            "-w",
            "/repo/java-sdk",
            "eclipse-temurin:17-jdk",
            "./gradlew",
            "publishToMavenLocal",
            "-PskipSigning=true",
            "--no-daemon",
        ],
        check=True,
    )
    console.print("[yellow]Building Java SDK example bundle (eclipse-temurin:17-jdk)...")
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-e",
            "GRADLE_USER_HOME=/repo/java-sdk/.gradle",
            "-e",
            "HOME=/workspace-home",
            "-v",
            f"{JAVA_SDK_MAVEN_CACHE_PATH}:/workspace-home/.m2",
            "-v",
            f"{AIRFLOW_ROOT_PATH}:/repo",
            "-w",
            "/repo/java-sdk/example",
            "eclipse-temurin:17-jdk",
            "../gradlew",
            "bundle",
            "--no-daemon",
        ],
        check=True,
    )

    # Copy compose override and Dockerfile into the temp directory.
    copyfile(JAVA_COMPOSE_PATH, tmp_dir / "java.yml")
    copyfile(JAVA_DOCKERFILE_PATH, tmp_dir / "Dockerfile.java")

    # Copy all JARs from installDist output so the compose bind-mount ./jars
    # gives the worker everything JavaCoordinator needs to build a classpath.
    copytree(JAVA_SDK_EXAMPLE_LIBS_PATH, tmp_dir / "jars")

    # Copy the Java SDK example Dag file so Airflow can discover it.
    copyfile(JAVA_SDK_EXAMPLE_DAGS_PATH / "java_examples.py", tmp_dir / "dags" / "java_examples.py")

    # Build a local Docker image that extends DOCKER_IMAGE with a JRE.
    # We do this explicitly so testcontainers' DockerCompose.start() does not
    # need to handle the build itself (which avoids --no-build vs --build flag
    # uncertainty across testcontainers versions).
    console.print(f"[yellow]Building airflow-java-worker image on top of {DOCKER_IMAGE}...")
    subprocess.run(
        [
            "docker",
            "build",
            "--build-arg",
            f"DOCKER_IMAGE={DOCKER_IMAGE}",
            "-t",
            "airflow-java-worker",
            "-f",
            str(tmp_dir / "Dockerfile.java"),
            str(tmp_dir),
        ],
        check=True,
    )

    # Coordinator registry: maps the logical name "java-jdk" to JavaCoordinator.
    # Queue mapping: routes tasks on the "java" Celery queue to "java-jdk".
    coordinator_config = json.dumps(
        {
            "java-jdk": {
                "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
                "kwargs": {"jars_root": ["/opt/airflow/jars"]},
            }
        }
    )
    queue_to_coordinator = json.dumps({"java": "java-jdk"})

    # Connection expected by the Java example bundle tasks. The JSON form
    # covers all connection fields, in particular the port: wire integers
    # arrive in the JVM as Long and the SDK must map them to Int.
    test_http_conn = json.dumps(
        {
            "conn_type": "http",
            "login": "user",
            "password": "pass",
            "host": "example.com",
            "port": 1234,
            "extra": {"param1": "val1", "param2": "val2"},
        }
    )

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        # Single-quote the JSON values so Docker Compose reads them literally.
        f"AIRFLOW__SDK__COORDINATORS='{coordinator_config}'\n"
        f"AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{queue_to_coordinator}'\n"
        f"AIRFLOW_CONN_TEST_HTTP='{test_http_conn}'\n"
        # Variable expected by the Java example bundle tasks.
        "AIRFLOW_VAR_MY_VARIABLE=test_value\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


def _setup_go_sdk_integration(dot_env_file, tmp_dir):
    """Set up the go_sdk E2E test mode.

    Compiles the Go SDK example bundle into a self-contained executable bundle
    via the ``airflow-go-pack`` tooling, drops it into the directory the
    ``ExecutableCoordinator`` scans, copies the Python stub Dag, and writes the
    coordinator configuration.

    The packed bundle is a statically linked native executable (built with
    ``CGO_ENABLED=0``), so the stock Airflow worker image can exec it directly
    without a Go toolchain or any extra runtime installed -- see ``go.yml``.
    """
    # Build + pack the example bundle inside an ephemeral Go container so the
    # host does not need Go installed.
    #
    # --user keeps build outputs owned by the current user (not root).
    # HOME points at a writable, gitignored dir under go-sdk/bin so the Go build
    # and module caches persist between runs (first run downloads modules once;
    # subsequent runs skip straight to compilation).
    # CGO_ENABLED=0 yields a fully static binary that runs on the stock worker.
    # USER/HOME must be set because the SDK calls user.Current() at init; with
    # cgo disabled Go's pure-Go resolver reads those env vars instead of libc,
    # and panics if either is empty (the same vars are set on the worker in
    # go.yml so the packed binary runs the same way at execution time).
    # `go tool airflow-go-pack` builds the bundle package, reads its
    # --airflow-metadata, and appends the source + airflow-metadata.yaml + the
    # AFBNDL01 trailer, writing a single self-contained executable bundle.
    go_cache_home = "/repo/go-sdk/bin/.home"
    bundle_out = f"/repo/go-sdk/bin/{GO_SDK_BUNDLE_NAME}"
    console.print(f"[yellow]Building Go SDK example bundle ({GO_BUILDER_IMAGE})...")
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-e",
            f"HOME={go_cache_home}",
            "-e",
            "USER=airflow",
            "-e",
            "CGO_ENABLED=0",
            # Mount the repo so the whole go-sdk module (go.mod, tool directive,
            # example sources) is visible to `go tool`.
            "-v",
            f"{AIRFLOW_ROOT_PATH}:/repo",
            "-w",
            "/repo/go-sdk",
            GO_BUILDER_IMAGE,
            "go",
            "tool",
            "airflow-go-pack",
            "--output",
            bundle_out,
            GO_SDK_EXAMPLE_BUNDLE_PKG,
        ],
        check=True,
    )

    # Copy the compose override into the temp directory.
    copyfile(GO_COMPOSE_PATH, tmp_dir / "go.yml")

    # Place the packed bundle where the compose bind-mount (./go-bundles) exposes
    # it to the worker at /opt/airflow/go-bundles. The bundle scanner requires
    # the file to be executable, so preserve the exec bit.
    go_bundles_dir = tmp_dir / "go-bundles"
    go_bundles_dir.mkdir()
    packed_bundle = go_bundles_dir / GO_SDK_BUNDLE_NAME
    copyfile(GO_SDK_BIN_PATH / GO_SDK_BUNDLE_NAME, packed_bundle)
    os.chmod(packed_bundle, 0o755)

    # Copy the Go SDK example stub Dag so Airflow can discover and serialize it.
    copyfile(GO_SDK_DAGS_PATH / "go_examples.py", tmp_dir / "dags" / "go_examples.py")

    # Coordinator registry: maps the logical name "go-sdk" to ExecutableCoordinator,
    # which scans executables_root for the packed bundle by dag_id.
    # Queue mapping: routes tasks on the "golang" queue to "go-sdk".
    coordinator_config = json.dumps(
        {
            "go-sdk": {
                "classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator",
                "kwargs": {"executables_root": ["/opt/airflow/go-bundles"]},
            }
        }
    )
    queue_to_coordinator = json.dumps({"golang": "go-sdk"})

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        # Single-quote the JSON values so Docker Compose reads them literally.
        f"AIRFLOW__SDK__COORDINATORS='{coordinator_config}'\n"
        f"AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{queue_to_coordinator}'\n"
        # Connection and variable read by the Go example bundle tasks.
        "AIRFLOW_CONN_TEST_HTTP=http://test:test@example.com/\n"
        "AIRFLOW_VAR_MY_VARIABLE=test_value\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


def spin_up_airflow_environment(tmp_path_factory: pytest.TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("breeze-airflow-e2e-tests")

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
    elif E2E_TEST_MODE == "event_driven":
        compose_file_names.extend(["kafka.yml", "providers-mount.yml"])
        _setup_event_driven_integration(dot_env_file, tmp_dir)
    elif E2E_TEST_MODE == "java_sdk":
        compose_file_names.append("java.yml")
        _setup_java_sdk_integration(dot_env_file, tmp_dir)
    elif E2E_TEST_MODE == "go_sdk":
        compose_file_names.append("go.yml")
        _setup_go_sdk_integration(dot_env_file, tmp_dir)

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

        if E2E_TEST_MODE == "event_driven":
            console.print("[yellow]Creating Kafka topics...")
            _create_kafka_topics(_E2ETestState.compose_instance)

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


@pytest.fixture(scope="session")
def compose_instance():
    """Provide access to the running Docker Compose instance."""
    return _E2ETestState.compose_instance


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
