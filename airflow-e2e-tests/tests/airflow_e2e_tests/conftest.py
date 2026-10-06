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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from shutil import copyfile, copytree, rmtree

import pytest
from rich.console import Console
from testcontainers.compose import DockerCompose

from airflow_e2e_tests.constants import (
    AIRFLOW_ROOT_PATH,
    AIRFLOW_SERVICES_FOR_PROVIDER_MOUNT,
    AWS_INIT_PATH,
    DAGS_BUNDLE_NAME,
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
    GO_SDK_COORDINATOR,
    GO_SDK_DAGS_PATH,
    GO_SDK_EXAMPLE_BUNDLE_PKG,
    GO_SDK_QUEUE,
    GO_SDK_ROOT_PATH,
    GO_SDK_TASK_HANDLER_BUNDLE,
    GO_TEST_BUNDLE_ARTIFACT,
    GO_TEST_BUNDLE_BUILD_PATH,
    GO_TEST_BUNDLE_DAGS_PATH,
    GO_TEST_BUNDLE_PKG,
    GO_TEST_BUNDLE_ROOT_PATH,
    GO_TEST_COORDINATOR,
    GO_TEST_QUEUE,
    GO_TEST_TASK_HANDLER_BUNDLE,
    JAVA_COMPOSE_PATH,
    JAVA_DOCKERFILE_PATH,
    JAVA_SDK_EXAMPLE_DAGS_PATH,
    JAVA_SDK_EXAMPLE_LIBS_PATH,
    JAVA_SDK_MAVEN_CACHE_PATH,
    JAVA_SDK_QUEUE,
    JAVA_SDK_ROOT_PATH,
    JAVA_SDK_TASK_HANDLER_BUNDLE,
    JAVA_TEST_BUNDLE_DAGS_PATH,
    JAVA_TEST_BUNDLE_LIBS_PATH,
    JAVA_TEST_BUNDLE_ROOT_PATH,
    JAVA_TEST_QUEUE,
    JAVA_TEST_TASK_HANDLER_BUNDLE,
    KAFKA_DIR_PATH,
    LANG_SDK_E2E_MODES,
    LANG_SDK_NATIVE_TOOLCHAIN,
    LOCALSTACK_PATH,
    LOGS_FOLDER,
    NODE_IMAGE,
    OPENLINEAGE_COMPAT_DB_COMPOSE_PATH,
    OPENLINEAGE_COMPOSE_PATH,
    OPENSEARCH_PATH,
    PROVIDERS_MOUNT_CONTAINER_PATH,
    PROVIDERS_ROOT_PATH,
    SCALA_SPARK_EXAMPLE_DAGS_PATH,
    SCALA_SPARK_EXAMPLE_LIBS_PATH,
    SCALA_SPARK_QUEUE,
    SCALA_SPARK_TASK_HANDLER_BUNDLE,
    TEST_REPORT_FILE,
    TS_COMPOSE_PATH,
    TS_SDK_BUILD_HOME_PATH,
    TS_SDK_BUNDLE_FILE,
    TS_SDK_EXAMPLE_PATH,
    TS_SDK_QUEUE,
    TS_SDK_ROOT_PATH,
    TS_SDK_TASK_HANDLER_BUNDLE,
    XCOM_BUCKET,
)
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.e2e_test_utils.go_toolchain import run_go
from airflow_e2e_tests.e2e_test_utils.lang_sdk import wait_until_stub_tasks_are_checked

from tests_common.test_utils.fernet import generate_fernet_key_string

console = Console(width=400, color_system="standard")


class _E2ETestState:
    compose_instance: DockerCompose | None = None
    airflow_logs_path: Path | None = None
    airflow_dags_path: Path | None = None
    # In the Lang-SDK modes: the Dag files of the Lang SDK that the stack was given, and the files of the
    # Dags folder that fail to import by design.
    lang_sdk_dag_files: tuple[str, ...] = ()
    expected_import_errors: tuple[str, ...] = ()


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


# Spark normally injects these JVM options through its own launcher; the raw
# JavaCoordinator launch bypasses that, so the bundle must carry them itself.
# This mirrors org.apache.spark.launcher.JavaModuleOptions.defaultModuleOptions()
# verbatim for the pinned Spark 3.5.8 (java-sdk/scala_spark_example/build.gradle).
# A partial set passes the toy aggregation here but breaks real Spark code paths
# (Kryo -> java.lang.reflect, off-heap cleaner -> jdk.internal.ref, charset ->
# sun.nio.cs, Kerberos -> sun.security.krb5); keep it in sync if Spark is bumped.
# The user-facing writeup lives in java-sdk/scala_spark_example/README.md.
_SPARK_JAVA_MODULE_OPTIONS = [
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    "-Djdk.reflect.useDirectMethodHandle=false",
]


def _build_dag_bundle_config(artifact_bundles: dict[str, str]) -> str:
    """Return a ``dag_bundle_config_list`` of the Dags folder plus a ``LocalDagBundle`` per artifact path.

    Registration is what makes a coordinator's ``task_handler_bundle_name`` resolvable on the
    worker and the Dag processor. The artifact directories are mounted on both; the Dag processor
    runs the artifacts in them to check the stub tasks, and finds no Dag files there.
    """
    local_bundle = "airflow.dag_processing.bundles.local.LocalDagBundle"
    bundles = [{"name": DAGS_BUNDLE_NAME, "classpath": local_bundle, "kwargs": {}}]
    bundles.extend(
        {"name": name, "classpath": local_bundle, "kwargs": {"path": path}}
        for name, path in artifact_bundles.items()
    )
    return json.dumps(bundles)


def _ignore_dag_files_in(directory: Path) -> None:
    """Keep the Dag processor from parsing the task handler artifacts in *directory* as Dag files.

    Native's Java and Node Dag importers register in every bundle, and otherwise claim every ``.jar``
    or bundled script mounted here (see docker/go.yml, java.yml, ts.yml) once it tries to import it as
    a Dag file. The stub-task check itself is unaffected: the coordinator's find hook scans the
    filesystem directly and never reads ``.airflowignore``.
    """
    (directory / ".airflowignore").write_text("*\n")


def _run_java_sdk_gradle(workdir, *gradle_argv, capture_output=False, native=False):
    """Run the Java SDK Gradle wrapper natively or inside the pinned JDK container.

    In ``native`` mode (used in CI, where the host already has a cached JDK +
    Gradle cache via ``actions/setup-java``) it invokes the host ``./gradlew``
    directly, skipping the toolchain-image pull and the container workarounds
    below while reusing the runner's ~/.gradle cache.

    The containerized path stays the default for local runs so a dev host needs
    no JDK installed:

    * --user keeps build outputs owned by the current user (not root).
    * --network=host shares one loopback across concurrent builds so Gradle's
      cross-process lock handover works: a UDP ping to the lock owner's port
      (org.gradle.cache.internal.locklistener.FileLockCommunicator.pingOwner,
      see https://github.com/gradle/gradle/blob/v8.14.4/platforms/core-execution/persistent-cache/src/main/java/org/gradle/cache/internal/locklistener/FileLockCommunicator.java)
      would fail across isolated container network namespaces, as reported in
      https://github.com/gradle/gradle/issues/851.
    * --no-daemon avoids a background JVM that would outlive the container.
    * GRADLE_USER_HOME persists the Gradle distribution and dependency cache
      in java-sdk/.gradle/ so subsequent runs skip straight to compilation.
    * HOME is set explicitly because --user runs as the host UID which has no
      entry in the container's /etc/passwd; Docker would otherwise inherit the
      image's HOME (/root) which the non-root process cannot write to.
    * files/m2 is mounted directly as ~/.m2 so publishToMavenLocal writes
      there without nesting, and its contents are visible on the host.
    """
    if native:
        cwd = workdir
        argv = [
            str(JAVA_SDK_ROOT_PATH / "gradlew"),
            "--no-daemon",
            "--console=plain",
            *gradle_argv,
        ]
    else:
        JAVA_SDK_MAVEN_CACHE_PATH.mkdir(parents=True, exist_ok=True)
        cwd = None
        argv = [
            "docker",
            "run",
            "--rm",
            "--network=host",
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
            f"/repo/{workdir.relative_to(AIRFLOW_ROOT_PATH)}",
            "eclipse-temurin:17-jdk",
            "/repo/java-sdk/gradlew",
            "--no-daemon",
            *gradle_argv,
        ]
    return subprocess.run(argv, cwd=cwd, check=True, capture_output=capture_output, text=True)


def _build_example_bundle(workdir, *, native=False):
    """Build one example bundle, capturing output so concurrent builds don't interleave."""
    try:
        completed = _run_java_sdk_gradle(workdir, "bundle", capture_output=True, native=native)
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Bundle build failed in {workdir}:")
        console.print(e.stdout, e.stderr, sep="\n", markup=False, soft_wrap=True)
        raise
    console.print(f"[yellow]Bundle build finished in {workdir}:")
    console.print(completed.stdout, completed.stderr, sep="\n", markup=False, soft_wrap=True)


def _setup_java_sdk_integration(dot_env_file, tmp_dir):
    """Set up the java_sdk E2E test mode.

    Builds the Java SDK and Scala Spark example bundles via the Gradle wrapper,
    then builds a Java-capable Airflow worker image, copies the JARs into the
    temp directory, and writes the coordinator configuration.
    """
    native = LANG_SDK_NATIVE_TOOLCHAIN
    console.print("[yellow]Publishing Java SDK artifacts to local Maven repository...")
    _run_java_sdk_gradle(JAVA_SDK_ROOT_PATH, "publishToMavenLocal", "-PskipSigning=true", native=native)

    # The example, scala_spark_example, and java-test-bundle are independent
    # Gradle builds that all consume the SDK artifact published above, so build
    # them concurrently. Sharing a writable Gradle user home between concurrent
    # builds is safe because each build can ping the other's lock-owner port over
    # one shared loopback - the host's own in native mode, --network=host in the
    # container path (see the helper's docstring); publishToMavenLocal has
    # already unpacked the shared wrapper distribution, so no build races to
    # fetch it.
    #
    # The Gradle `bundle` task is a Copy that never prunes its destination, so
    # JARs from an earlier build linger. A stale dependency JAR with its own
    # Main-Class would make JavaCoordinator's Main-Class discovery ambiguous, so
    # start each bundle from an empty directory.
    rmtree(JAVA_SDK_EXAMPLE_LIBS_PATH, ignore_errors=True)
    rmtree(SCALA_SPARK_EXAMPLE_LIBS_PATH, ignore_errors=True)
    rmtree(JAVA_TEST_BUNDLE_LIBS_PATH, ignore_errors=True)
    toolchain = "host toolchain" if native else "eclipse-temurin:17-jdk"
    console.print(
        f"[yellow]Building Java SDK, Scala Spark, and test-fixture bundles concurrently ({toolchain})..."
    )
    example_bundle_workdirs = [
        JAVA_SDK_ROOT_PATH / "example",
        JAVA_SDK_ROOT_PATH / "scala_spark_example",
        JAVA_TEST_BUNDLE_ROOT_PATH,
    ]
    with ThreadPoolExecutor(max_workers=len(example_bundle_workdirs)) as pool:
        bundle_builds = [
            pool.submit(_build_example_bundle, workdir, native=native) for workdir in example_bundle_workdirs
        ]
        for build in bundle_builds:
            build.result()

    # Copy compose override and Dockerfile into the temp directory.
    copyfile(JAVA_COMPOSE_PATH, tmp_dir / "java.yml")
    copyfile(JAVA_DOCKERFILE_PATH, tmp_dir / "Dockerfile.java")

    # Copy each bundle's JARs into its own directory; the compose bind-mounts expose them to the
    # worker and the Dag processor, where each is registered as its own Dag bundle.
    copytree(JAVA_SDK_EXAMPLE_LIBS_PATH, tmp_dir / "java-jars")
    copytree(SCALA_SPARK_EXAMPLE_LIBS_PATH, tmp_dir / "scala-jars")
    copytree(JAVA_TEST_BUNDLE_LIBS_PATH, tmp_dir / "java-test-jars")
    for artifact_dir in ("java-jars", "scala-jars", "java-test-jars"):
        _ignore_dag_files_in(tmp_dir / artifact_dir)

    # Copy the Java SDK example Dag files so Airflow can discover them.
    copyfile(JAVA_SDK_EXAMPLE_DAGS_PATH / "java_examples.py", tmp_dir / "dags" / "java_examples.py")
    copyfile(
        SCALA_SPARK_EXAMPLE_DAGS_PATH / "scala_spark_examples.py",
        tmp_dir / "dags" / "scala_spark_examples.py",
    )
    copyfile(JAVA_TEST_BUNDLE_DAGS_PATH / "java_test_dags.py", tmp_dir / "dags" / "java_test_dags.py")
    copyfile(
        JAVA_TEST_BUNDLE_DAGS_PATH / "java_task_handler_failures.py",
        tmp_dir / "dags" / "java_task_handler_failures.py",
    )
    _E2ETestState.lang_sdk_dag_files = (
        "java_examples.py",
        "scala_spark_examples.py",
        "java_test_dags.py",
        "java_task_handler_failures.py",
    )
    _E2ETestState.expected_import_errors = ("java_task_handler_failures.py",)

    # Keep the bundle JARs out of the build context: Dockerfile.java only adds a
    # JRE and copies nothing from the context, so without this docker build would
    # tar and stream the bundles (hundreds of MB of Spark JARs) to the daemon for
    # nothing. The JARs reach the worker and the Dag processor via the compose
    # bind-mounts, not the image.
    (tmp_dir / ".dockerignore").write_text("java-jars/\nscala-jars/\njava-test-jars/\n")

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

    # One JavaCoordinator per queue on the same worker image, each serving its
    # own artifact bundle (one bundle is one classpath). The scala-jdk entry pins
    # main_class (Spark's large classpath makes Main-Class discovery ambiguous)
    # and carries Spark's Java 17 module openings, a small driver heap, and a
    # longer startup timeout for its large dependency classpath.
    dag_bundle_config = _build_dag_bundle_config(
        {
            JAVA_SDK_TASK_HANDLER_BUNDLE: "/opt/airflow/java-jars",
            SCALA_SPARK_TASK_HANDLER_BUNDLE: "/opt/airflow/scala-jars",
            JAVA_TEST_TASK_HANDLER_BUNDLE: "/opt/airflow/java-test-jars",
        }
    )
    coordinator_config = json.dumps(
        {
            "java-jdk": {
                "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
                "kwargs": {"task_handler_bundle_name": JAVA_SDK_TASK_HANDLER_BUNDLE},
            },
            "scala-jdk": {
                "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
                "kwargs": {
                    "task_handler_bundle_name": SCALA_SPARK_TASK_HANDLER_BUNDLE,
                    "main_class": "org.apache.airflow.example.ScalaSparkBundleBuilder",
                    "jvm_args": ["-Xmx512m", *_SPARK_JAVA_MODULE_OPTIONS],
                    "task_startup_timeout": 60.0,
                },
            },
            "java-test-jdk": {
                "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
                "kwargs": {"task_handler_bundle_name": JAVA_TEST_TASK_HANDLER_BUNDLE},
            },
        }
    )
    queue_to_coordinator = json.dumps(
        {JAVA_SDK_QUEUE: "java-jdk", SCALA_SPARK_QUEUE: "scala-jdk", JAVA_TEST_QUEUE: "java-test-jdk"}
    )

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
        f"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='{dag_bundle_config}'\n"
        f"AIRFLOW__SDK__COORDINATORS='{coordinator_config}'\n"
        f"AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{queue_to_coordinator}'\n"
        f"AIRFLOW_CONN_TEST_HTTP='{test_http_conn}'\n"
        # Variable expected by the Java example bundle tasks.
        "AIRFLOW_VAR_MY_VARIABLE=test_value\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


def _pack_go_bundle(module: Path, package: str, output: Path, *, native: bool):
    """
    Pack the ``main`` package *package* of the Go module *module* into the bundle *output*.

    ``go tool airflow-go-pack`` builds the package, execs the built binary with --airflow-metadata to
    read its Dag/task inventory, and appends the source + airflow-metadata.yaml + the AFBNDL01 trailer,
    writing a single self-contained executable bundle.
    The output of a failed pack is printed, so the build log is not lost.
    """
    mode_label = "host toolchain" if native else GO_BUILDER_IMAGE
    console.print(f"[yellow]Packing Go bundle {output.name} from {package} ({mode_label})...")
    try:
        completed = run_go(
            ["tool", "airflow-go-pack", "--output", output, package], module=module, native=native
        )
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Packing Go bundle {output.name} failed:")
        console.print(e.stdout, e.stderr, sep="\n", markup=False, soft_wrap=True)
        raise
    console.print(completed.stdout, completed.stderr, sep="\n", markup=False, soft_wrap=True)


def _setup_go_sdk_integration(dot_env_file, tmp_dir):
    """Set up the go_sdk E2E test mode.

    Compiles the Go SDK example bundle and the Go test bundle into self-contained executable
    bundles via the ``airflow-go-pack`` tooling, drops each into the directory registered as its
    Dag bundle, copies the Python stub Dags, and writes the coordinator configuration.

    The packed bundles are statically linked native executables (built with
    ``CGO_ENABLED=0``), so the stock Airflow image can exec them directly on the
    worker and the Dag processor without a Go toolchain or any extra runtime
    installed -- see ``go.yml``.

    The example keeps its own coordinator, Dag bundle and queue, and the test bundle has its
    own, so the failure fixtures of the test bundle cannot change what the example registers.
    """
    native = LANG_SDK_NATIVE_TOOLCHAIN
    # The packs share the Go module and build caches, which are safe to share between processes.
    with ThreadPoolExecutor(max_workers=2) as pool:
        example_pack = pool.submit(
            _pack_go_bundle,
            GO_SDK_ROOT_PATH,
            GO_SDK_EXAMPLE_BUNDLE_PKG,
            GO_SDK_BIN_PATH / GO_SDK_BUNDLE_NAME,
            native=native,
        )
        test_pack = pool.submit(
            _pack_go_bundle,
            GO_TEST_BUNDLE_ROOT_PATH,
            GO_TEST_BUNDLE_PKG,
            GO_TEST_BUNDLE_BUILD_PATH / GO_TEST_BUNDLE_ARTIFACT,
            native=native,
        )
        example_pack.result()
        test_pack.result()

    # Copy the compose override into the temp directory.
    copyfile(GO_COMPOSE_PATH, tmp_dir / "go.yml")

    # Place each packed bundle where the compose bind-mount (./go-bundles and ./go-test-bundles)
    # exposes it to the worker and the Dag processor at /opt/airflow/go-bundles and
    # /opt/airflow/go-test-bundles. The coordinator runs only an executable file, so preserve
    # the exec bit.
    go_bundles_dir = tmp_dir / "go-bundles"
    go_bundles_dir.mkdir()
    packed_bundle = go_bundles_dir / GO_SDK_BUNDLE_NAME
    copyfile(GO_SDK_BIN_PATH / GO_SDK_BUNDLE_NAME, packed_bundle)
    os.chmod(packed_bundle, 0o755)
    _ignore_dag_files_in(go_bundles_dir)

    go_test_bundles_dir = tmp_dir / "go-test-bundles"
    go_test_bundles_dir.mkdir()
    packed_test_bundle = go_test_bundles_dir / GO_TEST_BUNDLE_ARTIFACT
    copyfile(GO_TEST_BUNDLE_BUILD_PATH / GO_TEST_BUNDLE_ARTIFACT, packed_test_bundle)
    os.chmod(packed_test_bundle, 0o755)
    _ignore_dag_files_in(go_test_bundles_dir)

    # Copy the Go SDK example stub Dag so Airflow can discover and serialize it, and the test
    # bundle's stub Dags.
    copyfile(GO_SDK_DAGS_PATH / "go_examples.py", tmp_dir / "dags" / "go_examples.py")
    go_test_dag_files = sorted(GO_TEST_BUNDLE_DAGS_PATH.glob("*.py"))
    for dag_file in go_test_dag_files:
        copyfile(dag_file, tmp_dir / "dags" / dag_file.name)
    _E2ETestState.lang_sdk_dag_files = ("go_examples.py", *(dag_file.name for dag_file in go_test_dag_files))
    # The one Dag file that fails to import, because its stub tasks do not match their task handlers.
    _E2ETestState.expected_import_errors = ("go_task_handler_failures.py",)

    # Coordinator registry: maps each logical name to an ExecutableCoordinator, which scans its
    # own Dag bundle for the packed bundle by dag_id.
    # Queue mapping: routes tasks on the "golang" queue to "go-sdk" and on "golang-test" to
    # "go-test-sdk".
    dag_bundle_config = _build_dag_bundle_config(
        {
            GO_SDK_TASK_HANDLER_BUNDLE: "/opt/airflow/go-bundles",
            GO_TEST_TASK_HANDLER_BUNDLE: "/opt/airflow/go-test-bundles",
        }
    )
    executable_coordinator = "airflow.sdk.coordinators.executable.ExecutableCoordinator"
    coordinator_config = json.dumps(
        {
            GO_SDK_COORDINATOR: {
                "classpath": executable_coordinator,
                "kwargs": {"task_handler_bundle_name": GO_SDK_TASK_HANDLER_BUNDLE},
            },
            GO_TEST_COORDINATOR: {
                "classpath": executable_coordinator,
                "kwargs": {"task_handler_bundle_name": GO_TEST_TASK_HANDLER_BUNDLE},
            },
        }
    )
    queue_to_coordinator = json.dumps({GO_SDK_QUEUE: GO_SDK_COORDINATOR, GO_TEST_QUEUE: GO_TEST_COORDINATOR})

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        # Single-quote the JSON values so Docker Compose reads them literally.
        f"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='{dag_bundle_config}'\n"
        f"AIRFLOW__SDK__COORDINATORS='{coordinator_config}'\n"
        f"AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{queue_to_coordinator}'\n"
        # Connection and variable read by the Go example bundle tasks.
        "AIRFLOW_CONN_TEST_HTTP=http://test:test@example.com/\n"
        "AIRFLOW_VAR_MY_VARIABLE=test_value\n"
    )
    os.environ["ENV_FILE_PATH"] = str(dot_env_file)


def _setup_openlineage_integration(dot_env_file, tmp_dir, compose_file_names):
    """Set up the openlineage E2E test mode.

    The OpenLineage system-test DAGs are the single source of truth; ``prepare_dags`` copies them
    into the stack's dags folder (stripping the pytest-only footer) alongside the harness-only warmup
    DAG and versioned bundle. The ``openlineage.yml`` overlay carries the OpenLineage env config and
    mounts the generated ``dag_doc.md`` where the docs DAG resolves it.
    """
    from airflow_e2e_tests.openlineage_tests.prepare_dags import prepare_dags

    console.print("[yellow]Preparing OpenLineage DAGs from the provider system tests...")
    prepare_dags(tmp_dir / "dags")
    copyfile(OPENLINEAGE_COMPOSE_PATH, tmp_dir / "openlineage.yml")

    if os.environ.get("E2E_TARGET_AIRFLOW_VERSION", "").strip():
        copyfile(OPENLINEAGE_COMPAT_DB_COMPOSE_PATH, tmp_dir / "openlineage-compat-db.yml")
        compose_file_names.append("openlineage-compat-db.yml")


def _build_ts_sdk_example_bundle(*, native=False):
    build_commands = (
        "pnpm install --frozen-lockfile && pnpm run build && cd example && pnpm install && pnpm run build"
    )
    if native:
        console.print("[yellow]Building TypeScript SDK example bundle (host toolchain)...")
        subprocess.run(["bash", "-c", build_commands], cwd=TS_SDK_ROOT_PATH, check=True)
        return
    # --user keeps build outputs owned by the current user; HOME is a
    # writable, gitignored dir so pnpm/corepack caches persist between runs.
    TS_SDK_BUILD_HOME_PATH.mkdir(parents=True, exist_ok=True)
    # corepack shims go in $HOME/bin (on PATH) because the container user
    # cannot write to /usr/local/bin.
    build_script = (
        'export PATH="$HOME/bin:$PATH"'
        ' && mkdir -p "$HOME/bin"'
        ' && corepack enable --install-directory "$HOME/bin"'
        " && cd /repo/ts-sdk"
        f" && {build_commands}"
    )
    console.print(f"[yellow]Building TypeScript SDK example bundle ({NODE_IMAGE})...")
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-e",
            "HOME=/repo/files/pnpm-home",
            "-e",
            "COREPACK_ENABLE_DOWNLOAD_PROMPT=0",
            "-e",
            "CI=true",
            "-v",
            f"{AIRFLOW_ROOT_PATH}:/repo",
            NODE_IMAGE,
            "bash",
            "-c",
            build_script,
        ],
        check=True,
    )


def _setup_ts_sdk_integration(dot_env_file, tmp_dir):
    """Set up the ts_sdk E2E test mode."""
    _build_ts_sdk_example_bundle(native=LANG_SDK_NATIVE_TOOLCHAIN)

    copyfile(TS_COMPOSE_PATH, tmp_dir / "ts.yml")

    # Deliberately no metadata sidecar: the coordinator must resolve the schema
    # version from the metadata airflow-ts-pack embedded in the bundle.
    ts_bundles_dir = tmp_dir / "ts-bundles"
    ts_bundles_dir.mkdir()
    # Deliberately renamed: the coordinator routes on embedded metadata, not on a fixed name.
    copyfile(TS_SDK_EXAMPLE_PATH / "dist" / "bundle.min.mjs", ts_bundles_dir / TS_SDK_BUNDLE_FILE)
    _ignore_dag_files_in(ts_bundles_dir)

    # Both of the example bundle's Dags: one bundle.mjs provides for two dag_ids,
    # and the tests check that dispatch tells their same-named tasks apart.
    ts_dag_files = ("typescript_example.py", "typescript_taskflow_example.py")
    for dag_file in ts_dag_files:
        copyfile(TS_SDK_EXAMPLE_PATH / "dags" / dag_file, tmp_dir / "dags" / dag_file)
    _E2ETestState.lang_sdk_dag_files = ts_dag_files

    dag_bundle_config = _build_dag_bundle_config({TS_SDK_TASK_HANDLER_BUNDLE: "/opt/airflow/ts-bundles"})
    coordinator_config = json.dumps(
        {
            "ts": {
                "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
                "kwargs": {
                    "task_handler_bundle_name": TS_SDK_TASK_HANDLER_BUNDLE,
                    "node_executable": "/opt/nodejs/node",
                },
            }
        }
    )
    queue_to_coordinator = json.dumps({TS_SDK_QUEUE: "ts"})

    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        f"NODE_IMAGE={NODE_IMAGE}\n"
        # single-quoted so Docker Compose reads the JSON literally
        f"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='{dag_bundle_config}'\n"
        f"AIRFLOW__SDK__COORDINATORS='{coordinator_config}'\n"
        f"AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{queue_to_coordinator}'\n"
        "AIRFLOW_CONN_TYPESCRIPT_EXAMPLE_HTTP=http://user:pass@example.com/\n"
        "AIRFLOW_VAR_TYPESCRIPT_EXAMPLE_GREETING=greetings from e2e\n"
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
    _E2ETestState.airflow_dags_path = tmp_dir / "dags"

    # openlineage sources its dags from the provider system tests (via _setup_openlineage_integration),
    # so it must not also load the stock e2e dags, since the harness triggers every dag it finds.
    if E2E_TEST_MODE != "openlineage":
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
    elif E2E_TEST_MODE == "openlineage":
        compose_file_names.append("openlineage.yml")
        _setup_openlineage_integration(dot_env_file, tmp_dir, compose_file_names)
    elif E2E_TEST_MODE == "ts_sdk":
        compose_file_names.append("ts.yml")
        _setup_ts_sdk_integration(dot_env_file, tmp_dir)

    #
    # Please Do not use this Fernet key in any deployments! Please generate your own key.
    # This is specifically generated for integration tests and not as default.
    #
    os.environ["FERNET_KEY"] = generate_fernet_key_string()

    # Skip pull for images that exist only locally and cannot be fetched from a registry:
    # - ghcr.io/apache/airflow/: pre-pulled by the prepare_breeze_and_image CI step
    # - openlineage-e2e/: locally built by _build_openlineage_e2e_compat_image (never pushed)
    pull = not DOCKER_IMAGE.startswith(("ghcr.io/apache/airflow/", "openlineage-e2e/"))

    try:
        console.print(f"[blue]Spinning up airflow environment using {DOCKER_IMAGE}")
        _E2ETestState.compose_instance = DockerCompose(
            tmp_dir, compose_file_name=compose_file_names, pull=pull
        )

        _E2ETestState.compose_instance.start()

        _E2ETestState.compose_instance.wait_for(f"http://{DOCKER_COMPOSE_HOST_PORT}/api/v2/monitor/health")
        # A reserialize imports the stub Dag files without the Dag processor's stub-task check, so the
        # first rows for a Lang-SDK Dag file would carry neither its probe record nor its import errors.
        # Leaving the files to the Dag processor makes its parse the first one, and AirflowClient.trigger_dag
        # already waits for a Dag to exist before it unpauses and triggers it.
        if E2E_TEST_MODE not in LANG_SDK_E2E_MODES:
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


@pytest.fixture(scope="session", autouse=True)
def _wait_until_stub_tasks_are_checked():
    """
    Wait until the Dag processor has probed a task handler artifact for every Lang-SDK Dag file.

    The stack is up before the Dag processor has parsed the Dag files with the artifacts in place, and the
    Lang-SDK modes skip ``airflow dags reserialize``, which would write stub Dags without the check having
    run on them. A test that triggers a Dag at once would then run ahead of the Dag processor. Waiting here
    turns a probe that never comes, or an import error nobody expected, into one message instead of a
    confusing test failure.
    """
    if E2E_TEST_MODE not in LANG_SDK_E2E_MODES:
        return
    assert _E2ETestState.airflow_logs_path is not None
    wait_until_stub_tasks_are_checked(
        AirflowClient(),
        _E2ETestState.airflow_logs_path,
        dag_files=_E2ETestState.lang_sdk_dag_files,
        expected_import_errors=_E2ETestState.expected_import_errors,
        timeout=300,
    )


@pytest.fixture(scope="session")
def compose_instance():
    """Provide access to the running Docker Compose instance."""
    return _E2ETestState.compose_instance


@pytest.fixture(scope="session")
def airflow_logs_path():
    """Live host path of the stack's task logs (bind-mounted), readable while tests run."""
    return _E2ETestState.airflow_logs_path


@pytest.fixture(scope="session")
def airflow_dags_path():
    """Host path of the dags served to the stack."""
    return _E2ETestState.airflow_dags_path


@pytest.fixture(scope="session")
def lang_sdk_dag_files():
    """The ``dags-folder`` files of the Lang SDK that the stack was given, in the Lang-SDK modes."""
    return _E2ETestState.lang_sdk_dag_files


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
