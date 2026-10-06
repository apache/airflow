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
"""Build the Java SDK example as a thin bundle and probe it for its task handlers."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import structlog

from airflow.dag_processing.processor import (
    TaskHandlerDeclaration,
    TaskHandlerParam,
    TaskHandlerParsingResult,
)
from airflow.dag_processing.task_handler_processor import LangSDKTaskHandlerProcessorProcess
from airflow.sdk.coordinators._subprocess import supports_task_handler_parsing
from airflow.sdk.execution_time import supervisor
from airflow.sdk.execution_time.coordinator import get_coordinator_manager, reset_coordinator_manager

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH
from unit.dag_processing.fake_task_handler_runtime import (
    LOCAL_BUNDLE,
    parse_dag_file,
    require_toolchain,
    task_handler_config,
)

if TYPE_CHECKING:
    from pathlib import Path

JAVA_SDK_PATH = AIRFLOW_ROOT_PATH / "java-sdk"

# The example's sources, built as a thin bundle against the SDK as an included build, so nothing is
# published. The plugin's fat mode looks the SDK up as a published airflow-sdk artifact.
_SETTINGS = """
pluginManagement {
    includeBuild("../java-sdk")
}
includeBuild("../java-sdk")
rootProject.name = "probe-example"
"""

_BUILD = """
plugins {
    id("org.apache.airflow.sdk")
}
repositories {
    mavenCentral()
}
dependencies {
    annotationProcessor("org.apache.airflow:processor")
    implementation("org.apache.airflow:sdk")
    implementation("org.apache.airflow:jpl")
}
sourceSets {
    main {
        java.srcDir("../java-sdk/example/src/java")
    }
}
airflowBundle {
    mainClass = "org.apache.airflow.example.ExampleBundleBuilder"
    fatJar = false
}
"""

pytestmark = pytest.mark.skipif(
    os.environ.get("AIRFLOW_LANG_SDK_REAL_PROBE_TESTS") != "1",
    reason="set AIRFLOW_LANG_SDK_REAL_PROBE_TESTS=1 to build and probe a real Java bundle",
)

INT32 = {"type": "integer", "format": "int32"}
INT64 = {"type": "integer", "format": "int64"}
FLOAT = {"type": "number", "format": "float"}
DOUBLE = {"type": "number", "format": "double"}
STRING = {"type": "string"}


def _make_nullable(schema: dict) -> dict:
    return {"anyOf": [schema, {"type": "null"}]}


def _declare_positional(task_id: str, *params: tuple[str, dict]) -> TaskHandlerDeclaration:
    return TaskHandlerDeclaration(
        task_id=task_id,
        binding="positional",
        params=[TaskHandlerParam(name=name, value_schema=schema) for name, schema in params],
    )


def _declare_named(task_id: str, *params: tuple[str, dict, bool]) -> TaskHandlerDeclaration:
    return TaskHandlerDeclaration(
        task_id=task_id,
        binding="named",
        params=[
            TaskHandlerParam(name=name, value_schema=schema, exact_name=exact)
            for name, schema, exact in params
        ],
    )


@pytest.fixture(scope="module")
def example_bundle(tmp_path_factory) -> Path:
    """Build ``java-sdk/example`` from a copy of the SDK sources, so the checkout gets no build output."""
    require_toolchain(None if JAVA_SDK_PATH.is_dir() else "the Java SDK sources are absent")
    require_toolchain(
        "needs a JDK" if shutil.which("java") is None or shutil.which("javac") is None else None
    )
    root = tmp_path_factory.mktemp("java-sdk")
    sdk = root / "java-sdk"
    shutil.copytree(JAVA_SDK_PATH, sdk, ignore=shutil.ignore_patterns("build", ".gradle", ".kotlin"))
    project = root / "probe-example"
    project.mkdir()
    (project / "settings.gradle").write_text(_SETTINGS)
    (project / "build.gradle").write_text(_BUILD)
    subprocess.run([sdk / "gradlew", "--no-daemon", "--quiet", "bundle"], cwd=project, check=True)
    return project / "build" / "bundle"


@pytest.fixture(autouse=True)
def _java_coordinator():
    spec = {"java": {"classpath": "airflow.sdk.coordinators.java.JavaCoordinator"}}
    reset_coordinator_manager()
    try:
        # A bare fork, so the parse child sees this config.
        with (
            conf_vars({("sdk", "coordinators"): json.dumps(spec)}),
            mock.patch.object(supervisor, "_should_use_exec", autospec=True, return_value=False),
        ):
            yield
    finally:
        reset_coordinator_manager()


def test_probes_the_task_handlers_of_a_built_java_bundle(example_bundle):
    coordinator = get_coordinator_manager().get_coordinator("java")
    artifact = coordinator._find_task_handler_artifact(
        bundle_path=example_bundle, dag_id="java_annotation_example"
    )
    assert supports_task_handler_parsing(artifact.schema_version)
    # The airflow-sdk JAR beside it sets the schema version; the example's own JAR sets the Main-Class.
    assert artifact.path == (example_bundle / "probe-example.jar").resolve()

    result = LangSDKTaskHandlerProcessorProcess.run(
        coordinator="java",
        path=artifact.path,
        bundle_path=example_bundle,
        bundle_name="java-task-handlers",
        artifact_rel_path=os.fspath(artifact.path.relative_to(example_bundle.resolve())),
        logger=structlog.get_logger(),
    )

    # The example also registers the native Dag java_native_interface_example, which is not a task handler.
    assert result == TaskHandlerParsingResult(
        fileloc=os.fspath(artifact.path),
        task_handlers={
            "java_interface_example": [
                _declare_named("extract"),
                _declare_named("transform", ("extracted", INT64, False)),
                _declare_named(
                    "summarize", ("region_code", _make_nullable(STRING), True), ("transformed", INT64, False)
                ),
            ],
            "java_annotation_example": [
                _declare_named("extract"),
                _declare_positional("transform", ("extracted", INT64)),
                _declare_positional("load", ("transformed", INT64)),
                _declare_named(
                    "report", ("runLabel", _make_nullable(STRING), False), ("transformed", INT64, False)
                ),
                _declare_named("concurrent"),
            ],
            "java_xcom_casting_example": [
                _declare_named("produce_number"),
                _declare_positional("widen_to_long", ("value", INT64)),
                _declare_positional("widen_to_double", ("value", DOUBLE)),
                _declare_named("produce_nothing"),
                _declare_positional("consume_nullable", ("value", _make_nullable(INT32))),
                _declare_named("produce_fraction"),
                _declare_positional("consume_float", ("value", FLOAT)),
                _declare_positional(
                    "consume_double_list",
                    ("values", _make_nullable({"type": "array", "items": _make_nullable(DOUBLE)})),
                ),
            ],
        },
    )
    assert list(result.task_handlers) == [
        "java_interface_example",
        "java_annotation_example",
        "java_xcom_casting_example",
    ]


def test_the_example_dags_match_the_built_bundle(example_bundle, cap_structlog):
    dag_file = JAVA_SDK_PATH / "example" / "src" / "resources" / "dags" / "java_examples.py"
    coordinators = {
        "java-jdk": {
            "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
            "kwargs": {"task_handler_bundle_name": "java-task-handlers"},
        }
    }
    bundles = [
        {"name": "dags", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(dag_file.parent)}},
        {
            "name": "java-task-handlers",
            "classpath": LOCAL_BUNDLE,
            "kwargs": {"path": os.fspath(example_bundle)},
        },
    ]

    with task_handler_config(
        dag_file.parent,
        example_bundle,
        coordinators,
        queue_to_coordinator={"java": "java-jdk"},
        bundles=bundles,
    ):
        result = parse_dag_file(dag_file)

    assert result.import_errors == {}
    assert {"event": "Probed a task handler artifact", "path": "probe-example.jar"} in cap_structlog
    assert not any(
        e["event"]
        in (
            "Dag's call passed argument(s) the task handler does not declare",
            "Task handler declares argument(s) the Dag's call did not pass",
            "Not checking a Dag's stub tasks against their task handlers",
        )
        for e in cap_structlog.entries
    )
