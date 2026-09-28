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
from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4
from zipfile import ZipFile

import pytest

from airflow._shared.timezones import timezone
from airflow.dag_processing.executor_importer import SdkDagParseRequest, import_sdk_definition
from airflow.dag_processing.executor_worker import ParsingAPIClient, compute_source_revision, parse_definition
from airflow.exceptions import AirflowClusterPolicySkipDag, AirflowClusterPolicyViolation
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.parsing import DagDefinitionAttempt, ParseDagDefinitions
from airflow.sdk.importers.base import FilesystemDagDefinition
from airflow.sdk.importers.python_importer import PythonDagImporter
from airflow.sdk.importers.zip_importer import ZipMemberDagDefinition

from tests_common.test_utils.config import conf_vars


@pytest.fixture(params=["file", "zip_member"])
def request_definition(request, tmp_path):
    source = (
        b"from airflow.sdk import DAG, task\n"
        b"with DAG('sdk_executor_import', schedule=None) as dag:\n"
        b"    @task\n"
        b"    def sample():\n"
        b"        return 1\n"
        b"    sample()\n"
    )
    if request.param == "zip_member":
        archive = tmp_path / "definitions.zip"
        with ZipFile(archive, "w") as stream:
            stream.writestr("nested/selected.py", source)
            stream.writestr("unselected.py", "raise RuntimeError('must not import another member')\n")
        values = {
            "relative_path": "definitions.zip/nested/selected.py",
            "archive_path": "definitions.zip",
            "archive_revision": compute_source_revision(archive),
        }
    else:
        (tmp_path / "selected.py").write_bytes(source)
        values = {"relative_path": "selected.py"}
    return SdkDagParseRequest(
        definition=DagDefinitionAttempt(
            attempt_id=uuid4(),
            source_revision=hashlib.sha256(source).hexdigest(),
            timeout_seconds=30,
            **values,
        ),
        bundle_path=tmp_path,
        bundle_name="sdk-test",
        include_source=True,
    )


def replace_source(request, source):
    attempt = request.definition
    if attempt.archive_path:
        archive = request.bundle_path / attempt.archive_path
        with ZipFile(archive, "w") as stream:
            stream.writestr("nested/selected.py", source)
        attempt.archive_revision = compute_source_revision(archive)
    else:
        (request.bundle_path / attempt.relative_path).write_text(source)
    attempt.source_revision = hashlib.sha256(source.encode()).hexdigest()


@patch("airflow.dag_processing.dagbag.DagBag.process_file", autospec=True, side_effect=AssertionError)
def test_uses_sdk_importer_and_serializes_inside_worker(legacy, request_definition, mocker):
    importer = mocker.spy(PythonDagImporter, "import_definition")
    result = import_sdk_definition(request_definition)
    assert not result.worker_error, result.diagnostics
    assert result.import_errors == {}
    assert len(result.serialized_dags) == 1
    data = result.serialized_dags[0].data["dag"]
    assert data["dag_id"] == "sdk_executor_import"
    assert data["relative_fileloc"] == request_definition.definition.relative_path
    assert result.source_code.startswith("from airflow.sdk import DAG")
    assert (
        hashlib.sha256(result.source_code.encode()).hexdigest()
        == request_definition.definition.source_revision
    )
    definition = importer.call_args.args[1]
    assert isinstance(
        definition,
        ZipMemberDagDefinition if request_definition.definition.archive_path else FilesystemDagDefinition,
    )
    legacy.assert_not_called()


@pytest.mark.parametrize("use_exec", [False, True])
@patch("airflow.sdk.execution_time.supervisor._should_use_exec", autospec=True)
def test_supervised_sdk_process(start_mode, use_exec, request_definition, tmp_path, monkeypatch):
    start_mode.return_value = use_exec
    monkeypatch.setenv("AIRFLOW_DAG_PARSING_POC_INCLUDE_SOURCE", "1")
    now = timezone.utcnow()
    workload = ParseDagDefinitions(
        workload_id=uuid4(),
        token="unused",
        bundle_info=BundleInfo(name=request_definition.bundle_name, version="v1"),
        definitions=(request_definition.definition,),
        start_deadline=now + timedelta(seconds=60),
        stop_deadline=now + timedelta(seconds=120),
    )
    with ParsingAPIClient(base_url="http://unused.invalid/", token="unused") as client:
        result = parse_definition(
            workload,
            workload.definitions[0],
            bundle_root=tmp_path,
            client=client,
            log_dir=tmp_path / "logs",
        )
    assert result.outcome == "success", result.model_dump()
    assert [dag["dag"]["dag_id"] for dag in result.serialized_dags] == ["sdk_executor_import"]
    assert result.source_code is not None


@patch("airflow.sdk.importers.python_importer.PythonDagImporter.import_definition", autospec=True)
def test_rejects_changed_definition_before_import(importer, request_definition):
    request_definition.definition.source_revision = "wrong-revision"
    result = import_sdk_definition(request_definition)
    assert result.worker_error
    assert result.serialized_dags == []
    assert "revision" in result.diagnostics[0]
    importer.assert_not_called()


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("from airflow.sdk import DAG\nraise ValueError('broken SDK import')\n", "broken SDK import"),
        (
            "from airflow.sdk import DAG, task\n"
            "with DAG('cycle', schedule=None):\n"
            "    @task\n"
            "    def sample(): pass\n"
            "    first = sample()\n"
            "    second = sample()\n"
            "    first >> second >> first\n",
            "cycle",
        ),
    ],
)
def test_sdk_errors_and_validation_preserve_definition_identity(request_definition, source, expected):
    attempt = request_definition.definition
    replace_source(request_definition, source)
    result = import_sdk_definition(request_definition)
    assert not result.worker_error, result.diagnostics
    assert result.serialized_dags == []
    assert set(result.import_errors) == {attempt.relative_path}
    assert expected.lower() in result.import_errors[attempt.relative_path].lower()


@patch("airflow.settings.dag_policy", autospec=True)
@pytest.mark.parametrize("skip", [False, True])
def test_sdk_import_preserves_cluster_policy(policy, skip, request_definition):
    policy.side_effect = AirflowClusterPolicySkipDag() if skip else AirflowClusterPolicyViolation("rejected")
    result = import_sdk_definition(request_definition)
    policy.assert_called_once()
    assert not result.worker_error, result.diagnostics
    assert result.serialized_dags == []
    assert bool(result.import_errors) is not skip


@conf_vars({("dag_processor", "dag_version_inflation_check_level"): "off"})
def test_import_warnings_are_preserved_as_diagnostics(request_definition):
    replace_source(
        request_definition,
        "import warnings\nfrom airflow.sdk import DAG\n"
        "warnings.warn('SDK diagnostic', UserWarning)\ndag = DAG('warning', schedule=None)\n",
    )
    result = import_sdk_definition(request_definition)
    assert not result.worker_error
    assert result.import_errors == {}
    assert "UserWarning: SDK diagnostic" in result.diagnostics


@conf_vars({("dag_processor", "dag_version_inflation_check_level"): "error"})
@patch("airflow.sdk.importers.python_importer.PythonDagImporter.import_definition", autospec=True)
def test_stability_check_runs_before_sdk_import(importer, request_definition):
    replace_source(
        request_definition,
        "import uuid\nfrom airflow.sdk import DAG\ndag = DAG(str(uuid.uuid4()), schedule=None)\n",
    )
    result = import_sdk_definition(request_definition)
    assert not result.worker_error
    assert set(result.import_errors) == {request_definition.definition.relative_path}
    importer.assert_not_called()


def test_source_change_during_sdk_import_discards_output(request_definition, mocker):
    original = PythonDagImporter.import_definition

    def mutate(importer, definition, bundle):
        result = original(importer, definition, bundle)
        path = request_definition.bundle_path / (
            request_definition.definition.archive_path or request_definition.definition.relative_path
        )
        with path.open("ab") as stream:
            stream.write(b"changed")
        return result

    mocker.patch.object(PythonDagImporter, "import_definition", autospec=True, side_effect=mutate)
    result = import_sdk_definition(request_definition)
    assert result.worker_error
    assert result.serialized_dags == []
    assert result.source_code is None
    assert "revision" in result.diagnostics[0]


@pytest.mark.parametrize(
    ("source", "outcome"),
    [("import time\ntime.sleep(30)\n", "timeout"), ("import os\nos._exit(9)\n", "worker_error")],
)
def test_supervisor_handles_sdk_timeout_and_exit(request_definition, tmp_path, source, outcome):
    replace_source(request_definition, "from airflow.sdk import DAG\n" + source)
    request_definition.definition.timeout_seconds = 1
    now = timezone.utcnow()
    workload = ParseDagDefinitions(
        workload_id=uuid4(),
        token="unused",
        bundle_info=BundleInfo(name="sdk-test", version="v1"),
        definitions=(request_definition.definition,),
        start_deadline=now + timedelta(seconds=60),
        stop_deadline=now + timedelta(seconds=120),
    )
    with ParsingAPIClient(base_url="http://unused.invalid/", token="unused") as client:
        result = parse_definition(
            workload, workload.definitions[0], bundle_root=tmp_path, client=client, log_dir=tmp_path / "logs"
        )
    assert result.outcome == outcome, result.model_dump()
    assert result.serialized_dags == []


def test_zip_member_can_import_sibling_without_parsing_other_members(tmp_path):
    helper = f"helper_{uuid4().hex}"
    source = f"from airflow.sdk import DAG\nfrom {helper} import NAME\ndag = DAG(NAME, schedule=None)\n"
    archive = tmp_path / "definitions.zip"
    with ZipFile(archive, "w") as stream:
        stream.writestr("selected.py", source)
        stream.writestr(f"{helper}.py", "NAME = 'sdk_zip_sibling'\n")
        stream.writestr("unselected.py", "raise RuntimeError('not this member')\n")
    request = SdkDagParseRequest(
        definition=DagDefinitionAttempt(
            attempt_id=uuid4(),
            relative_path="definitions.zip/selected.py",
            archive_path="definitions.zip",
            archive_revision=compute_source_revision(archive),
            source_revision=hashlib.sha256(source.encode()).hexdigest(),
            timeout_seconds=10,
        ),
        bundle_path=tmp_path,
        bundle_name="sdk-test",
    )
    result = import_sdk_definition(request)
    assert not result.worker_error, result.diagnostics
    assert result.import_errors == {}
    assert result.serialized_dags[0].data["dag"]["dag_id"] == "sdk_zip_sibling"
    with ZipFile(archive, "a") as stream:
        stream.writestr("new_dependency.py", "NEW = True\n")
    changed = import_sdk_definition(request)
    assert changed.worker_error
    assert "revision" in changed.diagnostics[0]


def test_user_code_runs_in_child(tmp_path, monkeypatch):
    monkeypatch.setenv("AIRFLOW_DAG_PARSING_POC_INCLUDE_SOURCE", "1")
    # The real supervised test above covers both launch modes; this marker detects in-process importing.
    source = (
        f"from pathlib import Path\nimport os\nPath({str(tmp_path / 'pid')!r}).write_text(str(os.getpid()))\n"
    )
    source += "from airflow.sdk import DAG\ndag = DAG('child_only', schedule=None)\n"
    (tmp_path / "pid.py").write_text(source)
    now = timezone.utcnow()
    workload = ParseDagDefinitions(
        workload_id=uuid4(),
        token="unused",
        bundle_info=BundleInfo(name="sdk-test", version="v1"),
        definitions=(
            DagDefinitionAttempt(
                attempt_id=uuid4(),
                relative_path="pid.py",
                source_revision=hashlib.sha256(source.encode()).hexdigest(),
                timeout_seconds=10,
            ),
        ),
        start_deadline=now + timedelta(seconds=60),
        stop_deadline=now + timedelta(seconds=120),
    )
    with ParsingAPIClient(base_url="http://unused.invalid/", token="unused") as client:
        result = parse_definition(
            workload, workload.definitions[0], bundle_root=tmp_path, client=client, log_dir=tmp_path / "logs"
        )
    assert result.outcome == "success", result.model_dump()
    assert int((tmp_path / "pid").read_text()) != os.getpid()
