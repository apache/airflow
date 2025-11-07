#
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
"""
This module contains various unit tests for GCP Cloud Build Operators
"""

from __future__ import annotations

import json
import tempfile
from copy import deepcopy
from unittest import mock

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.devtools.cloudbuild_v1.types import Build, BuildTrigger, RepoSource, StorageSource

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.operators.cloud_build import (
    BuildProcessor,
    CloudBuildCancelBuildOperator,
    CloudBuildCreateBuildOperator,
    CloudBuildCreateBuildTriggerOperator,
    CloudBuildDeleteBuildTriggerOperator,
    CloudBuildGetBuildOperator,
    CloudBuildGetBuildTriggerOperator,
    CloudBuildListBuildsOperator,
    CloudBuildListBuildTriggersOperator,
    CloudBuildRetryBuildOperator,
    CloudBuildRunBuildTriggerOperator,
    CloudBuildUpdateBuildTriggerOperator,
)
from airflow.providers.google.cloud.triggers.cloud_build import CloudBuildCreateBuildTrigger
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "cloud-build-project"
CLOUD_BUILD_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook"
BUILD_ID = "test-build-id-9832661"
REPO_SOURCE = {"repo_source": {"repo_name": "test_repo", "branch_name": "main"}}
BUILD = {
    "source": REPO_SOURCE,
    "steps": [{"name": "gcr.io/cloud-builders/gcloud", "entrypoint": "/bin/sh", "args": ["-c", "ls"]}],
    "status": "SUCCESS",
}
BUILD_TRIGGER = {
    "name": "test-cloud-build-trigger",
    "trigger_template": {"project_id": PROJECT_ID, "repo_name": "test_repo", "branch_name": "master"},
    "filename": "cloudbuild.yaml",
}
OPERATION = {"metadata": {"build": {"id": BUILD_ID}}}
TRIGGER_ID = "32488e7f-09d6-4fe9-a5fb-4ca1419a6e7a"
TEST_BUILD_INSTANCE = dict(
    id="test-build-id-9832662",
    status=3,
    steps=[
        {
            "name": "ubuntu",
            "env": [],
            "args": [],
            "dir_": "",
            "id": "",
            "wait_for": [],
            "entrypoint": "",
            "secret_env": [],
            "volumes": [],
            "status": 0,
            "script": "",
        }
    ],
    name="",
    project_id="",
    status_detail="",
    images=[],
    logs_bucket="",
    build_trigger_id="",
    log_url="",
    substitutions={},
    tags=[],
    secrets=[],
    timing={},
    service_account="",
    warnings=[],
)


class TestCloudBuildOperator:
    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_cancel_build(self, mock_hook):
        mock_hook.return_value.cancel_build.return_value = Build()

        operator = CloudBuildCancelBuildOperator(id_=TRIGGER_ID, task_id="id")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.cancel_build.assert_called_once_with(
            id_=TRIGGER_ID, project_id=None, retry=DEFAULT, timeout=None, metadata=(), location="global"
        )

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_create_build(self, mock_hook):
        mock_hook.return_value.create_build_without_waiting_for_result.return_value = (BUILD, BUILD_ID)
        mock_hook.return_value.wait_for_operation.return_value = Build()

        operator = CloudBuildCreateBuildOperator(build=BUILD, task_id="id")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        build = Build(BUILD)
        mock_hook.return_value.create_build_without_waiting_for_result.assert_called_once_with(
            build=build, project_id=None, retry=DEFAULT, timeout=None, metadata=(), location="global"
        )
        mock_hook.return_value.wait_for_operation.assert_called_once_with(timeout=None, operation=BUILD)

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_create_build_with_missing_build(self, mock_hook):
        mock_hook.return_value.create_build_without_waiting_for_result.return_value = Build()
        with pytest.raises((TypeError, AirflowException), match="missing keyword argument 'build'"):
            CloudBuildCreateBuildOperator(task_id="id")

    @pytest.mark.parametrize(
        ("file_type", "file_content"),
        [
            (
                ".json",
                json.dumps({"steps": [{"name": "ubuntu", "args": ["echo", "Hello {{ params.name }}!"]}]}),
            ),
            (
                ".yaml",
                """
                steps:
                - name: 'ubuntu'
                  args: ['echo', 'Hello {{ params.name }}!']
                """,
            ),
        ],
    )
    def test_load_templated(self, file_type, file_content):
        with tempfile.NamedTemporaryFile(suffix=file_type, mode="w+") as f:
            f.writelines(file_content)
            f.flush()

            operator = CloudBuildCreateBuildOperator(
                build=f.name, task_id="task-id", params={"name": "airflow"}
            )
            operator.prepare_template()
            expected_body = {"steps": [{"name": "ubuntu", "args": ["echo", "Hello {{ params.name }}!"]}]}
            assert expected_body == operator.build

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_create_build_trigger(self, mock_hook):
        mock_hook.return_value.create_build_trigger.return_value = BuildTrigger()

        operator = CloudBuildCreateBuildTriggerOperator(trigger=BUILD_TRIGGER, task_id="id")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.create_build_trigger.assert_called_once_with(
            trigger=BUILD_TRIGGER,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
            location="global",
        )

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_delete_build_trigger(self, mock_hook):
        mock_hook.return_value.delete_build_trigger.return_value = None

        operator = CloudBuildDeleteBuildTriggerOperator(trigger_id=TRIGGER_ID, task_id="id")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.delete_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
            location="global",
        )

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_get_build(self, mock_hook):
        mock_hook.return_value.get_build.return_value = Build()

        operator = CloudBuildGetBuildOperator(id_=BUILD_ID, task_id="id")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.get_build.assert_called_once_with(
            id_=BUILD_ID, project_id=None, retry=DEFAULT, timeout=None, metadata=(), location="global"
        )

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_get_build_trigger(self, mock_hook):
        mock_hook.return_value.get_build_trigger.return_value = BuildTrigger()

        operator = CloudBuildGetBuildTriggerOperator(trigger_id=TRIGGER_ID, task_id="id")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.get_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
            location="global",
        )

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_list_build_triggers(self, mock_hook):
        mock_hook.return_value.list_build_triggers.return_value = mock.MagicMock()

        operator = CloudBuildListBuildTriggersOperator(task_id="id", location="global")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.list_build_triggers.assert_called_once_with(
            project_id=None,
            location="global",
            page_size=None,
            page_token=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_list_builds(self, mock_hook):
        mock_hook.return_value.list_builds.return_value = mock.MagicMock()

        operator = CloudBuildListBuildsOperator(task_id="id", location="global")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.list_builds.assert_called_once_with(
            project_id=None,
            location="global",
            page_size=None,
            filter_=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_retry_build(self, mock_hook):
        mock_hook.return_value.retry_build.return_value = Build()

        operator = CloudBuildRetryBuildOperator(id_=BUILD_ID, task_id="id")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.retry_build.assert_called_once_with(
            id_=BUILD_ID,
            project_id=None,
            wait=True,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
            location="global",
        )

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_run_build_trigger(self, mock_hook):
        mock_hook.return_value.run_build_trigger.return_value = Build()

        operator = CloudBuildRunBuildTriggerOperator(trigger_id=TRIGGER_ID, source=REPO_SOURCE, task_id="id")
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.run_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID,
            source=REPO_SOURCE,
            project_id=None,
            wait=True,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
            location="global",
        )

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_update_build_trigger(self, mock_hook):
        mock_hook.return_value.update_build_trigger.return_value = BuildTrigger()

        operator = CloudBuildUpdateBuildTriggerOperator(
            trigger_id=TRIGGER_ID, trigger=BUILD_TRIGGER, task_id="id"
        )
        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
        mock_hook.return_value.update_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID,
            trigger=BUILD_TRIGGER,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
            location="global",
        )


class TestBuildProcessor:
    def test_verify_source(self):
        error_message = r"The source could not be determined."
        with pytest.raises(AirflowException, match=error_message):
            BuildProcessor(build={"source": {"storage_source": {}, "repo_source": {}}}).process_body()

        with pytest.raises(AirflowException, match=error_message):
            BuildProcessor(build={"source": {}}).process_body()

    @pytest.mark.parametrize(
        ("url", "expected_dict"),
        [
            (
                "https://source.cloud.google.com/airflow-project/airflow-repo",
                {"project_id": "airflow-project", "repo_name": "airflow-repo", "branch_name": "master"},
            ),
            (
                "https://source.cloud.google.com/airflow-project/airflow-repo/+/branch-name:",
                {"project_id": "airflow-project", "repo_name": "airflow-repo", "branch_name": "branch-name"},
            ),
            (
                "https://source.cloud.google.com/airflow-project/airflow-repo/+/feature/branch:",
                {
                    "project_id": "airflow-project",
                    "repo_name": "airflow-repo",
                    "branch_name": "feature/branch",
                },
            ),
        ],
    )
    def test_convert_repo_url_to_dict_valid(self, url, expected_dict):
        body = {"source": {"repo_source": url}}
        body = BuildProcessor(build=body).process_body()
        assert body.source.repo_source == RepoSource(expected_dict)

    @pytest.mark.parametrize(
        "url",
        [
            "http://source.e.com/airflow-project/airflow-repo/branch-name",
            "httpXs://source.cloud.google.com/airflow-project/airflow-repo",
            "://source.cloud.google.com/airflow-project/airflow-repo",
        ],
    )
    def test_convert_repo_url_to_dict_invalid(self, url):
        body = {"source": {"repo_source": url}}
        with pytest.raises(AirflowException, match="Invalid URL."):
            BuildProcessor(build=body).process_body()

    @pytest.mark.parametrize(
        ("url", "expected_dict"),
        [
            (
                "gs://bucket-name/airflow-object.tar.gz",
                {"bucket": "bucket-name", "object_": "airflow-object.tar.gz"},
            ),
            (
                "gs://bucket-name/airflow-object.tar.gz#1231231",
                {"bucket": "bucket-name", "object_": "airflow-object.tar.gz", "generation": 1231231},
            ),
        ],
    )
    def test_convert_storage_url_to_dict_valid(self, url, expected_dict):
        body = {"source": {"storage_source": url}}
        body = BuildProcessor(build=body).process_body()
        assert body.source.storage_source == StorageSource(expected_dict)

    @pytest.mark.parametrize("url", ["///object", "gsXXa:///object", "gs://bucket-name/", "gs://bucket-name"])
    def test_convert_storage_url_to_dict_invalid(self, url):
        body = {"source": {"storage_source": url}}
        with pytest.raises(AirflowException, match="Invalid URL."):
            BuildProcessor(build=body).process_body()

    @pytest.mark.parametrize("source_key", ["storage_source", "repo_source"])
    def test_do_nothing(self, source_key):
        body = {"source": {source_key: {}}}
        expected_body = deepcopy(body)

        BuildProcessor(build=body).process_body()
        assert body == expected_body


def set_execute_complete(session, ti, **next_kwargs):
    ti.next_method = "execute_complete"
    ti.next_kwargs = next_kwargs
    session.flush()


@pytest.mark.db_test
@mock.patch(CLOUD_BUILD_HOOK_PATH)
def test_async_create_build_fires_correct_trigger_should_execute_successfully(
    mock_hook, create_task_instance_of_operator, session
):
    mock_hook.return_value.create_build_without_waiting_for_result.return_value = (BUILD, BUILD_ID)

    ti = create_task_instance_of_operator(
        build=BUILD,
        task_id="id",
        deferrable=True,
        operator_class=CloudBuildCreateBuildOperator,
        dag_id="cloud_build_create_build_job",
    )

    with pytest.raises(TaskDeferred) as exc:
        ti.task.execute({"ti": ti, "task_instance": ti})

    assert isinstance(exc.value.trigger, CloudBuildCreateBuildTrigger), (
        "Trigger is not a CloudBuildCreateBuildTrigger"
    )


@mock.patch(CLOUD_BUILD_HOOK_PATH)
def test_async_create_build_without_wait_should_execute_successfully(mock_hook):
    mock_hook.return_value.create_build_without_waiting_for_result.return_value = (BUILD, BUILD_ID)
    mock_hook.return_value.get_build.return_value = Build()

    operator = CloudBuildCreateBuildOperator(
        build=BUILD,
        task_id="id",
        wait=False,
        deferrable=True,
    )
    operator.execute(context=mock.MagicMock())

    mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None)
    build = Build(BUILD)
    mock_hook.return_value.create_build_without_waiting_for_result.assert_called_once_with(
        build=build, project_id=None, retry=DEFAULT, timeout=None, metadata=(), location="global"
    )
    mock_hook.return_value.get_build.assert_called_once_with(id_=BUILD_ID, project_id=None, location="global")


@pytest.mark.db_test
@mock.patch(CLOUD_BUILD_HOOK_PATH)
def test_async_create_build_correct_logging_should_execute_successfully(
    mock_hook, create_task_instance_of_operator, session
):
    mock_hook.return_value.create_build_without_waiting_for_result.return_value = (BUILD, BUILD_ID)
    mock_hook.return_value.get_build.return_value = Build()

    ti = create_task_instance_of_operator(
        build=BUILD,
        task_id="id",
        deferrable=True,
        operator_class=CloudBuildCreateBuildOperator,
        dag_id="cloud_build_create_build_logging",
    )

    with mock.patch.object(ti.task.log, "info") as mock_log_info:
        ti.task.execute_complete(
            context={"ti": ti, "task": ti.task},
            event={
                "instance": TEST_BUILD_INSTANCE,
                "status": "success",
                "message": "Build completed",
                "id_": BUILD_ID,
            },
        )

    mock_log_info.assert_called_with("Cloud Build completed with response %s ", "Build completed")


def test_async_create_build_error_event_should_throw_exception():
    operator = CloudBuildCreateBuildOperator(
        build=BUILD,
        task_id="id",
        deferrable=True,
    )
    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


@mock.patch(CLOUD_BUILD_HOOK_PATH)
def test_async_create_build_with_missing_build_should_throw_exception(mock_hook):
    mock_hook.return_value.create_build.return_value = Build()
    with pytest.raises((TypeError, AirflowException), match="missing keyword argument 'build'"):
        CloudBuildCreateBuildOperator(task_id="id")


@pytest.mark.parametrize(
    ("file_type", "file_content"),
    [
        (
            ".json",
            json.dumps({"steps": [{"name": "ubuntu", "args": ["echo", "Hello {{ params.name }}!"]}]}),
        ),
        (
            ".yaml",
            """
            steps:
            - name: 'ubuntu'
              args: ['echo', 'Hello {{ params.name }}!']
            """,
        ),
    ],
)
def test_async_load_templated_should_execute_successfully(file_type, file_content):
    with tempfile.NamedTemporaryFile(suffix=file_type, mode="w+") as f:
        f.writelines(file_content)
        f.flush()

        operator = CloudBuildCreateBuildOperator(
            build=f.name,
            task_id="task-id",
            params={"name": "airflow"},
            deferrable=True,
        )
        operator.prepare_template()
        expected_body = {"steps": [{"name": "ubuntu", "args": ["echo", "Hello {{ params.name }}!"]}]}
        assert expected_body == operator.build


def create_context(task):
    dag = DAG(dag_id="dag", schedule=None)
    logical_date = datetime(2022, 1, 1, 0, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        logical_date=logical_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, logical_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "logical_date": logical_date,
    }
