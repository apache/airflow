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
from __future__ import annotations

import copy
import unittest
from copy import deepcopy
from unittest import mock

import pytest as pytest

import airflow
from airflow.providers.google.cloud.operators.dataflow import (
    CheckJobRunning,
    DataflowCreateJavaJobOperator,
    DataflowCreatePythonJobOperator,
    DataflowStartFlexTemplateOperator,
    DataflowStartSqlJobOperator,
    DataflowStopJobOperator,
    DataflowTemplatedJobStartOperator,
)
from airflow.version import version

TASK_ID = "test-dataflow-operator"
JOB_ID = "test-dataflow-pipeline-id"
JOB_NAME = "test-dataflow-pipeline-name"
TEMPLATE = "gs://dataflow-templates/wordcount/template_file"
PARAMETERS = {
    "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
    "output": "gs://test/output/my_output",
}
PY_FILE = "gs://my-bucket/my-object.py"
PY_INTERPRETER = "python3"
JAR_FILE = "gs://my-bucket/example/test.jar"
LOCAL_JAR_FILE = "/mnt/dev/example/test.jar"
JOB_CLASS = "com.test.NotMain"
PY_OPTIONS = ["-m"]
DEFAULT_OPTIONS_PYTHON = DEFAULT_OPTIONS_JAVA = {
    "project": "test",
    "stagingLocation": "gs://test/staging",
}
DEFAULT_OPTIONS_TEMPLATE = {
    "project": "test",
    "stagingLocation": "gs://test/staging",
    "tempLocation": "gs://test/temp",
    "zone": "us-central1-f",
}
ADDITIONAL_OPTIONS = {"output": "gs://test/output", "labels": {"foo": "bar"}}
TEST_VERSION = f"v{version.replace('.', '-').replace('+', '-')}"
EXPECTED_ADDITIONAL_OPTIONS = {
    "output": "gs://test/output",
    "labels": {"foo": "bar", "airflow-version": TEST_VERSION},
}
POLL_SLEEP = 30
GCS_HOOK_STRING = "airflow.providers.google.cloud.operators.dataflow.{}"
TEST_FLEX_PARAMETERS = {
    "containerSpecGcsPath": "gs://test-bucket/test-file",
    "jobName": "test-job-name",
    "parameters": {
        "inputSubscription": "test-subscription",
        "outputTable": "test-project:test-dataset.streaming_beam_sql",
    },
}
TEST_LOCATION = "custom-location"
TEST_PROJECT = "test-project"
TEST_SQL_JOB_NAME = "test-sql-job-name"
TEST_DATASET = "test-dataset"
TEST_SQL_OPTIONS = {
    "bigquery-project": TEST_PROJECT,
    "bigquery-dataset": TEST_DATASET,
    "bigquery-table": "beam_output",
    "bigquery-write-disposition": "write-truncate",
}
TEST_SQL_QUERY = """
SELECT
    sales_region as sales_region,
    count(state_id) as count_state
FROM
    bigquery.table.test-project.beam_samples.beam_table
GROUP BY sales_region;
"""
TEST_SQL_JOB = {"id": "test-job-id"}
GCP_CONN_ID = "test_gcp_conn_id"
DELEGATE_TO = "delegating_to_something"
IMPERSONATION_CHAIN = ["impersonate", "this"]
CANCEL_TIMEOUT = 10 * 420


class TestDataflowPythonOperator(unittest.TestCase):
    def setUp(self):
        self.dataflow = DataflowCreatePythonJobOperator(
            task_id=TASK_ID,
            py_file=PY_FILE,
            job_name=JOB_NAME,
            py_options=PY_OPTIONS,
            dataflow_default_options=DEFAULT_OPTIONS_PYTHON,
            options=ADDITIONAL_OPTIONS,
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
        )
        self.expected_airflow_version = "v" + airflow.version.version.replace(".", "-").replace("+", "-")

    def test_init(self):
        """Test DataFlowPythonOperator instance is properly initialized."""
        assert self.dataflow.task_id == TASK_ID
        assert self.dataflow.job_name == JOB_NAME
        assert self.dataflow.py_file == PY_FILE
        assert self.dataflow.py_options == PY_OPTIONS
        assert self.dataflow.py_interpreter == PY_INTERPRETER
        assert self.dataflow.poll_sleep == POLL_SLEEP
        assert self.dataflow.dataflow_default_options == DEFAULT_OPTIONS_PYTHON
        assert self.dataflow.options == EXPECTED_ADDITIONAL_OPTIONS

    @mock.patch(
        "airflow.providers.google.cloud.operators.dataflow.process_line_and_extract_dataflow_job_id_callback"
    )
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.BeamHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.GCSHook")
    def test_exec(self, gcs_hook, dataflow_hook_mock, beam_hook_mock, mock_callback_on_job_id):
        """Test DataflowHook is created and the right args are passed to
        start_python_workflow.

        """
        start_python_mock = beam_hook_mock.return_value.start_python_pipeline
        provide_gcloud_mock = dataflow_hook_mock.return_value.provide_authorized_gcloud
        gcs_provide_file = gcs_hook.return_value.provide_file
        job_name = dataflow_hook_mock.return_value.build_dataflow_job_name.return_value
        self.dataflow.execute(None)
        beam_hook_mock.assert_called_once_with(runner="DataflowRunner")
        self.assertTrue(self.dataflow.py_file.startswith("/tmp/dataflow"))
        gcs_provide_file.assert_called_once_with(object_url=PY_FILE)
        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback=mock.ANY)
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            delegate_to=mock.ANY,
            poll_sleep=POLL_SLEEP,
            impersonation_chain=None,
            drain_pipeline=False,
            cancel_timeout=mock.ANY,
            wait_until_finished=None,
        )
        expected_options = {
            "project": dataflow_hook_mock.return_value.project_id,
            "staging_location": "gs://test/staging",
            "job_name": job_name,
            "region": TEST_LOCATION,
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": self.expected_airflow_version},
        }
        start_python_mock.assert_called_once_with(
            variables=expected_options,
            py_file=gcs_provide_file.return_value.__enter__.return_value.name,
            py_options=PY_OPTIONS,
            py_interpreter=PY_INTERPRETER,
            py_requirements=None,
            py_system_site_packages=False,
            process_line_callback=mock_callback_on_job_id.return_value,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=mock.ANY,
            job_name=job_name,
            location=TEST_LOCATION,
            multiple_jobs=False,
        )
        assert self.dataflow.py_file.startswith("/tmp/dataflow")
        provide_gcloud_mock.assert_called_once_with()


class TestDataflowJavaOperator(unittest.TestCase):
    def setUp(self):
        self.dataflow = DataflowCreateJavaJobOperator(
            task_id=TASK_ID,
            jar=JAR_FILE,
            job_name=JOB_NAME,
            job_class=JOB_CLASS,
            dataflow_default_options=DEFAULT_OPTIONS_JAVA,
            options=ADDITIONAL_OPTIONS,
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
        )
        self.expected_airflow_version = "v" + airflow.version.version.replace(".", "-").replace("+", "-")

    def test_init(self):
        """Test DataflowTemplateOperator instance is properly initialized."""
        assert self.dataflow.task_id == TASK_ID
        assert self.dataflow.job_name == JOB_NAME
        assert self.dataflow.poll_sleep == POLL_SLEEP
        assert self.dataflow.dataflow_default_options == DEFAULT_OPTIONS_JAVA
        assert self.dataflow.job_class == JOB_CLASS
        assert self.dataflow.jar == JAR_FILE
        assert self.dataflow.options == EXPECTED_ADDITIONAL_OPTIONS
        assert self.dataflow.check_if_running == CheckJobRunning.WaitForRun

    @mock.patch(
        "airflow.providers.google.cloud.operators.dataflow.process_line_and_extract_dataflow_job_id_callback"
    )
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.BeamHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.GCSHook")
    def test_exec(self, gcs_hook, dataflow_hook_mock, beam_hook_mock, mock_callback_on_job_id):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow.

        """
        start_java_mock = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        job_name = dataflow_hook_mock.return_value.build_dataflow_job_name.return_value
        provide_gcloud_mock = dataflow_hook_mock.return_value.provide_authorized_gcloud
        self.dataflow.check_if_running = CheckJobRunning.IgnoreJob

        self.dataflow.execute(None)

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback=mock.ANY)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        expected_variables = {
            "project": dataflow_hook_mock.return_value.project_id,
            "stagingLocation": "gs://test/staging",
            "jobName": job_name,
            "region": TEST_LOCATION,
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": self.expected_airflow_version},
        }

        start_java_mock.assert_called_once_with(
            variables=expected_variables,
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=mock.ANY,
            job_name=job_name,
            location=TEST_LOCATION,
            multiple_jobs=False,
        )

        provide_gcloud_mock.assert_called_once_with()

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.BeamHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.GCSHook")
    def test_check_job_running_exec(self, gcs_hook, dataflow_mock, beam_hook_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow.

        """
        dataflow_running = dataflow_mock.return_value.is_job_dataflow_running
        dataflow_running.return_value = True
        start_java_hook = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.check_if_running = True

        self.dataflow.execute(None)

        self.assertTrue(dataflow_mock.called)
        start_java_hook.assert_not_called()
        gcs_provide_file.assert_called_once()
        variables = {
            "project": dataflow_mock.return_value.project_id,
            "stagingLocation": "gs://test/staging",
            "jobName": JOB_NAME,
            "region": TEST_LOCATION,
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": self.expected_airflow_version},
        }
        dataflow_running.assert_called_once_with(name=JOB_NAME, variables=variables)

    @mock.patch(
        "airflow.providers.google.cloud.operators.dataflow.process_line_and_extract_dataflow_job_id_callback"
    )
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.BeamHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.GCSHook")
    def test_check_job_not_running_exec(
        self, gcs_hook, dataflow_hook_mock, beam_hook_mock, mock_callback_on_job_id
    ):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow with option to check if job is running
        """
        is_job_dataflow_running_variables = None

        def set_is_job_dataflow_running_variables(*args, **kwargs):
            nonlocal is_job_dataflow_running_variables
            is_job_dataflow_running_variables = copy.deepcopy(kwargs.get("variables"))

        dataflow_running = dataflow_hook_mock.return_value.is_job_dataflow_running
        dataflow_running.side_effect = set_is_job_dataflow_running_variables
        dataflow_running.return_value = False
        start_java_mock = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.check_if_running = True

        self.dataflow.execute(None)

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback=mock.ANY)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        expected_variables = {
            "project": dataflow_hook_mock.return_value.project_id,
            "stagingLocation": "gs://test/staging",
            "jobName": JOB_NAME,
            "region": TEST_LOCATION,
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": self.expected_airflow_version},
        }
        self.assertEqual(expected_variables, is_job_dataflow_running_variables)
        job_name = dataflow_hook_mock.return_value.build_dataflow_job_name.return_value
        expected_variables["jobName"] = job_name
        start_java_mock.assert_called_once_with(
            variables=expected_variables,
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=mock.ANY,
            job_name=job_name,
            location=TEST_LOCATION,
            multiple_jobs=False,
        )

    @mock.patch(
        "airflow.providers.google.cloud.operators.dataflow.process_line_and_extract_dataflow_job_id_callback"
    )
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.BeamHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.GCSHook")
    def test_check_multiple_job_exec(
        self, gcs_hook, dataflow_hook_mock, beam_hook_mock, mock_callback_on_job_id
    ):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow with option to check if job is running
        """
        is_job_dataflow_running_variables = None

        def set_is_job_dataflow_running_variables(*args, **kwargs):
            nonlocal is_job_dataflow_running_variables
            is_job_dataflow_running_variables = copy.deepcopy(kwargs.get("variables"))

        dataflow_running = dataflow_hook_mock.return_value.is_job_dataflow_running
        dataflow_running.side_effect = set_is_job_dataflow_running_variables
        dataflow_running.return_value = False
        start_java_mock = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.check_if_running = True
        self.dataflow.multiple_jobs = True

        self.dataflow.execute(None)

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback=mock.ANY)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        expected_variables = {
            "project": dataflow_hook_mock.return_value.project_id,
            "stagingLocation": "gs://test/staging",
            "jobName": JOB_NAME,
            "region": TEST_LOCATION,
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": self.expected_airflow_version},
        }
        self.assertEqual(expected_variables, is_job_dataflow_running_variables)
        job_name = dataflow_hook_mock.return_value.build_dataflow_job_name.return_value
        expected_variables["jobName"] = job_name
        start_java_mock.assert_called_once_with(
            variables=expected_variables,
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=mock.ANY,
            job_name=job_name,
            location=TEST_LOCATION,
            multiple_jobs=True,
        )


class TestDataflowJavaOperatorWithLocal(unittest.TestCase):
    def setUp(self):
        self.dataflow = DataflowCreateJavaJobOperator(
            task_id=TASK_ID,
            jar=LOCAL_JAR_FILE,
            job_name=JOB_NAME,
            job_class=JOB_CLASS,
            dataflow_default_options=DEFAULT_OPTIONS_JAVA,
            options=ADDITIONAL_OPTIONS,
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
        )
        self.expected_airflow_version = "v" + airflow.version.version.replace(".", "-").replace("+", "-")

    def test_init(self):
        """Test DataflowTemplateOperator instance is properly initialized."""
        assert self.dataflow.jar == LOCAL_JAR_FILE

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.BeamHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_check_job_not_running_exec(self, dataflow_hook_mock, beam_hook_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow with option to check if job is running
        """
        is_job_dataflow_running_variables = None

        def set_is_job_dataflow_running_variables(*args, **kwargs):
            nonlocal is_job_dataflow_running_variables
            is_job_dataflow_running_variables = copy.deepcopy(kwargs.get("variables"))

        dataflow_running = dataflow_hook_mock.return_value.is_job_dataflow_running
        dataflow_running.side_effect = set_is_job_dataflow_running_variables
        dataflow_running.return_value = False
        start_java_mock = beam_hook_mock.return_value.start_java_pipeline
        self.dataflow.check_if_running = True

        self.dataflow.execute(None)
        expected_variables = {
            "project": dataflow_hook_mock.return_value.project_id,
            "stagingLocation": "gs://test/staging",
            "jobName": JOB_NAME,
            "region": TEST_LOCATION,
            "output": "gs://test/output",
            "labels": {"foo": "bar", "airflow-version": self.expected_airflow_version},
        }
        self.assertEqual(expected_variables, is_job_dataflow_running_variables)
        job_name = dataflow_hook_mock.return_value.build_dataflow_job_name.return_value
        expected_variables["jobName"] = job_name
        start_java_mock.assert_called_once_with(
            variables=expected_variables,
            jar=LOCAL_JAR_FILE,
            job_class=JOB_CLASS,
            process_line_callback=mock.ANY,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=mock.ANY,
            job_name=job_name,
            location=TEST_LOCATION,
            multiple_jobs=False,
        )


class TestDataflowTemplateOperator:
    @pytest.fixture
    def sync_operator(self):
        return DataflowTemplatedJobStartOperator(
            project_id=TEST_PROJECT,
            task_id=TASK_ID,
            template=TEMPLATE,
            job_name=JOB_NAME,
            parameters=PARAMETERS,
            options=DEFAULT_OPTIONS_TEMPLATE,
            dataflow_default_options={"EXTRA_OPTION": "TEST_A"},
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
            environment={"maxWorkers": 2},
        )

    @pytest.fixture
    def deferrable_operator(self):
        return DataflowTemplatedJobStartOperator(
            project_id=TEST_PROJECT,
            task_id=TASK_ID,
            template=TEMPLATE,
            job_name=JOB_NAME,
            parameters=PARAMETERS,
            options=DEFAULT_OPTIONS_TEMPLATE,
            dataflow_default_options={"EXTRA_OPTION": "TEST_A"},
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
            environment={"maxWorkers": 2},
            deferrable=True,
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
            cancel_timeout=CANCEL_TIMEOUT,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_exec(self, dataflow_mock, sync_operator):
        start_template_hook = dataflow_mock.return_value.start_template_dataflow
        sync_operator.execute(None)
        assert dataflow_mock.called
        expected_options = {
            "project": "test",
            "stagingLocation": "gs://test/staging",
            "tempLocation": "gs://test/temp",
            "zone": "us-central1-f",
            "EXTRA_OPTION": "TEST_A",
        }
        start_template_hook.assert_called_once_with(
            job_name=JOB_NAME,
            variables=expected_options,
            parameters=PARAMETERS,
            dataflow_template=TEMPLATE,
            on_new_job_callback=mock.ANY,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
            environment={"maxWorkers": 2},
            append_job_name=True,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator.defer")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator.hook")
    def test_execute_with_deferrable_mode(self, mock_hook, mock_defer_method, deferrable_operator):
        deferrable_operator.execute(mock.MagicMock())
        mock_defer_method.assert_called_once()

    def test_validation_deferrable_params_raises_error(self):
        init_kwargs = {
            "project_id": TEST_PROJECT,
            "task_id": TASK_ID,
            "template": TEMPLATE,
            "job_name": JOB_NAME,
            "parameters": PARAMETERS,
            "options": DEFAULT_OPTIONS_TEMPLATE,
            "dataflow_default_options": {"EXTRA_OPTION": "TEST_A"},
            "poll_sleep": POLL_SLEEP,
            "location": TEST_LOCATION,
            "environment": {"maxWorkers": 2},
            "wait_until_finished": True,
            "deferrable": True,
            "gcp_conn_id": GCP_CONN_ID,
            "delegate_to": DELEGATE_TO,
            "impersonation_chain": IMPERSONATION_CHAIN,
            "cancel_timeout": CANCEL_TIMEOUT,
        }
        with pytest.raises(ValueError):
            DataflowTemplatedJobStartOperator(**init_kwargs)


class TestDataflowStartFlexTemplateOperator:
    @pytest.fixture
    def sync_operator(self):
        return DataflowStartFlexTemplateOperator(
            task_id="start_flex_template_streaming_beam_sql",
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            do_xcom_push=True,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
        )

    @pytest.fixture
    def deferrable_operator(self):
        return DataflowStartFlexTemplateOperator(
            task_id="start_flex_template_streaming_beam_sql",
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            do_xcom_push=True,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
            deferrable=True,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_execute(self, mock_dataflow, sync_operator):
        sync_operator.execute(mock.MagicMock())
        mock_dataflow.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            delegate_to=None,
            drain_pipeline=False,
            cancel_timeout=600,
            wait_until_finished=None,
            impersonation_chain=None,
        )
        mock_dataflow.return_value.start_flex_template.assert_called_once_with(
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            location=TEST_LOCATION,
            project_id=TEST_PROJECT,
            on_new_job_callback=mock.ANY,
        )

    def test_on_kill(self, sync_operator):
        sync_operator.hook = mock.MagicMock()
        sync_operator.job = {"id": JOB_ID, "projectId": TEST_PROJECT, "location": TEST_LOCATION}
        sync_operator.on_kill()
        sync_operator.hook.cancel_job.assert_called_once_with(
            job_id="test-dataflow-pipeline-id", project_id=TEST_PROJECT, location=TEST_LOCATION
        )

    def test_validation_deferrable_params_raises_error(self):
        init_kwargs = {
            "task_id": "start_flex_template_streaming_beam_sql",
            "body": {"launchParameter": TEST_FLEX_PARAMETERS},
            "do_xcom_push": True,
            "location": TEST_LOCATION,
            "project_id": TEST_PROJECT,
            "wait_until_finished": True,
            "deferrable": True,
        }
        with pytest.raises(ValueError):
            DataflowStartFlexTemplateOperator(**init_kwargs)

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowStartFlexTemplateOperator.defer")
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_execute_with_deferrable_mode(self, mock_hook, mock_defer_method, deferrable_operator):
        deferrable_operator.execute(mock.MagicMock())

        mock_hook.return_value.start_flex_template.assert_called_once_with(
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            location=TEST_LOCATION,
            project_id=TEST_PROJECT,
            on_new_job_callback=mock.ANY,
        )
        mock_defer_method.assert_called_once()


class TestDataflowSqlOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_execute(self, mock_hook):
        start_sql = DataflowStartSqlJobOperator(
            task_id="start_sql_query",
            job_name=TEST_SQL_JOB_NAME,
            query=TEST_SQL_QUERY,
            options=deepcopy(TEST_SQL_OPTIONS),
            location=TEST_LOCATION,
            do_xcom_push=True,
        )

        start_sql.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            delegate_to=None,
            drain_pipeline=False,
            impersonation_chain=None,
        )
        mock_hook.return_value.start_sql_job.assert_called_once_with(
            job_name=TEST_SQL_JOB_NAME,
            query=TEST_SQL_QUERY,
            options=TEST_SQL_OPTIONS,
            location=TEST_LOCATION,
            project_id=None,
            on_new_job_callback=mock.ANY,
        )
        start_sql.job = TEST_SQL_JOB
        start_sql.on_kill()
        mock_hook.return_value.cancel_job.assert_called_once_with(
            job_id="test-job-id", project_id=None, location=None
        )


class TestDataflowStopJobOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_exec_job_id(self, dataflow_mock):
        self.dataflow = DataflowStopJobOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            job_id=JOB_ID,
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
        )
        """
        Test DataflowHook is created and the right args are passed to cancel_job.
        """
        cancel_job_hook = dataflow_mock.return_value.cancel_job
        self.dataflow.execute(None)
        assert dataflow_mock.called
        cancel_job_hook.assert_called_once_with(
            job_name=None,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
            job_id=JOB_ID,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_exec_job_name_prefix(self, dataflow_mock):
        self.dataflow = DataflowStopJobOperator(
            task_id=TASK_ID,
            project_id=TEST_PROJECT,
            job_name_prefix=JOB_NAME,
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
        )
        """
        Test DataflowHook is created and the right args are passed to cancel_job
        and is_job_dataflow_running.
        """
        is_job_running_hook = dataflow_mock.return_value.is_job_dataflow_running
        cancel_job_hook = dataflow_mock.return_value.cancel_job
        self.dataflow.execute(None)
        assert dataflow_mock.called
        is_job_running_hook.assert_called_once_with(
            name=JOB_NAME,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
        )
        cancel_job_hook.assert_called_once_with(
            job_name=JOB_NAME,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
            job_id=None,
        )
