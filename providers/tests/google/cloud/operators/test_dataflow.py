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
import warnings
from copy import deepcopy
from unittest import mock

import httplib2
import pytest
from googleapiclient.errors import HttpError

import airflow
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.dataflow import (
    DEFAULT_DATAFLOW_LOCATION,
    DataflowJobStatus,
)
from airflow.providers.google.cloud.operators.dataflow import (
    CheckJobRunning,
    DataflowCreateJavaJobOperator,
    DataflowCreatePipelineOperator,
    DataflowCreatePythonJobOperator,
    DataflowDeletePipelineOperator,
    DataflowRunPipelineOperator,
    DataflowStartFlexTemplateOperator,
    DataflowStartSqlJobOperator,
    DataflowStartYamlJobOperator,
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
TEST_REGION = "custom-region"
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
IMPERSONATION_CHAIN = ["impersonate", "this"]
CANCEL_TIMEOUT = 10 * 420
DATAFLOW_PATH = "airflow.providers.google.cloud.operators.dataflow"

TEST_PIPELINE_NAME = "test_data_pipeline_name"
TEST_PIPELINE_BODY = {
    "name": f"projects/test-datapipeline-operators/locations/test-location/pipelines/{TEST_PIPELINE_NAME}",
    "type": "PIPELINE_TYPE_BATCH",
    "workload": {
        "dataflowFlexTemplateRequest": {
            "launchParameter": {
                "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/Word_Count_metadata",
                "jobName": "test-job",
                "environment": {"tempLocation": "test-temp-location"},
                "parameters": {
                    "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
                    "output": "gs://test/output/my_output",
                },
            },
            "projectId": f"{TEST_PROJECT}",
            "location": f"{TEST_LOCATION}",
        }
    },
}


class TestDataflowCreatePythonJobOperator:
    def setup_method(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
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
        """Test DataflowCreatePythonJobOperator instance is properly initialized."""
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
        assert self.dataflow.py_file.startswith("/tmp/dataflow")
        gcs_provide_file.assert_called_once_with(object_url=PY_FILE)
        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback=mock.ANY)
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
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


class TestDataflowCreateJavaJobOperator:
    def setup_method(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
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
        """Test DataflowCreateJavaJobOperator instance is properly initialized."""
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

        assert dataflow_mock.called
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
        dataflow_running.assert_called_once_with(name=JOB_NAME, variables=variables, location=TEST_LOCATION)

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
        assert expected_variables == is_job_dataflow_running_variables
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
        assert expected_variables == is_job_dataflow_running_variables
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


class TestDataflowCreateJavaJobOperatorWithLocal:
    def setup_method(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
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
        """Test DataflowCreateJavaJobOperator instance is properly initialized."""
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
        assert expected_variables == is_job_dataflow_running_variables
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


class TestDataflowTemplatedJobStartOperator:
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
            impersonation_chain=IMPERSONATION_CHAIN,
            cancel_timeout=CANCEL_TIMEOUT,
        )

    @mock.patch(f"{DATAFLOW_PATH}.DataflowTemplatedJobStartOperator.xcom_push")
    @mock.patch(f"{DATAFLOW_PATH}.DataflowHook")
    def test_execute(self, hook_mock, mock_xcom_push, sync_operator):
        start_template_hook = hook_mock.return_value.start_template_dataflow
        sync_operator.execute(None)
        assert hook_mock.called
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

    @mock.patch(f"{DATAFLOW_PATH}.DataflowTemplatedJobStartOperator.defer")
    @mock.patch(f"{DATAFLOW_PATH}.DataflowHook")
    def test_execute_with_deferrable_mode(self, mock_hook, mock_defer_method, deferrable_operator):
        deferrable_operator.execute(mock.MagicMock())
        expected_variables = {
            "project": "test",
            "stagingLocation": "gs://test/staging",
            "tempLocation": "gs://test/temp",
            "zone": "us-central1-f",
            "EXTRA_OPTION": "TEST_A",
        }
        mock_hook.return_value.launch_job_with_template.assert_called_once_with(
            job_name=JOB_NAME,
            variables=expected_variables,
            parameters=PARAMETERS,
            dataflow_template=TEMPLATE,
            project_id=TEST_PROJECT,
            append_job_name=True,
            location=TEST_LOCATION,
            environment={"maxWorkers": 2},
        )
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
            "impersonation_chain": IMPERSONATION_CHAIN,
            "cancel_timeout": CANCEL_TIMEOUT,
        }
        with pytest.raises(ValueError):
            DataflowTemplatedJobStartOperator(**init_kwargs)

    @pytest.mark.db_test
    @mock.patch(f"{DATAFLOW_PATH}.DataflowTemplatedJobStartOperator.xcom_push")
    @mock.patch(f"{DATAFLOW_PATH}.DataflowHook.start_template_dataflow")
    def test_start_with_custom_region(self, dataflow_mock, mock_xcom_push):
        init_kwargs = {
            "task_id": TASK_ID,
            "template": TEMPLATE,
            "dataflow_default_options": {
                "region": TEST_REGION,
            },
            "poll_sleep": POLL_SLEEP,
            "wait_until_finished": True,
            "cancel_timeout": CANCEL_TIMEOUT,
        }
        operator = DataflowTemplatedJobStartOperator(**init_kwargs)
        operator.execute(None)
        assert dataflow_mock.called
        _, kwargs = dataflow_mock.call_args_list[0]
        assert kwargs["variables"]["region"] == TEST_REGION
        assert kwargs["location"] == DEFAULT_DATAFLOW_LOCATION

    @pytest.mark.db_test
    @mock.patch(f"{DATAFLOW_PATH}.DataflowTemplatedJobStartOperator.xcom_push")
    @mock.patch(f"{DATAFLOW_PATH}.DataflowHook.start_template_dataflow")
    def test_start_with_location(self, dataflow_mock, mock_xcom_push):
        init_kwargs = {
            "task_id": TASK_ID,
            "template": TEMPLATE,
            "location": TEST_LOCATION,
            "poll_sleep": POLL_SLEEP,
            "wait_until_finished": True,
            "cancel_timeout": CANCEL_TIMEOUT,
        }
        operator = DataflowTemplatedJobStartOperator(**init_kwargs)
        operator.execute(None)
        assert dataflow_mock.called
        _, kwargs = dataflow_mock.call_args_list[0]
        assert not kwargs["variables"]
        assert kwargs["location"] == TEST_LOCATION


class TestDataflowStartFlexTemplateOperator:
    @pytest.fixture
    def sync_operator(self):
        return DataflowStartFlexTemplateOperator(
            task_id="start_flex_template_streaming_beam_sql",
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            do_xcom_push=True,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
            expected_terminal_state=DataflowJobStatus.JOB_STATE_DONE,
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

    @mock.patch(f"{DATAFLOW_PATH}.DataflowHook")
    def test_execute(self, mock_dataflow, sync_operator):
        sync_operator.execute(mock.MagicMock())
        mock_dataflow.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            drain_pipeline=False,
            expected_terminal_state=DataflowJobStatus.JOB_STATE_DONE,
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

    @mock.patch(f"{DATAFLOW_PATH}.DataflowStartFlexTemplateOperator.defer")
    @mock.patch(f"{DATAFLOW_PATH}.DataflowHook")
    def test_execute_with_deferrable_mode(self, mock_hook, mock_defer_method, deferrable_operator):
        deferrable_operator.execute(mock.MagicMock())

        mock_hook.return_value.launch_job_with_flex_template.assert_called_once_with(
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            location=TEST_LOCATION,
            project_id=TEST_PROJECT,
        )
        mock_defer_method.assert_called_once()


class TestDataflowStartSqlJobOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_execute(self, mock_hook):
        with pytest.warns(AirflowProviderDeprecationWarning):
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


class TestDataflowStartYamlJobOperator:
    @pytest.fixture
    def sync_operator(self):
        return DataflowStartYamlJobOperator(
            task_id="start_dataflow_yaml_job_sync",
            job_name="dataflow_yaml_job",
            yaml_pipeline_file="test_file_path",
            append_job_name=False,
            project_id=TEST_PROJECT,
            region=TEST_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            expected_terminal_state=DataflowJobStatus.JOB_STATE_DONE,
        )

    @pytest.fixture
    def deferrable_operator(self):
        return DataflowStartYamlJobOperator(
            task_id="start_dataflow_yaml_job_def",
            job_name="dataflow_yaml_job",
            yaml_pipeline_file="test_file_path",
            append_job_name=False,
            project_id=TEST_PROJECT,
            region=TEST_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            deferrable=True,
            expected_terminal_state=DataflowJobStatus.JOB_STATE_RUNNING,
        )

    @mock.patch(f"{DATAFLOW_PATH}.DataflowHook")
    def test_execute(self, mock_hook, sync_operator):
        sync_operator.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            poll_sleep=sync_operator.poll_sleep,
            drain_pipeline=False,
            impersonation_chain=None,
            cancel_timeout=sync_operator.cancel_timeout,
            expected_terminal_state=DataflowJobStatus.JOB_STATE_DONE,
            gcp_conn_id=GCP_CONN_ID,
        )
        mock_hook.return_value.launch_beam_yaml_job.assert_called_once_with(
            job_name=sync_operator.job_name,
            yaml_pipeline_file=sync_operator.yaml_pipeline_file,
            append_job_name=False,
            options=None,
            jinja_variables=None,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
        )

    @mock.patch(f"{DATAFLOW_PATH}.DataflowStartYamlJobOperator.defer")
    @mock.patch(f"{DATAFLOW_PATH}.DataflowHook")
    def test_execute_with_deferrable_mode(self, mock_hook, mock_defer_method, deferrable_operator):
        deferrable_operator.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            poll_sleep=deferrable_operator.poll_sleep,
            drain_pipeline=False,
            impersonation_chain=None,
            cancel_timeout=deferrable_operator.cancel_timeout,
            expected_terminal_state=DataflowJobStatus.JOB_STATE_RUNNING,
            gcp_conn_id=GCP_CONN_ID,
        )
        mock_defer_method.assert_called_once()

    @mock.patch(f"{DATAFLOW_PATH}.DataflowStartYamlJobOperator.xcom_push")
    @mock.patch(f"{DATAFLOW_PATH}.DataflowHook")
    def test_execute_complete_success(self, mock_hook, mock_xcom_push, deferrable_operator):
        expected_result = {"id": JOB_ID}
        actual_result = deferrable_operator.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": "Batch job completed.",
                "job": expected_result,
            },
        )
        mock_xcom_push.assert_called_with(None, key="job_id", value=JOB_ID)
        assert actual_result == expected_result

    def test_execute_complete_error_status_raises_exception(self, deferrable_operator):
        with pytest.raises(AirflowException, match="Job failed."):
            deferrable_operator.execute_complete(
                context=None, event={"status": "error", "message": "Job failed."}
            )
        with pytest.raises(AirflowException, match="Job was stopped."):
            deferrable_operator.execute_complete(
                context=None, event={"status": "stopped", "message": "Job was stopped."}
            )


class TestDataflowStopJobOperator:
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


class TestDataflowCreatePipelineOperator:
    @pytest.fixture
    def create_operator(self):
        """
        Creates a mock create datapipeline operator to be used in testing.
        """
        return DataflowCreatePipelineOperator(
            task_id="test_create_datapipeline",
            body=TEST_PIPELINE_BODY,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_execute(self, mock_hook, create_operator):
        """
        Test that operator creates and calls the Dataflow Data Pipeline hook with the correct parameters
        """
        create_operator.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id="test_gcp_conn_id",
            impersonation_chain=None,
        )

        mock_hook.return_value.create_data_pipeline.assert_called_once_with(
            project_id=TEST_PROJECT, body=TEST_PIPELINE_BODY, location=TEST_LOCATION
        )

    @pytest.mark.db_test
    def test_body_invalid(self):
        """
        Test that if the operator is not passed a Request Body, an AirflowException is raised
        """
        init_kwargs = {
            "task_id": "test_create_datapipeline",
            "body": {},
            "project_id": TEST_PROJECT,
            "location": TEST_LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowCreatePipelineOperator(**init_kwargs).execute(mock.MagicMock())

    def test_projectid_invalid(self):
        """
        Test that if the operator is not passed a Project ID, an AirflowException is raised
        """
        init_kwargs = {
            "task_id": "test_create_datapipeline",
            "body": TEST_PIPELINE_BODY,
            "project_id": None,
            "location": TEST_LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowCreatePipelineOperator(**init_kwargs).execute(mock.MagicMock())

    def test_location_invalid(self):
        """
        Test that if the operator is not passed a location, an AirflowException is raised
        """
        init_kwargs = {
            "task_id": "test_create_datapipeline",
            "body": TEST_PIPELINE_BODY,
            "project_id": TEST_PROJECT,
            "location": None,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowCreatePipelineOperator(**init_kwargs).execute(mock.MagicMock())

    @pytest.mark.db_test
    def test_response_invalid(self):
        """
        Test that if the Response Body contains an error message, an AirflowException is raised
        """
        init_kwargs = {
            "task_id": "test_create_datapipeline",
            "body": {"name": TEST_PIPELINE_NAME, "error": "Testing that AirflowException is raised"},
            "project_id": TEST_PROJECT,
            "location": TEST_LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowCreatePipelineOperator(**init_kwargs).execute(mock.MagicMock())

    @pytest.mark.db_test
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_response_409(self, mock_hook, create_operator):
        """
        Test that if the Pipeline already exists, the operator does not fail and retrieves existed Pipeline
        """
        mock_hook.return_value.create_data_pipeline.side_effect = HttpError(
            resp=httplib2.Response({"status": "409"}), content=b"content"
        )
        create_operator.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id="test_gcp_conn_id",
            impersonation_chain=None,
        )

        mock_hook.return_value.get_data_pipeline.assert_called_once_with(
            project_id=TEST_PROJECT, pipeline_name=TEST_PIPELINE_NAME, location=TEST_LOCATION
        )


@pytest.mark.db_test
class TestDataflowRunPipelineOperator:
    @pytest.fixture
    def run_operator(self):
        """
        Create a DataflowRunPipelineOperator instance with test data
        """
        return DataflowRunPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=TEST_PIPELINE_NAME,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_execute(self, data_pipeline_hook_mock, run_operator):
        """
        Test Run Operator execute with correct parameters
        """
        run_operator.execute(mock.MagicMock())
        data_pipeline_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )

        data_pipeline_hook_mock.return_value.run_data_pipeline.assert_called_once_with(
            pipeline_name=TEST_PIPELINE_NAME,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
        )

    def test_invalid_data_pipeline_name(self):
        """
        Test that AirflowException is raised if Run Operator is not given a data pipeline name.
        """
        init_kwargs = {
            "task_id": TASK_ID,
            "pipeline_name": None,
            "project_id": TEST_PROJECT,
            "location": TEST_LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowRunPipelineOperator(**init_kwargs).execute(mock.MagicMock())

    def test_invalid_project_id(self):
        """
        Test that AirflowException is raised if Run Operator is not given a project ID.
        """
        init_kwargs = {
            "task_id": TASK_ID,
            "pipeline_name": TEST_PIPELINE_NAME,
            "project_id": None,
            "location": TEST_LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowRunPipelineOperator(**init_kwargs).execute(mock.MagicMock())

    def test_invalid_location(self):
        """
        Test that AirflowException is raised if Run Operator is not given a location.
        """
        init_kwargs = {
            "task_id": TASK_ID,
            "pipeline_name": TEST_PIPELINE_NAME,
            "project_id": TEST_PROJECT,
            "location": None,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowRunPipelineOperator(**init_kwargs).execute(mock.MagicMock())

    def test_invalid_response(self):
        """
        Test that AirflowException is raised if Run Operator fails execution and returns error.
        """
        init_kwargs = {
            "task_id": TASK_ID,
            "data_pipeline_name": TEST_PIPELINE_NAME,
            "project_id": TEST_PROJECT,
            "location": TEST_LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(TypeError, match="missing keyword argument"):
            DataflowRunPipelineOperator(**init_kwargs).execute(mock.MagicMock()).return_value = {
                "error": {"message": "example error"}
            }

    @pytest.mark.db_test
    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_response_404(self, mock_hook, run_operator):
        """
        Test that if the Pipeline does not exist, the operator raise AirflowException
        """
        mock_hook.return_value.run_data_pipeline.side_effect = HttpError(
            resp=httplib2.Response({"status": "404"}), content=b"content"
        )
        with pytest.raises(AirflowException):
            run_operator.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id="test_gcp_conn_id",
            impersonation_chain=None,
        )


@pytest.mark.db_test
class TestDataflowDeletePipelineOperator:
    @pytest.fixture
    def run_operator(self):
        """
        Create a DataflowDeletePipelineOperator instance with test data
        """
        return DataflowDeletePipelineOperator(
            task_id=TASK_ID,
            pipeline_name=TEST_PIPELINE_NAME,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    # @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook.delete_data_pipeline")
    def test_execute(self, data_pipeline_hook_mock, run_operator):
        """
        Test Delete Operator execute with correct parameters
        """
        data_pipeline_hook_mock.return_value.delete_data_pipeline.return_value = None
        run_operator.execute(mock.MagicMock())
        data_pipeline_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )

        data_pipeline_hook_mock.return_value.delete_data_pipeline.assert_called_once_with(
            pipeline_name=TEST_PIPELINE_NAME,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
        )

    def test_invalid_data_pipeline_name(self):
        """
        Test that AirflowException is raised if Delete Operator is not given a data pipeline name.
        """
        init_kwargs = {
            "task_id": TASK_ID,
            "pipeline_name": None,
            "project_id": TEST_PROJECT,
            "location": TEST_LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowDeletePipelineOperator(**init_kwargs).execute(mock.MagicMock())

    def test_invalid_project_id(self):
        """
        Test that AirflowException is raised if Delete Operator is not given a project ID.
        """
        init_kwargs = {
            "task_id": TASK_ID,
            "pipeline_name": TEST_PIPELINE_NAME,
            "project_id": None,
            "location": TEST_LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowDeletePipelineOperator(**init_kwargs).execute(mock.MagicMock())

    def test_invalid_location(self):
        """
        Test that AirflowException is raised if Delete Operator is not given a location.
        """
        init_kwargs = {
            "task_id": TASK_ID,
            "pipeline_name": TEST_PIPELINE_NAME,
            "project_id": TEST_PROJECT,
            "location": None,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowDeletePipelineOperator(**init_kwargs).execute(mock.MagicMock())

    def test_invalid_response(self):
        """
        Test that AirflowException is raised if Delete Operator fails execution and returns error.
        """
        init_kwargs = {
            "task_id": TASK_ID,
            "pipeline_name": TEST_PIPELINE_NAME,
            "project_id": TEST_PROJECT,
            "location": TEST_LOCATION,
            "gcp_conn_id": GCP_CONN_ID,
        }
        with pytest.raises(AirflowException):
            DataflowDeletePipelineOperator(**init_kwargs).execute(mock.MagicMock()).return_value = {
                "error": {"message": "example error"}
            }
