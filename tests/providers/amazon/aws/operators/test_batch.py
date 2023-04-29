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

from unittest import mock
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.operators.batch import BatchCreateComputeEnvironmentOperator, BatchOperator

# Use dummy AWS credentials
AWS_REGION = "eu-west-1"
AWS_ACCESS_KEY_ID = "airflow_dummy_key"
AWS_SECRET_ACCESS_KEY = "airflow_dummy_secret"

JOB_NAME = "51455483-c62c-48ac-9b88-53a6a725baa3"
JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"

RESPONSE_WITHOUT_FAILURES = {
    "jobName": JOB_NAME,
    "jobId": JOB_ID,
}


class TestBatchOperator:

    MAX_RETRIES = 2
    STATUS_RETRIES = 3

    @mock.patch.dict("os.environ", AWS_DEFAULT_REGION=AWS_REGION)
    @mock.patch.dict("os.environ", AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID)
    @mock.patch.dict("os.environ", AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
    def setup_method(self, _, get_client_type_mock):
        self.get_client_type_mock = get_client_type_mock
        self.batch = BatchOperator(
            task_id="task",
            job_name=JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            max_retries=self.MAX_RETRIES,
            status_retries=self.STATUS_RETRIES,
            parameters=None,
            container_overrides={},
            array_properties=None,
            aws_conn_id="airflow_test",
            region_name="eu-west-1",
            tags={},
        )
        self.client_mock = self.get_client_type_mock.return_value
        # We're mocking all actual AWS calls and don't need a connection. This
        # avoids an Airflow warning about connection cannot be found.
        self.batch.hook.get_connection = lambda _: None
        assert self.batch.hook.client == self.client_mock  # setup client property

        # don't pause in unit tests
        self.mock_delay = mock.Mock(return_value=None)
        self.batch.delay = self.mock_delay
        self.mock_exponential_delay = mock.Mock(return_value=0)
        self.batch.exponential_delay = self.mock_exponential_delay

        # Assign a job ID for most tests, so they don't depend on a job submission.
        assert self.batch.job_id is None
        self.batch.job_id = JOB_ID

        self.mock_context = mock.MagicMock()

    def test_init(self):
        assert self.batch.job_id == JOB_ID
        assert self.batch.job_name == JOB_NAME
        assert self.batch.job_queue == "queue"
        assert self.batch.job_definition == "hello-world"
        assert self.batch.waiters is None
        assert self.batch.hook.max_retries == self.MAX_RETRIES
        assert self.batch.hook.status_retries == self.STATUS_RETRIES
        assert self.batch.parameters == {}
        assert self.batch.container_overrides == {}
        assert self.batch.array_properties is None
        assert self.batch.node_overrides is None
        assert self.batch.share_identifier is None
        assert self.batch.scheduling_priority_override is None
        assert self.batch.hook.region_name == "eu-west-1"
        assert self.batch.hook.aws_conn_id == "airflow_test"
        assert self.batch.hook.client == self.client_mock
        assert self.batch.tags == {}
        assert self.batch.wait_for_completion is True

        self.get_client_type_mock.assert_called_once_with(region_name="eu-west-1")

    def test_template_fields_overrides(self):
        assert self.batch.template_fields == (
            "job_id",
            "job_name",
            "job_definition",
            "job_queue",
            "container_overrides",
            "array_properties",
            "node_overrides",
            "parameters",
            "waiters",
            "tags",
            "wait_for_completion",
        )

    @mock.patch.object(BatchClientHook, "get_job_description")
    @mock.patch.object(BatchClientHook, "wait_for_job")
    @mock.patch.object(BatchClientHook, "check_job_success")
    def test_execute_without_failures(self, check_mock, wait_mock, job_description_mock):
        # JOB_ID is in RESPONSE_WITHOUT_FAILURES
        self.client_mock.submit_job.return_value = RESPONSE_WITHOUT_FAILURES
        self.batch.job_id = None
        self.batch.waiters = None  # use default wait

        self.batch.execute(self.mock_context)

        self.client_mock.submit_job.assert_called_once_with(
            jobQueue="queue",
            jobName=JOB_NAME,
            containerOverrides={},
            jobDefinition="hello-world",
            parameters={},
            tags={},
        )

        assert self.batch.job_id == JOB_ID
        wait_mock.assert_called_once_with(JOB_ID)
        check_mock.assert_called_once_with(JOB_ID)

        # First Call: Retrieve Batch Queue and Job Definition
        # Second Call: Retrieve CloudWatch information
        assert job_description_mock.call_count == 2

    def test_execute_with_failures(self):
        self.client_mock.submit_job.side_effect = Exception()

        with pytest.raises(AirflowException):
            self.batch.execute(self.mock_context)

        self.client_mock.submit_job.assert_called_once_with(
            jobQueue="queue",
            jobName=JOB_NAME,
            containerOverrides={},
            jobDefinition="hello-world",
            parameters={},
            tags={},
        )

    @mock.patch.object(BatchClientHook, "check_job_success")
    def test_wait_job_complete_using_waiters(self, check_mock):
        mock_waiters = mock.Mock()
        self.batch.waiters = mock_waiters

        self.client_mock.submit_job.return_value = RESPONSE_WITHOUT_FAILURES
        self.client_mock.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": JOB_ID,
                    "status": "SUCCEEDED",
                    "logStreamName": "logStreamName",
                    "container": {"logConfiguration": {}},
                }
            ]
        }
        self.batch.execute(self.mock_context)
        mock_waiters.wait_for_job.assert_called_once_with(JOB_ID)
        check_mock.assert_called_once_with(JOB_ID)

    @mock.patch.object(BatchClientHook, "check_job_success")
    def test_do_not_wait_job_complete(self, check_mock):
        self.batch.wait_for_completion = False

        self.client_mock.submit_job.return_value = RESPONSE_WITHOUT_FAILURES
        self.batch.execute(self.mock_context)

        check_mock.assert_not_called()

    def test_kill_job(self):
        self.client_mock.terminate_job.return_value = {}
        self.batch.on_kill()
        self.client_mock.terminate_job.assert_called_once_with(jobId=JOB_ID, reason="Task killed by the user")

    @pytest.mark.parametrize("override", ["overrides", "node_overrides"])
    @patch(
        "airflow.providers.amazon.aws.hooks.batch_client.BatchClientHook.client",
        new_callable=mock.PropertyMock,
    )
    def test_override_not_sent_if_not_set(self, client_mock, override):
        """
        check that when setting container override or node override, the other key is not sent
        in the API call (which would create a validation error from boto)
        """
        override_arg = {override: {"a": "a"}}
        batch = BatchOperator(
            task_id="task",
            job_name=JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            **override_arg,
            # setting those to bypass code that is not relevant here
            do_xcom_push=False,
            wait_for_completion=False,
        )

        batch.execute(None)

        expected_args = {
            "jobQueue": "queue",
            "jobName": JOB_NAME,
            "jobDefinition": "hello-world",
            "parameters": {},
            "tags": {},
        }
        if override == "overrides":
            expected_args["containerOverrides"] = {"a": "a"}
        else:
            expected_args["nodeOverrides"] = {"a": "a"}
        client_mock().submit_job.assert_called_once_with(**expected_args)

    def test_deprecated_override_param(self):
        with pytest.warns(DeprecationWarning):
            _ = BatchOperator(
                task_id="task",
                job_name=JOB_NAME,
                job_queue="queue",
                job_definition="hello-world",
                overrides={"a": "b"},  # <- the deprecated field
            )

    def test_cant_set_old_and_new_override_param(self):
        with pytest.raises(AirflowException):
            _ = BatchOperator(
                task_id="task",
                job_name=JOB_NAME,
                job_queue="queue",
                job_definition="hello-world",
                # can't set both of those, as one is a replacement for the other
                overrides={"a": "b"},
                container_overrides={"a": "b"},
            )


class TestBatchCreateComputeEnvironmentOperator:
    @mock.patch.object(BatchClientHook, "client")
    def test_execute(self, mock_conn):
        environment_name = "environment_name"
        environment_type = "environment_type"
        environment_state = "environment_state"
        compute_resources = {}
        tags = {}
        operator = BatchCreateComputeEnvironmentOperator(
            task_id="task",
            compute_environment_name=environment_name,
            environment_type=environment_type,
            state=environment_state,
            compute_resources=compute_resources,
            tags=tags,
        )
        operator.execute(None)
        mock_conn.create_compute_environment.assert_called_once_with(
            computeEnvironmentName=environment_name,
            type=environment_type,
            state=environment_state,
            computeResources=compute_resources,
            tags=tags,
        )
