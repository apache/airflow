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

import botocore.client
import pytest

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, TaskDeferred
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.operators.batch import BatchCreateComputeEnvironmentOperator, BatchOperator

# Use dummy AWS credentials
from airflow.providers.amazon.aws.triggers.batch import (
    BatchCreateComputeEnvironmentTrigger,
    BatchJobTrigger,
)
from airflow.utils.task_instance_session import set_current_task_instance_session

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
            retry_strategy={"attempts": 1},
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
        assert self.batch.retry_strategy == {"attempts": 1}
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

    def test_init_defaults(self):
        """Test constructor default values"""
        batch_job = BatchOperator(
            task_id="task",
            job_name=JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
        )
        assert batch_job.job_id is None
        assert batch_job.job_name == JOB_NAME
        assert batch_job.job_queue == "queue"
        assert batch_job.job_definition == "hello-world"
        assert batch_job.waiters is None
        assert batch_job.hook.max_retries == 4200
        assert batch_job.hook.status_retries == 10
        assert batch_job.parameters == {}
        assert batch_job.retry_strategy is None
        assert batch_job.container_overrides is None
        assert batch_job.array_properties is None
        assert batch_job.ecs_properties_override is None
        assert batch_job.eks_properties_override is None
        assert batch_job.node_overrides is None
        assert batch_job.share_identifier is None
        assert batch_job.scheduling_priority_override is None
        assert batch_job.hook.region_name is None
        assert batch_job.hook.aws_conn_id is None
        assert issubclass(type(batch_job.hook.client), botocore.client.BaseClient)
        assert batch_job.tags == {}
        assert batch_job.wait_for_completion is True

    def test_template_fields_overrides(self):
        assert self.batch.template_fields == (
            "job_id",
            "job_name",
            "job_definition",
            "job_queue",
            "container_overrides",
            "array_properties",
            "ecs_properties_override",
            "eks_properties_override",
            "node_overrides",
            "parameters",
            "retry_strategy",
            "waiters",
            "tags",
            "wait_for_completion",
            "awslogs_enabled",
            "awslogs_fetch_interval",
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
            retryStrategy={"attempts": 1},
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
            retryStrategy={"attempts": 1},
            tags={},
        )

    @mock.patch.object(BatchClientHook, "get_job_description")
    @mock.patch.object(BatchClientHook, "wait_for_job")
    @mock.patch.object(BatchClientHook, "check_job_success")
    def test_execute_with_ecs_overrides(self, check_mock, wait_mock, job_description_mock):
        self.batch.container_overrides = None
        self.batch.ecs_properties_override = {
            "taskProperties": [
                {
                    "containers": [
                        {
                            "command": [
                                "string",
                            ],
                            "environment": [
                                {"name": "string", "value": "string"},
                            ],
                            "name": "string",
                            "resourceRequirements": [
                                {"value": "string", "type": "'GPU'|'VCPU'|'MEMORY'"},
                            ],
                        },
                    ]
                },
            ]
        }
        self.batch.execute(self.mock_context)

        self.client_mock.submit_job.assert_called_once_with(
            jobQueue="queue",
            jobName=JOB_NAME,
            jobDefinition="hello-world",
            ecsPropertiesOverride={
                "taskProperties": [
                    {
                        "containers": [
                            {
                                "command": [
                                    "string",
                                ],
                                "environment": [
                                    {"name": "string", "value": "string"},
                                ],
                                "name": "string",
                                "resourceRequirements": [
                                    {"value": "string", "type": "'GPU'|'VCPU'|'MEMORY'"},
                                ],
                            },
                        ]
                    },
                ]
            },
            parameters={},
            retryStrategy={"attempts": 1},
            tags={},
        )

    @mock.patch.object(BatchClientHook, "get_job_description")
    @mock.patch.object(BatchClientHook, "wait_for_job")
    @mock.patch.object(BatchClientHook, "check_job_success")
    def test_execute_with_eks_overrides(self, check_mock, wait_mock, job_description_mock):
        self.batch.container_overrides = None
        self.batch.eks_properties_override = {
            "podProperties": [
                {
                    "containers": [
                        {
                            "image": "string",
                            "command": [
                                "string",
                            ],
                            "args": [
                                "string",
                            ],
                            "env": [
                                {"name": "string", "value": "string"},
                            ],
                            "resources": [{"limits": {"string": "string"}, "requests": {"string": "string"}}],
                        },
                    ],
                    "initContainers": [
                        {
                            "image": "string",
                            "command": [
                                "string",
                            ],
                            "args": [
                                "string",
                            ],
                            "env": [
                                {"name": "string", "value": "string"},
                            ],
                            "resources": [{"limits": {"string": "string"}, "requests": {"string": "string"}}],
                        },
                    ],
                    "metadata": {
                        "labels": {"string": "string"},
                    },
                },
            ]
        }
        self.batch.execute(self.mock_context)

        self.client_mock.submit_job.assert_called_once_with(
            jobQueue="queue",
            jobName=JOB_NAME,
            jobDefinition="hello-world",
            eksPropertiesOverride={
                "podProperties": [
                    {
                        "containers": [
                            {
                                "image": "string",
                                "command": [
                                    "string",
                                ],
                                "args": [
                                    "string",
                                ],
                                "env": [
                                    {"name": "string", "value": "string"},
                                ],
                                "resources": [
                                    {"limits": {"string": "string"}, "requests": {"string": "string"}}
                                ],
                            },
                        ],
                        "initContainers": [
                            {
                                "image": "string",
                                "command": [
                                    "string",
                                ],
                                "args": [
                                    "string",
                                ],
                                "env": [
                                    {"name": "string", "value": "string"},
                                ],
                                "resources": [
                                    {"limits": {"string": "string"}, "requests": {"string": "string"}}
                                ],
                            },
                        ],
                        "metadata": {
                            "labels": {"string": "string"},
                        },
                    },
                ]
            },
            parameters={},
            retryStrategy={"attempts": 1},
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

    @pytest.mark.parametrize(
        "override", ["overrides", "node_overrides", "ecs_properties_override", "eks_properties_override"]
    )
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
        if override == "overrides":
            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="Parameter `overrides` is deprecated, Please use `container_overrides` instead.",
            ):
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
        else:
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

        py2api = {
            "overrides": "containerOverrides",
            "node_overrides": "nodeOverrides",
            "ecs_properties_override": "ecsPropertiesOverride",
            "eks_properties_override": "eksPropertiesOverride",
        }

        expected_args[py2api[override]] = {"a": "a"}

        client_mock().submit_job.assert_called_once_with(**expected_args)

    def test_deprecated_override_param(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
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

    @mock.patch.object(BatchClientHook, "get_job_description")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
    def test_defer_if_deferrable_param_set(self, mock_client, mock_get_job_description):
        mock_get_job_description.return_value = {"status": "SUBMITTED"}

        batch = BatchOperator(
            task_id="task",
            job_name=JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            do_xcom_push=False,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            batch.execute(self.mock_context)
        assert isinstance(exc.value.trigger, BatchJobTrigger)

    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
    def test_defer_but_failed_due_to_job_id_not_found(self, mock_client):
        """Test that an AirflowException is raised if job_id is not set before deferral."""
        mock_client.return_value.submit_job.return_value = {
            "jobName": JOB_NAME,
            "jobId": None,
        }

        batch = BatchOperator(
            task_id="task",
            job_name=JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            do_xcom_push=False,
            deferrable=True,
        )
        with pytest.raises(AirflowException) as exc:
            batch.execute(self.mock_context)
        assert "AWS Batch job - job_id was not found" in str(exc.value)

    @mock.patch.object(BatchClientHook, "get_job_description")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
    def test_defer_but_success_before_deferred(self, mock_client, mock_get_job_description):
        """Test that an AirflowException is raised if job_id is not set before deferral."""
        mock_client.return_value.submit_job.return_value = RESPONSE_WITHOUT_FAILURES
        mock_get_job_description.return_value = {"status": "SUCCEEDED"}

        batch = BatchOperator(
            task_id="task",
            job_name=JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            do_xcom_push=False,
            deferrable=True,
        )
        assert batch.execute(self.mock_context) == JOB_ID

    @mock.patch.object(BatchClientHook, "get_job_description")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
    def test_defer_but_fail_before_deferred(self, mock_client, mock_get_job_description):
        """Test that an AirflowException is raised if job_id is not set before deferral."""
        mock_client.return_value.submit_job.return_value = RESPONSE_WITHOUT_FAILURES
        mock_get_job_description.return_value = {"status": "FAILED"}

        batch = BatchOperator(
            task_id="task",
            job_name=JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            do_xcom_push=False,
            deferrable=True,
        )
        with pytest.raises(AirflowException) as exc:
            batch.execute(self.mock_context)
        assert f"Error while running job: {JOB_ID} is in FAILED state" in str(exc.value)

    @mock.patch.object(BatchClientHook, "get_job_description")
    @mock.patch.object(BatchClientHook, "wait_for_job")
    @mock.patch.object(BatchClientHook, "check_job_success")
    @mock.patch("airflow.providers.amazon.aws.links.batch.BatchJobQueueLink.persist")
    @mock.patch("airflow.providers.amazon.aws.links.batch.BatchJobDefinitionLink.persist")
    def test_monitor_job_with_logs(
        self, job_definition_persist_mock, job_queue_persist_mock, check_mock, wait_mock, job_description_mock
    ):
        batch = BatchOperator(
            task_id="task",
            job_name=JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            awslogs_enabled=True,
        )

        batch.job_id = JOB_ID

        batch.monitor_job(context=None)

        job_description_mock.assert_called_with(job_id=JOB_ID)
        job_definition_persist_mock.assert_called_once()
        job_queue_persist_mock.assert_called_once()
        wait_mock.assert_called_once()
        assert len(wait_mock.call_args) == 2


class TestBatchCreateComputeEnvironmentOperator:
    warn_message = "The `status_retries` parameter is unused and should be removed"

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

    def test_deprecation(self):
        with pytest.warns(AirflowProviderDeprecationWarning, match=self.warn_message):
            BatchCreateComputeEnvironmentOperator(
                task_id="id",
                compute_environment_name="environment_name",
                environment_type="environment_type",
                state="environment_state",
                compute_resources={},
                status_retries="Huh?",
            )

    @pytest.mark.db_test
    def test_partial_deprecation(self, dag_maker, session):
        with dag_maker(dag_id="test_partial_deprecation_waiters_params_reg_ecs", session=session):
            BatchCreateComputeEnvironmentOperator.partial(
                task_id="id",
                compute_environment_name="environment_name",
                environment_type="environment_type",
                state="environment_state",
                status_retries="Huh?",
            ).expand(compute_resources=[{}, {}])

        dr = dag_maker.create_dagrun()
        tis = dr.get_task_instances(session=session)
        with set_current_task_instance_session(session=session):
            for ti in tis:
                with pytest.warns(AirflowProviderDeprecationWarning, match=self.warn_message):
                    ti.render_templates()
                assert not hasattr(ti.task, "status_retries")

    @mock.patch.object(BatchClientHook, "client")
    def test_defer(self, client_mock):
        client_mock.create_compute_environment.return_value = {"computeEnvironmentArn": "my_arn"}

        operator = BatchCreateComputeEnvironmentOperator(
            task_id="task",
            compute_environment_name="my_env_name",
            environment_type="my_env_type",
            state="my_state",
            compute_resources={},
            max_retries=123456,
            poll_interval=456789,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as deferred:
            operator.execute(None)

        assert isinstance(deferred.value.trigger, BatchCreateComputeEnvironmentTrigger)
        assert deferred.value.trigger.waiter_delay == 456789
        assert deferred.value.trigger.attempts == 123456
