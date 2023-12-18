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

import re
from unittest import mock

import boto3
import pytest
from botocore.exceptions import WaiterError
from moto import mock_emr

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrHook


class TestEmrHook:
    def test_service_waiters(self):
        hook = EmrHook(aws_conn_id=None)
        official_waiters = hook.conn.waiter_names
        custom_waiters = [
            "job_flow_waiting",
            "job_flow_terminated",
            "notebook_running",
            "notebook_stopped",
            "step_wait_for_terminal",
            "steps_wait_for_terminal",
        ]

        assert sorted(hook.list_waiters()) == sorted([*official_waiters, *custom_waiters])

    @mock_emr
    def test_get_conn_returns_a_boto3_connection(self):
        hook = EmrHook(aws_conn_id="aws_default", region_name="ap-southeast-2")
        assert hook.get_conn().list_clusters() is not None

    @mock_emr
    def test_create_job_flow_uses_the_emr_config_to_create_a_cluster(self):
        client = boto3.client("emr", region_name="us-east-1")

        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="emr_default", region_name="us-east-1")
        cluster = hook.create_job_flow(
            {"Name": "test_cluster", "Instances": {"KeepJobFlowAliveWhenNoSteps": False}}
        )

        assert client.list_clusters()["Clusters"][0]["Id"] == cluster["JobFlowId"]

    @mock_emr
    @pytest.mark.parametrize("num_steps", [1, 2, 3, 4])
    def test_add_job_flow_steps_one_step(self, num_steps):
        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="emr_default", region_name="us-east-1")
        cluster = hook.create_job_flow(
            {"Name": "test_cluster", "Instances": {"KeepJobFlowAliveWhenNoSteps": False}}
        )
        steps = [
            {
                "ActionOnFailure": "test_step",
                "HadoopJarStep": {
                    "Args": ["test args"],
                    "Jar": "test.jar",
                },
                "Name": f"step_{i}",
            }
            for i in range(num_steps)
        ]
        response = hook.add_job_flow_steps(job_flow_id=cluster["JobFlowId"], steps=steps)

        assert len(response) == num_steps
        for step_id in response:
            assert re.match("s-[A-Z0-9]{13}$", step_id)

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_add_job_flow_steps_wait_for_completion(self, mock_conn):
        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="emr_default", region_name="us-east-1")
        mock_conn.run_job_flow.return_value = {
            "JobFlowId": "job_flow_id",
            "ClusterArn": "cluster_arn",
        }
        mock_conn.add_job_flow_steps.return_value = {
            "StepIds": [
                "step_id",
            ],
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        hook.create_job_flow({"Name": "test_cluster", "Instances": {"KeepJobFlowAliveWhenNoSteps": False}})

        steps = [
            {
                "ActionOnFailure": "test_step",
                "HadoopJarStep": {
                    "Args": ["test args"],
                    "Jar": "test.jar",
                },
                "Name": "step_1",
            }
        ]

        hook.add_job_flow_steps(job_flow_id="job_flow_id", steps=steps, wait_for_completion=True)

        mock_conn.get_waiter.assert_called_once_with("step_complete")

    @mock.patch("time.sleep", return_value=True)
    @mock.patch.object(EmrHook, "conn")
    def test_add_job_flow_steps_raises_exception_on_failure(self, mock_conn, mock_sleep, caplog):
        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="emr_default", region_name="us-east-1")
        mock_conn.describe_step.return_value = {
            "Step": {
                "Status": {
                    "State": "FAILED",
                    "FailureDetails": "test failure details",
                }
            }
        }
        mock_conn.add_job_flow_steps.return_value = {
            "StepIds": [
                "step_id",
            ],
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        steps = [
            {
                "ActionOnFailure": "test_step",
                "HadoopJarStep": {
                    "Args": ["test args"],
                    "Jar": "test.jar",
                },
                "Name": "step_1",
            }
        ]
        waiter_error = WaiterError(name="test_error", reason="test_reason", last_response={})
        waiter_error_failure = WaiterError(name="test_error", reason="terminal failure", last_response={})
        mock_conn.get_waiter().wait.side_effect = [waiter_error, waiter_error_failure]

        with pytest.raises(AirflowException):
            hook.add_job_flow_steps(job_flow_id="job_flow_id", steps=steps, wait_for_completion=True)
        assert "test failure details" in caplog.messages[-1]
        mock_conn.get_waiter.assert_called_with("step_complete")

    @pytest.mark.db_test
    @mock_emr
    def test_create_job_flow_extra_args(self):
        """
        Test that we can add extra arguments to the launch call.

        This is useful for when AWS add new options, such as
        "SecurityConfiguration" so that we don't have to change our code
        """
        client = boto3.client("emr", region_name="us-east-1")

        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="emr_default")
        # AmiVersion is really old and almost no one will use it anymore, but
        # it's one of the "optional" request params that moto supports - it's
        # coverage of EMR isn't 100% it turns out.
        with pytest.warns(None):  # Expected no warnings if ``emr_conn_id`` exists with correct conn_type
            cluster = hook.create_job_flow({"Name": "test_cluster", "ReleaseLabel": "", "AmiVersion": "3.2"})
        cluster = client.describe_cluster(ClusterId=cluster["JobFlowId"])["Cluster"]

        # The AmiVersion comes back as {Requested,Running}AmiVersion fields.
        assert cluster["RequestedAmiVersion"] == "3.2"

    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_conn")
    def test_empty_emr_conn_id(self, mock_boto3_client):
        """Test empty ``emr_conn_id``."""
        mock_run_job_flow = mock.MagicMock()
        mock_boto3_client.return_value.run_job_flow = mock_run_job_flow
        job_flow_overrides = {"foo": "bar"}

        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id=None)
        hook.create_job_flow(job_flow_overrides)
        mock_run_job_flow.assert_called_once_with(**job_flow_overrides)

    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_conn")
    def test_missing_emr_conn_id(self, mock_boto3_client):
        """Test not exists ``emr_conn_id``."""
        mock_run_job_flow = mock.MagicMock()
        mock_boto3_client.return_value.run_job_flow = mock_run_job_flow
        job_flow_overrides = {"foo": "bar"}

        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="not-exists-emr-conn-id")
        warning_message = r"Unable to find Amazon Elastic MapReduce Connection ID 'not-exists-emr-conn-id',.*"
        with pytest.warns(UserWarning, match=warning_message):
            hook.create_job_flow(job_flow_overrides)
        mock_run_job_flow.assert_called_once_with(**job_flow_overrides)

    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_conn")
    def test_emr_conn_id_wrong_conn_type(self, mock_boto3_client):
        """Test exists ``emr_conn_id`` have unexpected ``conn_type``."""
        mock_run_job_flow = mock.MagicMock()
        mock_boto3_client.return_value.run_job_flow = mock_run_job_flow
        job_flow_overrides = {"foo": "bar"}

        with mock.patch.dict("os.environ", AIRFLOW_CONN_WRONG_TYPE_CONN="aws://"):
            hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="wrong_type_conn")
            warning_message = (
                r"Amazon Elastic MapReduce Connection expected connection type 'emr'"
                r".* This connection might not work correctly."
            )
            with pytest.warns(UserWarning, match=warning_message):
                hook.create_job_flow(job_flow_overrides)
            mock_run_job_flow.assert_called_once_with(**job_flow_overrides)

    @pytest.mark.parametrize("aws_conn_id", ["aws_default", None])
    @pytest.mark.parametrize("emr_conn_id", ["emr_default", None])
    def test_emr_connection(self, aws_conn_id, emr_conn_id):
        """Test that ``EmrHook`` always return False state."""
        hook = EmrHook(aws_conn_id=aws_conn_id, emr_conn_id=emr_conn_id)
        result, message = hook.test_connection()
        assert not result
        assert message.startswith("'Amazon Elastic MapReduce' Airflow Connection cannot be tested")

    @mock_emr
    def test_get_cluster_id_by_name(self):
        """
        Test that we can resolve cluster id by cluster name.
        """
        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="emr_default")

        job_flow = hook.create_job_flow(
            {"Name": "test_cluster", "Instances": {"KeepJobFlowAliveWhenNoSteps": True}}
        )

        job_flow_id = job_flow["JobFlowId"]

        matching_cluster = hook.get_cluster_id_by_name("test_cluster", ["RUNNING", "WAITING"])

        assert matching_cluster == job_flow_id

        no_match = hook.get_cluster_id_by_name("foo", ["RUNNING", "WAITING", "BOOTSTRAPPING"])

        assert no_match is None

    @mock_emr
    def test_get_cluster_id_by_name_duplicate(self):
        """
        Test that we get an exception when there are duplicate clusters
        """
        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="emr_default")

        hook.create_job_flow({"Name": "test_cluster", "Instances": {"KeepJobFlowAliveWhenNoSteps": True}})

        hook.create_job_flow({"Name": "test_cluster", "Instances": {"KeepJobFlowAliveWhenNoSteps": True}})

        with pytest.raises(AirflowException):
            hook.get_cluster_id_by_name("test_cluster", ["RUNNING", "WAITING", "BOOTSTRAPPING"])

    @mock_emr
    def test_get_cluster_id_by_name_pagination(self):
        """
        Test that we can resolve cluster id by cluster name when there are
        enough clusters to trigger pagination
        """
        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="emr_default")

        # Create enough clusters to trigger pagination
        for index in range(51):
            hook.create_job_flow(
                {"Name": f"test_cluster_{index}", "Instances": {"KeepJobFlowAliveWhenNoSteps": True}}
            )

        # Fetch a cluster from the second page using the boto API
        client = boto3.client("emr", region_name="us-east-1")
        response_marker = client.list_clusters(ClusterStates=["RUNNING", "WAITING", "BOOTSTRAPPING"])[
            "Marker"
        ]
        second_page_cluster = client.list_clusters(
            ClusterStates=["RUNNING", "WAITING", "BOOTSTRAPPING"], Marker=response_marker
        )["Clusters"][0]

        # Now that we have a cluster, fetch the id with the name
        second_page_cluster_id = hook.get_cluster_id_by_name(
            second_page_cluster["Name"], ["RUNNING", "WAITING", "BOOTSTRAPPING"]
        )

        # Assert that the id we got from the hook is the same as the one we got
        # from the boto api
        assert second_page_cluster_id == second_page_cluster["Id"]

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_add_job_flow_steps_execution_role_arn(self, mock_conn):
        """
        Test that execution_role_arn only gets passed when it is not None.
        """
        mock_conn.run_job_flow.return_value = {
            "JobFlowId": "job_flow_id",
            "ClusterArn": "cluster_arn",
        }
        mock_conn.add_job_flow_steps.return_value = {
            "StepIds": [
                "step_id",
            ],
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        hook = EmrHook(aws_conn_id="aws_default", emr_conn_id="emr_default")

        job_flow = hook.create_job_flow(
            {"Name": "test_cluster", "Instances": {"KeepJobFlowAliveWhenNoSteps": True}}
        )

        job_flow_id = job_flow["JobFlowId"]

        step = {
            "ActionOnFailure": "test_step",
            "HadoopJarStep": {
                "Args": ["test args"],
                "Jar": "test.jar",
            },
            "Name": "step_1",
        }

        hook.add_job_flow_steps(job_flow_id=job_flow_id, steps=step)
        mock_conn.add_job_flow_steps.assert_called_with(JobFlowId=job_flow_id, Steps=step)

        hook.add_job_flow_steps(job_flow_id=job_flow_id, steps=step, execution_role_arn=None)
        mock_conn.add_job_flow_steps.assert_called_with(JobFlowId=job_flow_id, Steps=step)

        hook.add_job_flow_steps(job_flow_id=job_flow_id, steps=step, execution_role_arn="")
        mock_conn.add_job_flow_steps.assert_called_with(JobFlowId=job_flow_id, Steps=step)

        hook.add_job_flow_steps(
            job_flow_id=job_flow_id, steps=step, execution_role_arn="test-execution-role-arn"
        )
        mock_conn.add_job_flow_steps.assert_called_with(
            JobFlowId=job_flow_id, Steps=step, ExecutionRoleArn="test-execution-role-arn"
        )
