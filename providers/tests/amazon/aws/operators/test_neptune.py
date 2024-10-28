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

from typing import Generator
from unittest import mock

import pytest
from boto3 import client
from moto import mock_aws

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.neptune import NeptuneHook
from airflow.providers.amazon.aws.operators.neptune import (
    NeptuneStartDbClusterOperator,
    NeptuneStopDbClusterOperator,
)

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

CLUSTER_ID = "test_cluster"

EXPECTED_RESPONSE = {"db_cluster_id": CLUSTER_ID}


@pytest.fixture
def hook() -> Generator[NeptuneHook, None, None]:
    with mock_aws():
        yield NeptuneHook(aws_conn_id="aws_default")


@pytest.fixture
def _create_cluster(hook: NeptuneHook):
    hook.conn.create_db_cluster(
        DBClusterIdentifier=CLUSTER_ID,
        Engine="neptune",
    )
    if not hook.conn.describe_db_clusters()["DBClusters"]:
        raise ValueError("AWS not properly mocked")


class TestNeptuneStartClusterOperator:
    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_start_cluster_wait_for_completion(self, mock_hook_get_waiter, mock_conn):
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)
        mock_hook_get_waiter.assert_called_once_with("cluster_available")
        assert resp == EXPECTED_RESPONSE

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_start_cluster_no_wait(self, mock_hook_get_waiter, mock_conn):
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)
        mock_hook_get_waiter.assert_not_called()
        assert resp == EXPECTED_RESPONSE

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_start_cluster_cluster_available(
        self, mock_waiter, mock_get_cluster_status, mock_conn
    ):
        mock_get_cluster_status.return_value = "available"
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)

        mock_conn.start_db_cluster.assert_not_called()
        mock_waiter.assert_not_called()
        assert resp == {"db_cluster_id": CLUSTER_ID}

    @mock.patch.object(NeptuneHook, "conn")
    def test_start_cluster_deferrable(self, mock_conn):
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=True,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        with pytest.raises(TaskDeferred):
            operator.execute(None)

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_start_cluster_cluster_error(
        self, mock_waiter, mock_get_cluster_status, mock_conn
    ):
        mock_get_cluster_status.return_value = "migration-failed"
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        with pytest.raises(AirflowException):
            operator.execute(None)

    @mock.patch(
        "airflow.providers.amazon.aws.operators.neptune.NeptuneStartDbClusterOperator.defer"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.operators.neptune.handle_waitable_exception"
    )
    @mock.patch.object(NeptuneHook, "conn")
    def test_start_cluster_not_ready_defer(self, mock_conn, mock_wait, mock_defer):
        err_response = {
            "Error": {"Code": "InvalidClusterState", "Message": "Test message"}
        }
        exception = client("neptune").exceptions.ClientError(err_response, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.start_db_cluster.side_effect = exception
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=True,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        operator.execute(None)

        mock_wait.assert_called_once_with(
            operator=operator,
            err="InvalidClusterState",
        )
        assert mock_defer.call_count == 1

    @mock.patch.object(NeptuneHook, "get_waiter")
    @mock.patch.object(NeptuneHook, "conn")
    def test_start_cluster_instances_not_ready(self, mock_conn, mock_get_waiter):
        err_response = {
            "Error": {"Code": "InvalidDBInstanceState", "Message": "Test message"}
        }
        exception = client("neptune").exceptions.ClientError(err_response, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.start_db_cluster.side_effect = exception

        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )
        operator.execute(None)
        mock_get_waiter.assert_any_call("db_instance_available")

    @mock.patch(
        "airflow.providers.amazon.aws.operators.neptune.NeptuneStartDbClusterOperator.defer"
    )
    @mock.patch.object(NeptuneHook, "conn")
    def test_start_cluster_instances_not_ready_defer(self, mock_conn, mock_defer):
        """Tests both waiters are called if an instance exception is raised"""

        err_response = {
            "Error": {"Code": "InvalidDBInstanceState", "Message": "Test message"}
        }
        exception = client("neptune").exceptions.ClientError(err_response, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.start_db_cluster.side_effect = exception
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=True,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        operator.execute(None)

        # mock_defer.assert_has_calls(calls)
        assert mock_defer.call_count == 2

    def test_template_fields(self):
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=True,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )
        validate_template_fields(operator)


class TestNeptuneStopClusterOperator:
    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_stop_cluster_wait_for_completion(
        self, mock_hook_get_waiter, mock_get_cluster_status, mock_conn
    ):
        '''Test the waiter is only once when the cluster is "available"'''
        mock_get_cluster_status.return_value = "available"
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)
        mock_hook_get_waiter.assert_called_once_with("cluster_stopped")
        assert resp == EXPECTED_RESPONSE

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_stop_cluster_no_wait(
        self, mock_hook_get_waiter, mock_get_cluster_status, mock_conn
    ):
        mock_get_cluster_status.return_value = "available"

        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)
        mock_hook_get_waiter.assert_not_called()
        assert resp == EXPECTED_RESPONSE

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_stop_cluster_cluster_stopped(
        self, mock_waiter, mock_get_cluster_status, mock_conn
    ):
        mock_get_cluster_status.return_value = "stopped"
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)

        mock_conn.stop_db_cluster.assert_not_called()
        mock_waiter.assert_not_called()
        assert resp == {"db_cluster_id": CLUSTER_ID}

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_stop_cluster_cluster_error(
        self, mock_waiter, mock_get_cluster_status, mock_conn
    ):
        mock_get_cluster_status.return_value = "migration-failed"
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        with pytest.raises(AirflowException):
            operator.execute(None)

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_stop_cluster_not_in_available(
        self, mock_waiter, mock_get_cluster_status, mock_conn
    ):
        mock_get_cluster_status.return_value = "backing-up"
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        operator.execute(None)
        mock_waiter.assert_called_with("cluster_stopped")

    @mock.patch.object(NeptuneStopDbClusterOperator, "defer")
    @mock.patch.object(NeptuneHook, "conn")
    def test_stop_cluster_not_ready_defer(self, mock_conn, mock_defer):
        err_response = {
            "Error": {"Code": "InvalidClusterState", "Message": "Test message"}
        }
        exception = client("neptune").exceptions.ClientError(err_response, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.stop_db_cluster.side_effect = exception
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=True,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        operator.execute(None)
        assert mock_defer.call_count == 2

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_stop_cluster_instances_not_ready(
        self, mock_get_waiter, mock_get_cluster_status, mock_conn
    ):
        err_response = {
            "Error": {"Code": "InvalidDBInstanceState", "Message": "Test message"}
        }
        exception = client("neptune").exceptions.ClientError(err_response, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.stop_db_cluster.side_effect = exception

        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )
        operator.execute(None)
        mock_get_waiter.assert_any_call("db_instance_available")

    @mock.patch(
        "airflow.providers.amazon.aws.operators.neptune.NeptuneStopDbClusterOperator.defer"
    )
    @mock.patch.object(NeptuneHook, "conn")
    def test_stop_cluster_instances_not_ready_defer(self, mock_conn, mock_defer):
        """Tests both waiters are called if an instance exception is raised"""

        err_response = {
            "Error": {"Code": "InvalidDBInstanceState", "Message": "Test message"}
        }
        exception = client("neptune").exceptions.ClientError(err_response, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.stop_db_cluster.side_effect = exception
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=True,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        operator.execute(None)
        # setting the trigger to ANY because an actual trigger doesn't seem to pass the test, even though
        # the output (strings) are exactly the same. Suspect it may be an issue in the trigger's __eq__ function

        assert mock_defer.call_count == 2

    @mock.patch.object(NeptuneHook, "conn")
    def test_stop_cluster_deferrable(self, mock_conn):
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=True,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        with pytest.raises(TaskDeferred):
            operator.execute(None)

    def test_template_fields(self):
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=True,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )
        validate_template_fields(operator)
