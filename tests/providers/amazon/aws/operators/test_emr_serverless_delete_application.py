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

from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.emr import EmrServerlessDeleteApplicationOperator

MOCK_DATA = {
    'application_id': 'test_emr_serverless_delete_application_operator',
    'task_id': 'test_emr_serverless_task_id',
}


class TestEmrServerlessDeleteOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_delete_application_with_wait_for_completion_successfully(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.stop_application.return_value = {}
        mock_conn.delete_application.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=MOCK_DATA['task_id'], application_id=MOCK_DATA['application_id']
        )

        operator.execute(None)

        assert operator.wait_for_completion is True

        assert mock_waiter.call_count == 2

        mock_conn.stop_application.assert_called_once()

        mock_conn.delete_application.assert_called_once_with(applicationId=MOCK_DATA['application_id'])

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_delete_application_without_wait_for_completion_successfully(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.stop_application.return_value = {}
        mock_conn.delete_application.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=MOCK_DATA['task_id'],
            application_id=MOCK_DATA['application_id'],
            wait_for_completion=False,
        )

        operator.execute(None)

        assert operator.wait_for_completion is False

        mock_waiter.assert_called_once()

        mock_conn.stop_application.assert_called_once()

        mock_conn.delete_application.assert_called_once_with(applicationId=MOCK_DATA['application_id'])

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrServerlessHook.conn")
    def test_delete_application_failed_deleteion(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True
        mock_conn.stop_application.return_value = {}
        mock_conn.delete_application.return_value = {'ResponseMetadata': {'HTTPStatusCode': 400}}

        operator = EmrServerlessDeleteApplicationOperator(
            task_id=MOCK_DATA['task_id'], application_id=MOCK_DATA['application_id']
        )
        with pytest.raises(AirflowException) as ex_message:
            operator.execute(None)

        assert "Application deletion failed:" in str(ex_message.value)

        assert operator.wait_for_completion is True

        mock_waiter.assert_called_once()

        mock_conn.stop_application.assert_called_once()

        mock_conn.delete_application.assert_called_once_with(applicationId=MOCK_DATA['application_id'])
