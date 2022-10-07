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

import unittest
from unittest import mock

import pytest

from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime

TEST_DAG_ID = 'unit_tests'
DEFAULT_DATE = datetime(2018, 1, 1)
ATHENA_QUERY_ID = 'eac29bf8-daa1-4ffc-b19a-0db31dc3b784'

MOCK_DATA = {
    'task_id': 'test_athena_operator',
    'query': 'SELECT * FROM TEST_TABLE',
    'database': 'TEST_DATABASE',
    'outputLocation': 's3://test_s3_bucket/',
    'client_request_token': 'eac427d0-1c6d-4dfb-96aa-2835d3ac6595',
    'workgroup': 'primary',
}

query_context = {'Database': MOCK_DATA['database']}
result_configuration = {'OutputLocation': MOCK_DATA['outputLocation']}


class TestAthenaOperator(unittest.TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
        }

        self.dag = DAG(f'{TEST_DAG_ID}test_schedule_dag_once', default_args=args, schedule='@once')

        self.athena = AthenaOperator(
            task_id='test_athena_operator',
            query='SELECT * FROM TEST_TABLE',
            database='TEST_DATABASE',
            output_location='s3://test_s3_bucket/',
            client_request_token='eac427d0-1c6d-4dfb-96aa-2835d3ac6595',
            sleep_time=0,
            max_polling_attempts=3,
            dag=self.dag,
        )

    def test_init(self):
        assert self.athena.task_id == MOCK_DATA['task_id']
        assert self.athena.query == MOCK_DATA['query']
        assert self.athena.database == MOCK_DATA['database']
        assert self.athena.aws_conn_id == 'aws_default'
        assert self.athena.client_request_token == MOCK_DATA['client_request_token']
        assert self.athena.sleep_time == 0

        assert self.athena.hook.sleep_time == 0

    @mock.patch.object(AthenaHook, 'check_query_status', side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, 'run_query', return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, 'get_conn')
    def test_hook_run_small_success_query(self, mock_conn, mock_run_query, mock_check_query_status):
        self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA['query'],
            query_context,
            result_configuration,
            MOCK_DATA['client_request_token'],
            MOCK_DATA['workgroup'],
        )
        assert mock_check_query_status.call_count == 1

    @mock.patch.object(
        AthenaHook,
        'check_query_status',
        side_effect=(
            "RUNNING",
            "RUNNING",
            "SUCCEEDED",
        ),
    )
    @mock.patch.object(AthenaHook, 'run_query', return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, 'get_conn')
    def test_hook_run_big_success_query(self, mock_conn, mock_run_query, mock_check_query_status):
        self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA['query'],
            query_context,
            result_configuration,
            MOCK_DATA['client_request_token'],
            MOCK_DATA['workgroup'],
        )
        assert mock_check_query_status.call_count == 3

    @mock.patch.object(
        AthenaHook,
        'check_query_status',
        side_effect=(
            None,
            None,
        ),
    )
    @mock.patch.object(AthenaHook, 'run_query', return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, 'get_conn')
    def test_hook_run_failed_query_with_none(self, mock_conn, mock_run_query, mock_check_query_status):
        with pytest.raises(Exception):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA['query'],
            query_context,
            result_configuration,
            MOCK_DATA['client_request_token'],
            MOCK_DATA['workgroup'],
        )
        assert mock_check_query_status.call_count == 3

    @mock.patch.object(AthenaHook, 'get_state_change_reason')
    @mock.patch.object(
        AthenaHook,
        'check_query_status',
        side_effect=(
            "RUNNING",
            "FAILED",
        ),
    )
    @mock.patch.object(AthenaHook, 'run_query', return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, 'get_conn')
    def test_hook_run_failure_query(
        self, mock_conn, mock_run_query, mock_check_query_status, mock_get_state_change_reason
    ):
        with pytest.raises(Exception):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA['query'],
            query_context,
            result_configuration,
            MOCK_DATA['client_request_token'],
            MOCK_DATA['workgroup'],
        )
        assert mock_check_query_status.call_count == 2
        assert mock_get_state_change_reason.call_count == 1

    @mock.patch.object(
        AthenaHook,
        'check_query_status',
        side_effect=(
            "RUNNING",
            "RUNNING",
            "CANCELLED",
        ),
    )
    @mock.patch.object(AthenaHook, 'run_query', return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, 'get_conn')
    def test_hook_run_cancelled_query(self, mock_conn, mock_run_query, mock_check_query_status):
        with pytest.raises(Exception):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA['query'],
            query_context,
            result_configuration,
            MOCK_DATA['client_request_token'],
            MOCK_DATA['workgroup'],
        )
        assert mock_check_query_status.call_count == 3

    @mock.patch.object(
        AthenaHook,
        'check_query_status',
        side_effect=(
            "RUNNING",
            "RUNNING",
            "RUNNING",
        ),
    )
    @mock.patch.object(AthenaHook, 'run_query', return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, 'get_conn')
    def test_hook_run_failed_query_with_max_tries(self, mock_conn, mock_run_query, mock_check_query_status):
        with pytest.raises(Exception):
            self.athena.execute({})
        mock_run_query.assert_called_once_with(
            MOCK_DATA['query'],
            query_context,
            result_configuration,
            MOCK_DATA['client_request_token'],
            MOCK_DATA['workgroup'],
        )
        assert mock_check_query_status.call_count == 3

    @mock.patch.object(AthenaHook, 'check_query_status', side_effect=("SUCCEEDED",))
    @mock.patch.object(AthenaHook, 'run_query', return_value=ATHENA_QUERY_ID)
    @mock.patch.object(AthenaHook, 'get_conn')
    def test_return_value(self, mock_conn, mock_run_query, mock_check_query_status):
        """Test we return the right value -- that will get put in to XCom by the execution engine"""
        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=self.athena)
        ti.dag_run = dag_run

        assert self.athena.execute(ti.get_template_context()) == ATHENA_QUERY_ID
