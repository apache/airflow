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
#
import shutil
import unittest
from unittest import mock

import pytest
import responses

from airflow.providers.delta_sharing.hooks.delta_sharing import DeltaSharingQueryResult
from airflow.providers.delta_sharing.operators.delta_sharing import DeltaSharingDownloadToLocalOperator

TASK_ID = 'delta-sharing-operator'
DEFAULT_CONN_ID = 'delta_sharing_default'
DEFAULT_SHARE = "share1"
DEFAULT_SCHEMA = "schema1"
DEFAULT_TABLE = "table1"
DEFAULT_LOCATION = "/tmp/test-delta-sharing-operator"
DEFAULT_RETRY_LIMIT = 3


class TestDeltaSharingDownloadToLocalOperator(unittest.TestCase):
    def tearDown(self) -> None:
        shutil.rmtree(DEFAULT_LOCATION, ignore_errors=True)

    def test_init_with_wrong_parameters(self):
        """
        Test the initializer with incorrect parameters.
        """

        with pytest.raises(ValueError):
            DeltaSharingDownloadToLocalOperator(
                task_id=TASK_ID,
                share=DEFAULT_SHARE,
                schema=DEFAULT_SCHEMA,
                table=DEFAULT_TABLE,
                limit=-1,
                location=DEFAULT_LOCATION,
            )
        with pytest.raises(ValueError):
            DeltaSharingDownloadToLocalOperator(
                task_id=TASK_ID,
                share=DEFAULT_SHARE,
                schema=DEFAULT_SCHEMA,
                table=DEFAULT_TABLE,
                location=DEFAULT_LOCATION,
                num_parallel_downloads=0,
            )

    @mock.patch('airflow.providers.delta_sharing.operators.delta_sharing.DeltaSharingHook')
    def test_exec_success_no_files(self, ds_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        op = DeltaSharingDownloadToLocalOperator(
            task_id=TASK_ID,
            share=DEFAULT_SHARE,
            schema=DEFAULT_SCHEMA,
            table=DEFAULT_TABLE,
            location=DEFAULT_LOCATION,
            save_stats=False,
            save_metadata=False,
        )
        ds_mock = ds_mock_class.return_value
        ds_mock.query_table.return_value = DeltaSharingQueryResult(123, {}, {}, [])

        op.execute(None)

        ds_mock_class.assert_called_once_with(
            delta_sharing_conn_id='delta_sharing_default',
            retry_args=None,
            retry_delay=2.0,
            retry_limit=3,
            timeout_seconds=180,
        )

        ds_mock.query_table.assert_called_once_with(
            DEFAULT_SHARE,
            DEFAULT_SCHEMA,
            DEFAULT_TABLE,
            limit=None,
            predicates=None,
        )

    @mock.patch('airflow.providers.delta_sharing.operators.delta_sharing.DeltaSharingHook')
    @responses.activate
    def test_exec_success_with_files(self, ds_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        op = DeltaSharingDownloadToLocalOperator(
            task_id=TASK_ID,
            share=DEFAULT_SHARE,
            schema=DEFAULT_SCHEMA,
            table=DEFAULT_TABLE,
            location=DEFAULT_LOCATION,
        )
        ds_mock = ds_mock_class.return_value
        file = {'url': 'https://www.com/share/table/123.parquet', 'size': 10, 'stats': '{"numRecords":1}'}
        ds_mock.query_table.return_value = DeltaSharingQueryResult(123, {}, {}, [file])

        responses.add(responses.GET, file['url'], body='1234567890')
        op.execute(None)

        ds_mock_class.assert_called_once_with(
            delta_sharing_conn_id='delta_sharing_default',
            retry_args=None,
            retry_delay=2.0,
            retry_limit=3,
            timeout_seconds=180,
        )
        ds_mock.query_table.assert_called_once_with(
            DEFAULT_SHARE,
            DEFAULT_SCHEMA,
            DEFAULT_TABLE,
            limit=None,
            predicates=None,
        )
