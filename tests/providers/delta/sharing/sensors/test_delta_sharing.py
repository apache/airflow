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
import unittest
from unittest import mock

from airflow.providers.delta.sharing.sensors.delta_sharing import DeltaSharingSensor

TASK_ID = 'delta-sharing-sensor'
DEFAULT_CONN_ID = 'delta_sharing_default'
DEFAULT_SHARE = "share1"
DEFAULT_SCHEMA = "schema1"
DEFAULT_TABLE = "table1"
DEFAULT_RETRY_LIMIT = 3


class TestDeltaSharingDownloadToLocalOperator(unittest.TestCase):
    @mock.patch('airflow.providers.delta.sharing.sensors.delta_sharing.DeltaSharingHook')
    def test_poke_success(self, ds_mock_class):
        """
        Test the poke function in case where the run is successful.
        """

        sensor = DeltaSharingSensor(
            task_id=TASK_ID, share=DEFAULT_SHARE, schema=DEFAULT_SCHEMA, table=DEFAULT_TABLE
        )

        ds_mock = ds_mock_class.return_value
        ds_mock.get_table_version.return_value = 123

        with mock.patch(
            'airflow.providers.delta.sharing.sensors.delta_sharing.DeltaSharingSensor'
        ) as mock_sensor:
            mock_sensor.get_previous_version.return_value = 122
            mock_sensor.set_version.return_value = None
            result = sensor.poke(None)
            assert result

        ds_mock_class.assert_called_once_with(
            delta_sharing_conn_id='delta_sharing_default',
            retry_args=None,
            retry_delay=2.0,
            retry_limit=3,
            timeout_seconds=180,
            profile_file=None,
        )
        ds_mock.get_table_version.assert_called_once_with(
            DEFAULT_SHARE,
            DEFAULT_SCHEMA,
            DEFAULT_TABLE,
        )
