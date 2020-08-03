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

from datetime import datetime
from unittest import TestCase, mock

from freezegun import freeze_time
from parameterized import parameterized

from airflow.models.dag import DAG, AirflowException
from airflow.providers.amazon.aws.sensors.s3_keys_unchanged import S3KeysUnchangedSensor

TEST_DAG_ID = 'unit_tests_aws_sensor'
DEFAULT_DATE = datetime(2015, 1, 1)


class TestS3KeysUnchangedSensor(TestCase):

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'
        self.dag = dag

        self.sensor = S3KeysUnchangedSensor(
            task_id='sensor_1',
            bucket_name='test-bucket',
            prefix='test-prefix/path',
            inactivity_period=12,
            poke_interval=0.1,
            min_objects=1,
            allow_delete=True,
            dag=self.dag
        )

    def test_reschedule_mode_not_allowed(self):
        with self.assertRaises(ValueError):
            S3KeysUnchangedSensor(
                task_id='sensor_2',
                bucket_name='test-bucket',
                prefix='test-prefix/path',
                poke_interval=0.1,
                mode='reschedule',
                dag=self.dag
            )

    @freeze_time(DEFAULT_DATE, auto_tick_seconds=10)
    def test_files_deleted_between_pokes_throw_error(self):
        self.sensor.allow_delete = False
        self.sensor.is_keys_unchanged({'a', 'b'})
        with self.assertRaises(AirflowException):
            self.sensor.is_keys_unchanged({'a'})

    @parameterized.expand([
        # Test: resetting inactivity period after key change
        (({'a'}, {'a', 'b'}, {'a', 'b', 'c'}), (False, False, False), (0, 0, 0)),
        # ..and in case an item was deleted with option `allow_delete=True`
        (({'a', 'b'}, {'a'}, {'a', 'c'}), (False, False, False), (0, 0, 0)),
        # Test: passes after inactivity period was exceeded
        (({'a'}, {'a'}, {'a'}), (False, False, True), (0, 10, 20)),
        # ..and do not pass if empty key is given
        ((set(), set(), set()), (False, False, False), (0, 10, 20))
    ])
    @freeze_time(DEFAULT_DATE, auto_tick_seconds=10)
    def test_key_changes(self, current_objects, expected_returns, inactivity_periods):
        self.assertEqual(self.sensor.is_keys_unchanged(current_objects[0]), expected_returns[0])
        self.assertEqual(self.sensor.inactivity_seconds, inactivity_periods[0])
        self.assertEqual(self.sensor.is_keys_unchanged(current_objects[1]), expected_returns[1])
        self.assertEqual(self.sensor.inactivity_seconds, inactivity_periods[1])
        self.assertEqual(self.sensor.is_keys_unchanged(current_objects[2]), expected_returns[2])
        self.assertEqual(self.sensor.inactivity_seconds, inactivity_periods[2])

    @freeze_time(DEFAULT_DATE, auto_tick_seconds=10)
    @mock.patch('airflow.providers.amazon.aws.sensors.s3_keys_unchanged.S3Hook')
    def test_poke_succeeds_on_upload_complete(self, mock_hook):
        mock_hook.return_value.list_keys.return_value = {'a'}
        self.assertFalse(self.sensor.poke(dict()))
        self.assertFalse(self.sensor.poke(dict()))
        self.assertTrue(self.sensor.poke(dict()))
