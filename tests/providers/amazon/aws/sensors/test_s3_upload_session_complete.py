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

from datetime import datetime, timedelta
from unittest import TestCase, mock

from airflow.models.dag import DAG, AirflowException
from airflow.providers.amazon.aws.sensors.s3_upload_session_complete import S3UploadSessionCompleteSensor

TEST_BUCKET = "TEST_BUCKET"
TEST_OBJECT = "TEST_OBJECT"
TEST_DELEGATE_TO = "TEST_DELEGATE_TO"
TEST_DAG_ID = 'unit_tests_aws_sensor'
DEFAULT_DATE = datetime(2015, 1, 1)
MOCK_DATE_ARRAY = [datetime(2019, 2, 24, 12, 0, 0) - i * timedelta(seconds=10)
                   for i in range(25)]


def next_time_side_effect():
    """
    This each time this is called mock a time 10 seconds later
    than the previous call.
    """
    return MOCK_DATE_ARRAY.pop()


mock_time = mock.Mock(side_effect=next_time_side_effect)


class TestS3UploadSessionCompleteSensor(TestCase):

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'
        self.dag = dag

        self.sensor = S3UploadSessionCompleteSensor(
            task_id='sensor_1',
            bucket_name='test-bucket',
            prefix='test-prefix/path',
            inactivity_period=12,
            poke_interval=10,
            min_objects=1,
            allow_delete=False,
            dag=self.dag
        )

        self.last_mocked_date = datetime(2019, 4, 24, 0, 0, 0)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_upload_session_complete.get_time', mock_time)
    def test_files_deleted_between_pokes_throw_error(self):
        self.sensor.is_bucket_updated({'a', 'b'})
        with self.assertRaises(AirflowException):
            self.sensor.is_bucket_updated({'a'})

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_upload_session_complete.get_time', mock_time)
    def test_files_deleted_between_pokes_allow_delete(self):
        self.sensor = S3UploadSessionCompleteSensor(
            task_id='sensor_2',
            bucket_name='test-bucket',
            prefix='test-prefix/path',
            inactivity_period=12,
            poke_interval=10,
            min_objects=1,
            allow_delete=True,
            dag=self.dag
        )
        self.sensor.is_bucket_updated({'a', 'b'})
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated({'a'})
        self.assertEqual(len(self.sensor.previous_objects), 1)
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated({'a', 'c'})
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated({'a', 'd'})
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated({'a', 'd'})
        self.assertEqual(self.sensor.inactivity_seconds, 10)
        self.assertTrue(self.sensor.is_bucket_updated({'a', 'd'}))

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_upload_session_complete.get_time', mock_time)
    def test_incoming_data(self):
        self.sensor.is_bucket_updated({'a'})
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated({'a', 'b'})
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated({'a', 'b', 'c'})
        self.assertEqual(self.sensor.inactivity_seconds, 0)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_upload_session_complete.get_time', mock_time)
    def test_no_new_data(self):
        self.sensor.is_bucket_updated({'a'})
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated({'a'})
        self.assertEqual(self.sensor.inactivity_seconds, 10)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_upload_session_complete.get_time', mock_time)
    def test_no_new_data_success_criteria(self):
        self.sensor.is_bucket_updated({'a'})
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated({'a'})
        self.assertEqual(self.sensor.inactivity_seconds, 10)
        self.assertTrue(self.sensor.is_bucket_updated({'a'}))

    @mock.patch('airflow.providers.amazon.aws.sensors.s3_upload_session_complete.get_time', mock_time)
    def test_not_enough_objects(self):
        self.sensor.is_bucket_updated(set())
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated(set())
        self.assertEqual(self.sensor.inactivity_seconds, 10)
        self.assertFalse(self.sensor.is_bucket_updated(set()))
