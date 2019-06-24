# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE object
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this object
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this object except in compliance
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
import unittest.mock as mock
from datetime import datetime, timedelta

from airflow import configuration, AirflowException
from airflow import models, DAG
from airflow.contrib.sensors import gcs_sensor
from airflow.settings import Session

TEST_DAG_ID = 'unit_tests'
DEFAULT_DATE = datetime(2015, 1, 1)
configuration.load_test_config()


def reset(dag_id=TEST_DAG_ID):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()


reset()

MOCK_DATE_ARRAY = [datetime(2019, 2, 24, 12, 0, 0) - i * timedelta(seconds=10)
                   for i in range(20)]


def next_time_side_effect():
    """
    This each time this is called mock a time 10 seconds later
    than the previous call.
    """
    return MOCK_DATE_ARRAY.pop()


mock_time = mock.Mock(side_effect=next_time_side_effect)


class GoogleCloudStorageUploadSessionCompleteSensorTest(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'provide_context': True
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'
        self.dag = dag

        self.sensor = gcs_sensor.GoogleCloudStorageUploadSessionCompleteSensor(
            task_id='sensor',
            bucket='test-bucket',
            prefix='test-prefix/path',
            inactivity_period=12,
            poke_interval=10,
            min_objects=1,
            allow_delete=False,
            previous_num_objects=0,
            dag=self.dag
        )
        self.last_mocked_date = datetime(2019, 4, 24, 0, 0, 0)

    @mock.patch('airflow.contrib.sensors.gcs_sensor.get_time', mock_time)
    def test_files_deleted_between_pokes_throw_error(self):
        self.sensor.is_bucket_updated(2)
        with self.assertRaises(AirflowException):
            self.sensor.is_bucket_updated(1)

    @mock.patch('airflow.contrib.sensors.gcs_sensor.get_time', mock_time)
    def test_files_deleted_between_pokes_allow_delete(self):
        self.sensor = gcs_sensor.GoogleCloudStorageUploadSessionCompleteSensor(
            task_id='sensor',
            bucket='test-bucket',
            prefix='test-prefix/path',
            inactivity_period=12,
            poke_interval=10,
            min_objects=1,
            allow_delete=True,
            previous_num_objects=0,
            dag=self.dag
        )
        self.sensor.is_bucket_updated(2)
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated(1)
        self.assertEqual(self.sensor.previous_num_objects, 1)
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated(2)
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated(2)
        self.assertEqual(self.sensor.inactivity_seconds, 10)
        self.assertTrue(self.sensor.is_bucket_updated(2))

    @mock.patch('airflow.contrib.sensors.gcs_sensor.get_time', mock_time)
    def test_incoming_data(self):
        self.sensor.is_bucket_updated(2)
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated(3)
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated(4)
        self.assertEqual(self.sensor.inactivity_seconds, 0)

    @mock.patch('airflow.contrib.sensors.gcs_sensor.get_time', mock_time)
    def test_no_new_data(self):
        self.sensor.is_bucket_updated(2)
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated(2)
        self.assertEqual(self.sensor.inactivity_seconds, 10)

    @mock.patch('airflow.contrib.sensors.gcs_sensor.get_time', mock_time)
    def test_no_new_data_success_criteria(self):
        self.sensor.is_bucket_updated(2)
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated(2)
        self.assertEqual(self.sensor.inactivity_seconds, 10)
        self.assertTrue(self.sensor.is_bucket_updated(2))

    @mock.patch('airflow.contrib.sensors.gcs_sensor.get_time', mock_time)
    def test_not_enough_objects(self):
        self.sensor.is_bucket_updated(0)
        self.assertEqual(self.sensor.inactivity_seconds, 0)
        self.sensor.is_bucket_updated(0)
        self.assertEqual(self.sensor.inactivity_seconds, 10)
        self.assertFalse(self.sensor.is_bucket_updated(0))


if __name__ == '__main__':
    unittest.main()
