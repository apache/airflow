# -*- coding: utf-8 -*-
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
import datetime
from unittest import TestCase, mock

import pendulum

from airflow.exceptions import AirflowSensorTimeout
from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor, \
    GoogleCloudStorageObjectUpdatedSensor, ts_function, GoogleCloudStoragePrefixSensor

TEST_DAG_ID = 'test-dag_id'

TEST_BUCKET = "TEST_BUCKET"

TEST_OBJECT = "TEST_OBJECT"

TEST_DELEGATE_TO = "TEST_DELEGATE_TO"

TEST_GCP_CONN_ID = 'TEST_GCP_CONN_ID'

TEST_PREFIX = "TEST_PREFIX"


class TestGoogleCloudStorageObjectSensor(TestCase):

    @mock.patch("airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageHook")
    def test_should_pass_argument_to_hook(self, mock_hook):
        task = GoogleCloudStorageObjectSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
        )
        mock_hook.return_value.exists.return_value = True

        result = task.poke(mock.MagicMock())

        self.assertEqual(True, result)
        mock_hook.assert_called_once_with(
            delegate_to=TEST_DELEGATE_TO,
            google_cloud_storage_conn_id=TEST_GCP_CONN_ID
        )
        mock_hook.return_value.exists.assert_called_once_with(TEST_BUCKET, TEST_OBJECT)


class TestTsFunction(TestCase):

    def test_should_support_datetime(self):
        context = {
            'dag': DAG(dag_id=TEST_DAG_ID, schedule_interval=datetime.timedelta(days=5)),
            'execution_date': datetime.datetime(2019, 2, 14, 0, 0)
        }
        result = ts_function(context)
        self.assertEqual(datetime.datetime(2019, 2, 19, 0, 0), result)

    def test_should_support_cron(self):
        dag = DAG(
            dag_id=TEST_DAG_ID,
            start_date=datetime.datetime(2019, 2, 19, 0, 0),
            schedule_interval='@weekly'
        )

        context = {
            'dag': dag,
            'execution_date': datetime.datetime(2019, 2, 19),
        }
        result = ts_function(context)
        self.assertEqual(pendulum.instance(datetime.datetime(2019, 2, 24)).isoformat(), result.isoformat())


class TestGoogleCloudStorageObjectUpdatedSensor(TestCase):

    @mock.patch("airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageHook")
    def test_should_pass_argument_to_hook(self, mock_hook):
        task = GoogleCloudStorageObjectUpdatedSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
        )
        mock_hook.return_value.is_updated_after.return_value = True
        result = task.poke(mock.MagicMock())

        mock_hook.assert_called_once_with(
            delegate_to=TEST_DELEGATE_TO,
            google_cloud_storage_conn_id=TEST_GCP_CONN_ID
        )
        mock_hook.return_value.is_updated_after.assert_called_once_with(TEST_BUCKET, TEST_OBJECT, mock.ANY)
        self.assertEqual(True, result)


class TestGoogleCloudStoragePrefixSensor(TestCase):

    @mock.patch("airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageHook")
    def test_should_pass_arguments_to_hook(self, mock_hook):
        task = GoogleCloudStoragePrefixSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
        )
        mock_hook.return_value.list.return_value = ["NOT_EMPTY_LIST"]
        result = task.poke(mock.MagicMock)

        mock_hook.assert_called_once_with(
            delegate_to=TEST_DELEGATE_TO,
            google_cloud_storage_conn_id=TEST_GCP_CONN_ID
        )
        mock_hook.return_value.list.assert_called_once_with(TEST_BUCKET, prefix=TEST_PREFIX)
        self.assertEqual(True, result)

    @mock.patch("airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageHook")
    def test_should_return_false_on_empty_list(self, mock_hook):
        task = GoogleCloudStoragePrefixSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
        )
        mock_hook.return_value.list.return_value = []
        result = task.poke(mock.MagicMock)

        self.assertEqual(False, result)

    @mock.patch('airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageHook')
    def test_execute(self, mock_hook):
        task = GoogleCloudStoragePrefixSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            poke_interval=0)
        generated_messages = ['test-prefix/obj%s' % i for i in range(5)]
        mock_hook.return_value.list.return_value = generated_messages

        response = task.execute(None)

        mock_hook.assert_called_once_with(
            delegate_to=TEST_DELEGATE_TO,
            google_cloud_storage_conn_id=TEST_GCP_CONN_ID
        )
        mock_hook.return_value.list.assert_called_once_with(TEST_BUCKET, prefix=TEST_PREFIX)
        self.assertEqual(response, generated_messages)

    @mock.patch('airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageHook')
    def test_execute_timeout(self, mock_hook):
        task = GoogleCloudStoragePrefixSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            poke_interval=0,
            timeout=1)
        mock_hook.return_value.list.return_value = []
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(mock.MagicMock)
            mock_hook.return_value.list.assert_called_once_with(
                TEST_BUCKET, prefix=TEST_PREFIX)
