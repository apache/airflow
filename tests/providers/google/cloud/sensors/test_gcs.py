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

from datetime import datetime, timedelta, timezone
from unittest import mock

import pendulum
import pytest
from google.cloud.storage.retry import DEFAULT_RETRY

from airflow.exceptions import (
    AirflowProviderDeprecationWarning,
    AirflowSensorTimeout,
    AirflowSkipException,
    TaskDeferred,
)
from airflow.models.dag import DAG, AirflowException
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceAsyncSensor,
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
    ts_function,
)
from airflow.providers.google.cloud.triggers.gcs import (
    GCSBlobTrigger,
    GCSCheckBlobUpdateTimeTrigger,
    GCSPrefixBlobTrigger,
    GCSUploadSessionTrigger,
)

TEST_BUCKET = "TEST_BUCKET"

TEST_OBJECT = "TEST_OBJECT"

TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"

TEST_IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

TEST_PREFIX = "TEST_PREFIX"

TEST_DAG_ID = "unit_tests_gcs_sensor"

DEFAULT_DATE = datetime(2015, 1, 1)

MOCK_DATE_ARRAY = [datetime(2019, 2, 24, 12, 0, 0) - i * timedelta(seconds=10) for i in range(25)]

TEST_INACTIVITY_PERIOD = 5

TEST_MIN_OBJECTS = 1


@pytest.fixture()
def context():
    """
    Creates an empty context.
    """
    context = {"data_interval_end": datetime.utcnow()}
    yield context


def next_time_side_effect():
    """
    This each time this is called mock a time 10 seconds later
    than the previous call.
    """
    return MOCK_DATE_ARRAY.pop()


mock_time = mock.Mock(side_effect=next_time_side_effect)


class TestGoogleCloudStorageObjectSensor:
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_should_pass_argument_to_hook(self, mock_hook):
        task = GCSObjectExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.exists.return_value = True

        result = task.poke(mock.MagicMock())

        assert result is True
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.exists.assert_called_once_with(TEST_BUCKET, TEST_OBJECT, DEFAULT_RETRY)

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor.defer")
    def test_gcs_object_existence_sensor_finish_before_deferred(self, mock_defer, mock_hook):
        task = GCSObjectExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            deferrable=True,
        )
        mock_hook.return_value.exists.return_value = True
        task.execute(mock.MagicMock())
        assert not mock_defer.called

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_gcs_object_existence_sensor_deferred(self, mock_hook):
        """
        Asserts that a task is deferred and a GCSBlobTrigger will be fired
        when the GCSObjectExistenceSensor is executed and deferrable is set to True.
        """
        task = GCSObjectExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            deferrable=True,
        )
        mock_hook.return_value.exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, GCSBlobTrigger), "Trigger is not a GCSBlobTrigger"

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    def test_gcs_object_existence_sensor_deferred_execute_failure(self, soft_fail, expected_exception):
        """Tests that an AirflowException is raised in case of error event when deferrable is set to True"""
        task = GCSObjectExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            deferrable=True,
            soft_fail=soft_fail,
        )
        with pytest.raises(expected_exception):
            task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})

    def test_gcs_object_existence_sensor_execute_complete(self):
        """Asserts that logging occurs as expected when deferrable is set to True"""
        task = GCSObjectExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            deferrable=True,
        )
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
        mock_log_info.assert_called_with("File %s was found in bucket %s.", TEST_OBJECT, TEST_BUCKET)


class TestGoogleCloudStorageObjectAsyncSensor:
    depcrecation_message = (
        "Class `GCSObjectExistenceAsyncSensor` is deprecated and will be removed in a future release. "
        "Please use `GCSObjectExistenceSensor` and set `deferrable` attribute to `True` instead"
    )

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_gcs_object_existence_async_sensor(self, mock_hook):
        """
        Asserts that a task is deferred and a GCSBlobTrigger will be fired
        when the GCSObjectExistenceAsyncSensor is executed.
        """
        with pytest.warns(AirflowProviderDeprecationWarning, match=self.depcrecation_message):
            task = GCSObjectExistenceAsyncSensor(
                task_id="task-id",
                bucket=TEST_BUCKET,
                object=TEST_OBJECT,
                google_cloud_conn_id=TEST_GCP_CONN_ID,
            )
        mock_hook.return_value.exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, GCSBlobTrigger), "Trigger is not a GCSBlobTrigger"

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    def test_gcs_object_existence_async_sensor_execute_failure(self, soft_fail, expected_exception):
        """Tests that an AirflowException is raised in case of error event"""
        with pytest.warns(AirflowProviderDeprecationWarning, match=self.depcrecation_message):
            task = GCSObjectExistenceAsyncSensor(
                task_id="task-id",
                bucket=TEST_BUCKET,
                object=TEST_OBJECT,
                google_cloud_conn_id=TEST_GCP_CONN_ID,
                soft_fail=soft_fail,
            )
        with pytest.raises(expected_exception):
            task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})

    def test_gcs_object_existence_async_sensor_execute_complete(self):
        """Asserts that logging occurs as expected"""
        with pytest.warns(AirflowProviderDeprecationWarning, match=self.depcrecation_message):
            task = GCSObjectExistenceAsyncSensor(
                task_id="task-id",
                bucket=TEST_BUCKET,
                object=TEST_OBJECT,
                google_cloud_conn_id=TEST_GCP_CONN_ID,
            )
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
        mock_log_info.assert_called_with("File %s was found in bucket %s.", TEST_OBJECT, TEST_BUCKET)


class TestTsFunction:
    def test_should_support_datetime(self):
        context = {
            "dag": DAG(dag_id=TEST_DAG_ID, schedule=timedelta(days=5)),
            "execution_date": datetime(2019, 2, 14, 0, 0),
        }
        result = ts_function(context)
        assert datetime(2019, 2, 19, 0, 0, tzinfo=timezone.utc) == result

    def test_should_support_cron(self):
        dag = DAG(dag_id=TEST_DAG_ID, start_date=datetime(2019, 2, 19, 0, 0), schedule="@weekly")

        context = {
            "dag": dag,
            "execution_date": datetime(2019, 2, 19),
        }
        result = ts_function(context)
        assert pendulum.instance(datetime(2019, 2, 24)).isoformat() == result.isoformat()


class TestGoogleCloudStorageObjectUpdatedSensor:
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_should_pass_argument_to_hook(self, mock_hook):
        task = GCSObjectUpdateSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.is_updated_after.return_value = True
        result = task.poke(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.is_updated_after.assert_called_once_with(TEST_BUCKET, TEST_OBJECT, mock.ANY)
        assert result is True

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor.defer")
    def test_gcs_object_update_sensor_finish_before_deferred(self, mock_defer, mock_hook):
        task = GCSObjectUpdateSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        mock_hook.return_value.is_updated_after.return_value = True
        task.execute(mock.MagicMock())
        assert not mock_defer.called


class TestGCSObjectUpdateAsyncSensor:
    OPERATOR = GCSObjectUpdateSensor(
        task_id="gcs-obj-update",
        bucket=TEST_BUCKET,
        object=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        deferrable=True,
    )

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_gcs_object_update_async_sensor(self, mock_hook):
        """
        Asserts that a task is deferred and a GCSBlobTrigger will be fired
        when the GCSObjectUpdateAsyncSensor is executed.
        """
        mock_hook.return_value.is_updated_after.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(mock.MagicMock())
        assert isinstance(
            exc.value.trigger, GCSCheckBlobUpdateTimeTrigger
        ), "Trigger is not a GCSCheckBlobUpdateTimeTrigger"

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    def test_gcs_object_update_async_sensor_execute_failure(self, context, soft_fail, expected_exception):
        """Tests that an AirflowException is raised in case of error event"""
        self.OPERATOR.soft_fail = soft_fail
        with pytest.raises(expected_exception):
            self.OPERATOR.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_gcs_object_update_async_sensor_execute_complete(self, context):
        """Asserts that logging occurs as expected"""

        with mock.patch.object(self.OPERATOR.log, "info") as mock_log_info:
            self.OPERATOR.execute_complete(
                context=context, event={"status": "success", "message": "Job completed"}
            )
        mock_log_info.assert_called_with(
            "Checking last updated time for object %s in bucket : %s", TEST_OBJECT, TEST_BUCKET
        )


class TestGoogleCloudStoragePrefixSensor:
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_should_pass_arguments_to_hook(self, mock_hook):
        task = GCSObjectsWithPrefixExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list.return_value = ["NOT_EMPTY_LIST"]
        result = task.poke(mock.MagicMock)

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list.assert_called_once_with(TEST_BUCKET, prefix=TEST_PREFIX)
        assert result is True

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_should_return_false_on_empty_list(self, mock_hook):
        task = GCSObjectsWithPrefixExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
        )
        mock_hook.return_value.list.return_value = []
        result = task.poke(mock.MagicMock)

        assert result is False

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_execute(self, mock_hook):
        task = GCSObjectsWithPrefixExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            poke_interval=0,
        )
        generated_messages = [f"test-prefix/obj{i}" for i in range(5)]
        mock_hook.return_value.list.return_value = generated_messages

        response = task.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list.assert_called_once_with(TEST_BUCKET, prefix=TEST_PREFIX)
        assert response == generated_messages

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowSensorTimeout), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_execute_timeout(self, mock_hook, soft_fail, expected_exception):
        task = GCSObjectsWithPrefixExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            poke_interval=0,
            timeout=1,
            soft_fail=soft_fail,
        )
        mock_hook.return_value.list.return_value = []
        with pytest.raises(expected_exception):
            task.execute(mock.MagicMock)
            mock_hook.return_value.list.assert_called_once_with(TEST_BUCKET, prefix=TEST_PREFIX)

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor.defer")
    def test_gcs_object_prefix_existence_sensor_finish_before_deferred(self, mock_defer, mock_hook):
        task = GCSObjectsWithPrefixExistenceSensor(
            task_id="task-id",
            bucket=TEST_BUCKET,
            prefix=TEST_PREFIX,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        mock_hook.return_value.list.return_value = True
        task.execute(mock.MagicMock())
        assert not mock_defer.called


class TestGCSObjectsWithPrefixExistenceAsyncSensor:
    OPERATOR = GCSObjectsWithPrefixExistenceSensor(
        task_id="gcs-obj-prefix",
        bucket=TEST_BUCKET,
        prefix=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        deferrable=True,
    )

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_gcs_object_with_prefix_existence_async_sensor(self, mock_hook):
        """
        Asserts that a task is deferred and a GCSPrefixBlobTrigger will be fired
        when the GCSObjectsWithPrefixExistenceAsyncSensor is executed.
        """
        mock_hook.return_value.list.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(mock.MagicMock())
        assert isinstance(exc.value.trigger, GCSPrefixBlobTrigger), "Trigger is not a GCSPrefixBlobTrigger"

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    def test_gcs_object_with_prefix_existence_async_sensor_execute_failure(
        self, context, soft_fail, expected_exception
    ):
        """Tests that an AirflowException is raised in case of error event"""
        self.OPERATOR.soft_fail = soft_fail
        with pytest.raises(expected_exception):
            self.OPERATOR.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_gcs_object_with_prefix_existence_async_sensor_execute_complete(self, context):
        """Asserts that logging occurs as expected"""

        with mock.patch.object(self.OPERATOR.log, "info") as mock_log_info:
            self.OPERATOR.execute_complete(
                context=context,
                event={"status": "success", "message": "Job completed", "matches": [TEST_OBJECT]},
            )
        mock_log_info.assert_called_with("Resuming from trigger and checking status")


class TestGCSUploadSessionCompleteSensor:
    def setup_method(self):
        self.dag = DAG(
            TEST_DAG_ID + "test_schedule_dag_once",
            schedule="@once",
            start_date=DEFAULT_DATE,
        )

        self.sensor = GCSUploadSessionCompleteSensor(
            task_id="sensor_1",
            bucket="test-bucket",
            prefix="test-prefix/path",
            inactivity_period=12,
            poke_interval=10,
            min_objects=1,
            allow_delete=False,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            dag=self.dag,
        )

        self.last_mocked_date = datetime(2019, 4, 24, 0, 0, 0)

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_get_gcs_hook(self, mock_hook):
        self.sensor._get_gcs_hook()
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        assert mock_hook.return_value == self.sensor.hook

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.google.cloud.sensors.gcs.get_time", mock_time)
    def test_files_deleted_between_pokes_throw_error(self, soft_fail, expected_exception):
        self.sensor.soft_fail = soft_fail
        self.sensor.is_bucket_updated({"a", "b"})
        with pytest.raises(expected_exception):
            self.sensor.is_bucket_updated({"a"})

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.get_time", mock_time)
    def test_files_deleted_between_pokes_allow_delete(self):
        self.sensor = GCSUploadSessionCompleteSensor(
            task_id="sensor_2",
            bucket="test-bucket",
            prefix="test-prefix/path",
            inactivity_period=12,
            poke_interval=10,
            min_objects=1,
            allow_delete=True,
            dag=self.dag,
        )
        self.sensor.is_bucket_updated({"a", "b"})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({"a"})
        assert len(self.sensor.previous_objects) == 1
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({"a", "c"})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({"a", "d"})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({"a", "d"})
        assert self.sensor.inactivity_seconds == 10
        assert self.sensor.is_bucket_updated({"a", "d"})

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.get_time", mock_time)
    def test_incoming_data(self):
        self.sensor.is_bucket_updated({"a"})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({"a", "b"})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({"a", "b", "c"})
        assert self.sensor.inactivity_seconds == 0

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.get_time", mock_time)
    def test_no_new_data(self):
        self.sensor.is_bucket_updated({"a"})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({"a"})
        assert self.sensor.inactivity_seconds == 10

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.get_time", mock_time)
    def test_no_new_data_success_criteria(self):
        self.sensor.is_bucket_updated({"a"})
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated({"a"})
        assert self.sensor.inactivity_seconds == 10
        assert self.sensor.is_bucket_updated({"a"})

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.get_time", mock_time)
    def test_not_enough_objects(self):
        self.sensor.is_bucket_updated(set())
        assert self.sensor.inactivity_seconds == 0
        self.sensor.is_bucket_updated(set())
        assert self.sensor.inactivity_seconds == 10
        assert not self.sensor.is_bucket_updated(set())


class TestGCSUploadSessionCompleteAsyncSensor:
    OPERATOR = GCSUploadSessionCompleteSensor(
        task_id="gcs-obj-session",
        bucket=TEST_BUCKET,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        prefix=TEST_OBJECT,
        inactivity_period=TEST_INACTIVITY_PERIOD,
        min_objects=TEST_MIN_OBJECTS,
        deferrable=True,
    )

    @mock.patch("airflow.providers.google.cloud.sensors.gcs.GCSHook")
    def test_gcs_upload_session_complete_async_sensor(self, mock_hook):
        """
        Asserts that a task is deferred and a GCSUploadSessionTrigger will be fired
        when the GCSUploadSessionCompleteAsyncSensor is executed.
        """
        mock_hook.return_value.is_bucket_updated.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(mock.MagicMock())
        assert isinstance(
            exc.value.trigger, GCSUploadSessionTrigger
        ), "Trigger is not a GCSUploadSessionTrigger"

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    def test_gcs_upload_session_complete_sensor_execute_failure(self, context, soft_fail, expected_exception):
        """Tests that an AirflowException is raised in case of error event"""

        self.OPERATOR.soft_fail = soft_fail
        with pytest.raises(expected_exception):
            self.OPERATOR.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_gcs_upload_session_complete_async_sensor_execute_complete(self, context):
        """Asserts that execute complete is completed as expected"""

        assert self.OPERATOR.execute_complete(
            context=context, event={"status": "success", "message": "success"}
        )
