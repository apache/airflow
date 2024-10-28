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

from datetime import datetime
from unittest import mock

import pytest
import time_machine
from moto import mock_aws

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3KeysUnchangedSensor
from airflow.utils import timezone
from airflow.utils.types import DagRunType

DEFAULT_DATE = datetime(2015, 1, 1)


class TestS3KeySensor:
    def test_bucket_name_none_and_bucket_key_as_relative_path(self):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided as relative path rather than s3:// url.
        :return:
        """
        op = S3KeySensor(task_id="s3_key_sensor", bucket_key="file_in_bucket")
        with pytest.raises(AirflowException):
            op.poke(None)

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    def test_bucket_name_none_and_bucket_key_is_list_and_contain_relative_path(
        self, mock_head_object
    ):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided with one of the two keys as relative path rather than s3:// url.
        :return:
        """
        mock_head_object.return_value = {"ContentLength": 0}
        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key=["s3://test_bucket/file", "file_in_bucket"],
        )
        with pytest.raises(AirflowException):
            op.poke(None)

    def test_bucket_name_provided_and_bucket_key_is_s3_url(self):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key is provided as a full s3:// url.
        :return:
        """
        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key="s3://test_bucket/file",
            bucket_name="test_bucket",
        )
        with pytest.raises(TypeError):
            op.poke(None)

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    def test_bucket_name_provided_and_bucket_key_is_list_and_contains_s3_url(
        self, mock_head_object
    ):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key contains a full s3:// url.
        :return:
        """
        mock_head_object.return_value = {"ContentLength": 0}
        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key=["test_bucket", "s3://test_bucket/file"],
            bucket_name="test_bucket",
        )
        with pytest.raises(TypeError):
            op.poke(None)

    @pytest.mark.parametrize(
        "key, bucket, parsed_key, parsed_bucket",
        [
            pytest.param("s3://bucket/key", None, "key", "bucket", id="key as s3url"),
            pytest.param("key", "bucket", "key", "bucket", id="separate bucket and key"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    def test_parse_bucket_key(
        self, mock_head_object, key, bucket, parsed_key, parsed_bucket
    ):
        print(key, bucket, parsed_key, parsed_bucket)
        mock_head_object.return_value = None

        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key=key,
            bucket_name=bucket,
        )

        op.poke(None)

        mock_head_object.assert_called_once_with(parsed_key, parsed_bucket)

    @pytest.mark.db_test
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    def test_parse_bucket_key_from_jinja(
        self, mock_head_object, session, clean_dags_and_dagruns
    ):
        mock_head_object.return_value = None

        Variable.set("test_bucket_key", "s3://bucket/key", session=session)

        execution_date = timezone.datetime(2020, 1, 1)

        dag = DAG("test_s3_key", schedule=None, start_date=execution_date)
        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key="{{ var.value.test_bucket_key }}",
            bucket_name=None,
            dag=dag,
        )

        dag_run = DagRun(
            dag_id=dag.dag_id,
            execution_date=execution_date,
            run_id="test",
            run_type=DagRunType.MANUAL,
        )
        ti = TaskInstance(task=op)
        ti.dag_run = dag_run
        session.add(ti)
        session.commit()
        context = ti.get_template_context(session)
        ti.render_templates(context)
        op.poke(None)

        mock_head_object.assert_called_once_with("key", "bucket")

    @pytest.mark.db_test
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    def test_parse_list_of_bucket_keys_from_jinja(
        self, mock_head_object, session, clean_dags_and_dagruns
    ):
        mock_head_object.return_value = None
        mock_head_object.side_effect = [{"ContentLength": 0}, {"ContentLength": 0}]

        Variable.set(
            "test_bucket_key", ["s3://bucket/file1", "s3://bucket/file2"], session=session
        )

        execution_date = timezone.datetime(2020, 1, 1)

        dag = DAG(
            "test_s3_key",
            schedule=None,
            start_date=execution_date,
            render_template_as_native_obj=True,
        )
        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key="{{ var.value.test_bucket_key }}",
            bucket_name=None,
            dag=dag,
        )

        dag_run = DagRun(
            dag_id=dag.dag_id,
            execution_date=execution_date,
            run_id="test",
            run_type=DagRunType.MANUAL,
        )
        ti = TaskInstance(task=op)
        ti.dag_run = dag_run
        session.add(ti)
        session.commit()
        context = ti.get_template_context(session)
        ti.render_templates(context)
        op.poke(None)

        mock_head_object.assert_any_call("file1", "bucket")
        mock_head_object.assert_any_call("file2", "bucket")

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    def test_poke(self, mock_head_object):
        op = S3KeySensor(task_id="s3_key_sensor", bucket_key="s3://test_bucket/file")

        mock_head_object.return_value = None
        assert op.poke(None) is False
        mock_head_object.assert_called_once_with("file", "test_bucket")

        mock_head_object.return_value = {"ContentLength": 0}
        assert op.poke(None) is True

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    def test_poke_multiple_files(self, mock_head_object):
        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key=["s3://test_bucket/file1", "s3://test_bucket/file2"],
        )

        mock_head_object.side_effect = [{"ContentLength": 0}, None]
        assert op.poke(None) is False

        mock_head_object.side_effect = [{"ContentLength": 0}, {"ContentLength": 0}]
        assert op.poke(None) is True

        mock_head_object.assert_any_call("file1", "test_bucket")
        mock_head_object.assert_any_call("file2", "test_bucket")

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.get_file_metadata")
    def test_poke_wildcard(self, mock_get_file_metadata):
        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key="s3://test_bucket/file*",
            wildcard_match=True,
        )

        mock_get_file_metadata.return_value = []
        assert op.poke(None) is False
        mock_get_file_metadata.assert_called_once_with("file", "test_bucket")

        mock_get_file_metadata.return_value = [{"Key": "dummyFile", "Size": 0}]
        assert op.poke(None) is False

        mock_get_file_metadata.return_value = [{"Key": "file1", "Size": 0}]
        assert op.poke(None) is True

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.get_file_metadata")
    def test_poke_wildcard_multiple_files(self, mock_get_file_metadata):
        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key=["s3://test_bucket/file*", "s3://test_bucket/*.zip"],
            wildcard_match=True,
        )

        mock_get_file_metadata.side_effect = [[{"Key": "file1", "Size": 0}], []]
        assert op.poke(None) is False

        mock_get_file_metadata.side_effect = [
            [{"Key": "file1", "Size": 0}],
            [{"Key": "file2", "Size": 0}],
        ]
        assert op.poke(None) is False

        mock_get_file_metadata.side_effect = [
            [{"Key": "file1", "Size": 0}],
            [{"Key": "test.zip", "Size": 0}],
        ]
        assert op.poke(None) is True

        mock_get_file_metadata.assert_any_call("file", "test_bucket")
        mock_get_file_metadata.assert_any_call("", "test_bucket")

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    def test_poke_with_check_function(self, mock_head_object):
        def check_fn(files: list) -> bool:
            return all(f.get("Size", 0) > 0 for f in files)

        op = S3KeySensor(
            task_id="s3_key_sensor", bucket_key="s3://test_bucket/file", check_fn=check_fn
        )

        mock_head_object.return_value = {"ContentLength": 0}
        assert op.poke(None) is False

        mock_head_object.return_value = {"ContentLength": 1}
        assert op.poke(None) is True

    @pytest.mark.parametrize(
        "key, pattern, expected",
        [
            ("test.csv", r"[a-z]+\.csv", True),
            ("test.txt", r"test/[a-z]+\.csv", False),
            ("test/test.csv", r"test/[a-z]+\.csv", True),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.get_file_metadata")
    def test_poke_with_use_regex(self, mock_get_file_metadata, key, pattern, expected):
        op = S3KeySensor(
            task_id="s3_key_sensor_async",
            bucket_key=pattern,
            bucket_name="test_bucket",
            use_regex=True,
        )
        mock_get_file_metadata.return_value = [{"Key": key, "Size": 0}]
        assert op.poke(None) is expected

    @mock.patch(
        "airflow.providers.amazon.aws.sensors.s3.S3KeySensor.poke", return_value=False
    )
    def test_s3_key_sensor_execute_complete_success_with_keys(self, mock_poke):
        """
        Asserts that a task is completed with success status and check function
        """

        def check_fn(files: list) -> bool:
            return all(f.get("Size", 0) > 0 for f in files)

        sensor = S3KeySensor(
            task_id="s3_key_sensor_async",
            bucket_key="key",
            bucket_name="bucket",
            check_fn=check_fn,
            deferrable=True,
        )
        assert (
            sensor.execute_complete(
                context={}, event={"status": "running", "files": [{"Size": 10}]}
            )
            is None
        )

    def test_fail_execute_complete(self):
        op = S3KeySensor(
            task_id="s3_key_sensor",
            bucket_key=["s3://test_bucket/file*", "s3://test_bucket/*.zip"],
            wildcard_match=True,
        )
        message = "error"
        with pytest.raises(AirflowException, match=message):
            op.execute_complete(context={}, event={"status": "error", "message": message})

    @mock_aws
    def test_custom_metadata_default_return_vals(self):
        def check_fn(files: list) -> bool:
            for f in files:
                if "Size" not in f:
                    return False
            return True

        hook = S3Hook()
        hook.create_bucket(bucket_name="test-bucket")
        hook.load_string(
            bucket_name="test-bucket",
            key="test-key",
            string_data="test-body",
        )

        op = S3KeySensor(
            task_id="test-metadata",
            bucket_key="test-key",
            bucket_name="test-bucket",
            metadata_keys=["Size"],
            check_fn=check_fn,
        )
        assert op.poke(None) is True
        op = S3KeySensor(
            task_id="test-metadata",
            bucket_key="test-key",
            bucket_name="test-bucket",
            metadata_keys=["Content"],
            check_fn=check_fn,
        )
        assert op.poke(None) is False

        op = S3KeySensor(
            task_id="test-metadata",
            bucket_key="test-key",
            bucket_name="test-bucket",
            check_fn=check_fn,
        )
        assert op.poke(None) is True

    @mock_aws
    def test_custom_metadata_default_custom_vals(self):
        def check_fn(files: list) -> bool:
            for f in files:
                if "LastModified" not in f or "ETag" not in f or "Size" in f:
                    return False
            return True

        hook = S3Hook()
        hook.create_bucket(bucket_name="test-bucket")
        hook.load_string(
            bucket_name="test-bucket",
            key="test-key",
            string_data="test-body",
        )

        op = S3KeySensor(
            task_id="test-metadata",
            bucket_key="test-key",
            bucket_name="test-bucket",
            metadata_keys=["LastModified", "ETag"],
            check_fn=check_fn,
        )
        assert op.poke(None) is True

    @mock_aws
    def test_custom_metadata_all_attributes(self):
        def check_fn(files: list) -> bool:
            hook = S3Hook()
            metadata_keys = set(
                hook.head_object(bucket_name="test-bucket", key="test-key").keys()
            )
            test_data_keys = set(files[0].keys())

            return test_data_keys == metadata_keys

        hook = S3Hook()
        hook.create_bucket(bucket_name="test-bucket")
        hook.load_string(
            bucket_name="test-bucket",
            key="test-key",
            string_data="test-body",
        )

        op = S3KeySensor(
            task_id="test-metadata",
            bucket_key="test-key",
            bucket_name="test-bucket",
            metadata_keys=["*"],
            check_fn=check_fn,
        )
        assert op.poke(None) is True

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.get_file_metadata")
    def test_custom_metadata_wildcard(self, mock_file_metadata, mock_head_object):
        def check_fn(files: list) -> bool:
            for f in files:
                if "ETag" not in f or "MissingMeta" not in f:
                    return False
            return True

        op = S3KeySensor(
            task_id="test-head-metadata",
            bucket_key=["s3://test-bucket/test-key*"],
            metadata_keys=["MissingMeta", "ETag"],
            check_fn=check_fn,
            wildcard_match=True,
        )

        mock_file_metadata.return_value = [{"Key": "test-key", "ETag": 0}]
        mock_head_object.return_value = {"MissingMeta": 0, "ContentLength": 100}
        assert op.poke(None) is True
        mock_head_object.assert_called_once()

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object")
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.get_file_metadata")
    def test_custom_metadata_wildcard_all_attributes(
        self, mock_file_metadata, mock_head_object
    ):
        def check_fn(files: list) -> bool:
            for f in files:
                if "ContentLength" not in f or "MissingMeta" not in f:
                    return False
            return True

        op = S3KeySensor(
            task_id="test-head-metadata",
            bucket_key=["s3://test-bucket/test-key*"],
            metadata_keys=["*"],
            check_fn=check_fn,
            wildcard_match=True,
        )

        mock_file_metadata.return_value = [{"Key": "test-key", "ETag": 0}]
        mock_head_object.return_value = {"MissingMeta": 0, "ContentLength": 100}
        assert op.poke(None) is True
        mock_head_object.assert_called_once()

        mock_head_object.return_value = {"MissingMeta": 0}
        assert op.poke(None) is False


class TestS3KeysUnchangedSensor:
    def setup_method(self):
        self.dag = DAG(
            "unit_tests_aws_sensor_test_schedule_dag_once",
            start_date=DEFAULT_DATE,
            schedule="@once",
        )

        self.sensor = S3KeysUnchangedSensor(
            task_id="sensor_1",
            bucket_name="test-bucket",
            prefix="test-prefix/path",
            inactivity_period=12,
            poke_interval=0.1,
            min_objects=1,
            allow_delete=True,
            dag=self.dag,
        )

    def test_reschedule_mode_not_allowed(self):
        with pytest.raises(ValueError):
            S3KeysUnchangedSensor(
                task_id="sensor_2",
                bucket_name="test-bucket",
                prefix="test-prefix/path",
                poke_interval=0.1,
                mode="reschedule",
                dag=self.dag,
            )

    @pytest.mark.db_test
    def test_render_template_fields(self, clean_dags_and_dagruns):
        S3KeysUnchangedSensor(
            task_id="sensor_3",
            bucket_name="test-bucket",
            prefix="test-prefix/path",
            inactivity_period=12,
            poke_interval=0.1,
            min_objects=1,
            allow_delete=True,
            dag=self.dag,
        ).render_template_fields({})

    @time_machine.travel(DEFAULT_DATE)
    def test_files_deleted_between_pokes_throw_error(self):
        self.sensor.allow_delete = False
        self.sensor.is_keys_unchanged({"a", "b"})
        with pytest.raises(AirflowException):
            self.sensor.is_keys_unchanged({"a"})

    @pytest.mark.parametrize(
        "current_objects, expected_returns, inactivity_periods",
        [
            pytest.param(
                ({"a"}, {"a", "b"}, {"a", "b", "c"}),
                (False, False, False),
                (0, 0, 0),
                id="resetting inactivity period after key change",
            ),
            pytest.param(
                ({"a", "b"}, {"a"}, {"a", "c"}),
                (False, False, False),
                (0, 0, 0),
                id="item was deleted with option `allow_delete=True`",
            ),
            pytest.param(
                ({"a"}, {"a"}, {"a"}),
                (False, False, True),
                (0, 10, 20),
                id="inactivity period was exceeded",
            ),
            pytest.param(
                (set(), set(), set()),
                (False, False, False),
                (0, 10, 20),
                id="not pass if empty key is given",
            ),
        ],
    )
    def test_key_changes(
        self, current_objects, expected_returns, inactivity_periods, time_machine
    ):
        time_machine.move_to(DEFAULT_DATE)
        for current, expected, period in zip(
            current_objects, expected_returns, inactivity_periods
        ):
            assert self.sensor.is_keys_unchanged(current) == expected
            assert self.sensor.inactivity_seconds == period
            time_machine.coordinates.shift(10)

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_poke_succeeds_on_upload_complete(self, mock_hook, time_machine):
        time_machine.move_to(DEFAULT_DATE)
        mock_hook.return_value.list_keys.return_value = {"a"}
        assert not self.sensor.poke(dict())
        time_machine.coordinates.shift(10)
        assert not self.sensor.poke(dict())
        time_machine.coordinates.shift(10)
        assert self.sensor.poke(dict())

    def test_fail_is_keys_unchanged(self):
        op = S3KeysUnchangedSensor(
            task_id="sensor", bucket_name="test-bucket", prefix="test-prefix/path"
        )
        op.previous_objects = {"1", "2", "3"}
        current_objects = {"1", "2"}
        op.allow_delete = False
        message = "Illegal behavior: objects were deleted in"
        with pytest.raises(AirflowException, match=message):
            op.is_keys_unchanged(current_objects=current_objects)

    def test_fail_execute_complete(self):
        op = S3KeysUnchangedSensor(
            task_id="sensor", bucket_name="test-bucket", prefix="test-prefix/path"
        )
        message = "test message"
        with pytest.raises(AirflowException, match=message):
            op.execute_complete(context={}, event={"status": "error", "message": message})
