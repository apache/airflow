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

from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.triggers.s3 import S3KeyTrigger
from airflow.triggers.base import TriggerEvent

TEST_KEY = ["test-key", "test-key"]
TEST_BUCKET = "test_bucket"
TEST_CONN_ID = "test_conn"
TEST_VERIFY = True
TEST_WILDCARD_MATCH = False
TEST_POLL_INTERVAL = 100


class TestS3KeySensor:
    def test_s3_key_trigger_serialization(self):
        s3_key_trigger = S3KeyTrigger(
            bucket_key=TEST_KEY[0],
            bucket_name=TEST_BUCKET,
            wildcard_match=TEST_WILDCARD_MATCH,
            aws_conn_id=TEST_CONN_ID,
            verify=TEST_VERIFY,
            poll_interval=TEST_POLL_INTERVAL,
        )

        class_path, args = s3_key_trigger.serialize()

        assert class_path == "airflow.providers.amazon.aws.triggers.s3.S3KeyTrigger"

        assert args["bucket_key"] == TEST_KEY[0]
        assert args["bucket_name"] == TEST_BUCKET
        assert args["aws_conn_id"] == TEST_CONN_ID
        assert args["wildcard_match"] is TEST_WILDCARD_MATCH
        assert args["verify"] is TEST_VERIFY
        assert isinstance(args["poll_interval"], int)
        assert args["poll_interval"] == TEST_POLL_INTERVAL

    @pytest.mark.asyncio
    @mock.patch.object(S3Hook, "head_object_async")
    async def test_s3_key_trigger_run(self, mock):
        mock.return_value = {
            "ContentLength": 123,
        }

        s3_key_trigger = S3KeyTrigger(
            bucket_key=TEST_KEY[0],
            bucket_name=TEST_BUCKET,
            wildcard_match=TEST_WILDCARD_MATCH,
            aws_conn_id=TEST_CONN_ID,
            verify=TEST_VERIFY,
            poll_interval=TEST_POLL_INTERVAL,
        )

        generator = s3_key_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "message": "S3KeyTrigger success", "files_list": [[{"Size": 123}]]}
        )

    @pytest.mark.asyncio
    @mock.patch.object(S3Hook, "get_file_metadata_async")
    async def test_s3_key_sensor_trigger_run_with_wildcard(self, mock_get_file_metadata_async):
        mock_get_file_metadata_async.return_value = [
            {
                "Key": "test-key",
                "Size": 11,
            },
        ]

        s3_key_trigger = S3KeyTrigger(
            bucket_key=TEST_KEY[0],
            bucket_name=TEST_BUCKET,
            wildcard_match=True,
            aws_conn_id=TEST_CONN_ID,
            verify=TEST_VERIFY,
            poll_interval=TEST_POLL_INTERVAL,
        )

        generator = s3_key_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "message": "S3KeyTrigger success", "files_list": [[{"Size": 11}]]}
        )

    @pytest.mark.asyncio
    async def test_deferrable_poke_bucket_name_none_and_bucket_key_as_relative_path(self):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided as relative path rather than s3:// url.
        :return:
        """
        op = S3KeyTrigger(bucket_key="file_in_bucket")
        with pytest.raises(AirflowException):
            await op.poke()

    @pytest.mark.asyncio
    @mock.patch.object(S3Hook, "head_object_async")
    async def test_deferrable_poke_bucket_name_none_and_bucket_key_is_list_and_contain_relative_path(
        self, mock_head_object_async
    ):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided with one of the two keys as relative path rather than s3:// url.
        :return:
        """
        mock_head_object_async.return_value = {"ContentLength": 0}
        op = S3KeyTrigger(bucket_key=["s3://test_bucket/file", "file_in_bucket"])
        with pytest.raises(AirflowException):
            await op.poke()

    @pytest.mark.asyncio
    async def test_deferrable_poke_bucket_name_provided_and_bucket_key_is_s3_url(self):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key is provided as a full s3:// url.
        :return:
        """
        op = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
        with pytest.raises(TypeError):
            await op.poke()

    @pytest.mark.asyncio
    @mock.patch.object(S3Hook, "head_object_async")
    async def test_deferrable_poke_bucket_name_provided_and_bucket_key_is_list_and_contains_s3_url(
        self, mock_head_object_async
    ):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key contains a full s3:// url.
        :return:
        """
        mock_head_object_async.return_value = {"ContentLength": 0}
        op = S3KeyTrigger(
            bucket_key=["test_bucket", "s3://test_bucket/file"],
            bucket_name="test_bucket",
        )
        with pytest.raises(TypeError):
            await op.poke()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "key, bucket, parsed_key, parsed_bucket",
        [
            pytest.param("s3://bucket/key", None, "key", "bucket", id="key as s3url"),
            pytest.param("key", "bucket", "key", "bucket", id="separate bucket and key"),
        ],
    )
    @mock.patch.object(S3Hook, "head_object_async")
    async def test_deferrable_poke_parse_bucket_key(
        self, mock_head_object_async, key, bucket, parsed_key, parsed_bucket
    ):
        print(key, bucket, parsed_key, parsed_bucket)
        mock_head_object_async.return_value = None

        op = S3KeyTrigger(
            bucket_key=key,
            bucket_name=bucket,
        )
        await op.poke()

        mock_head_object_async.assert_called_once_with(parsed_key, parsed_bucket)

    @pytest.mark.asyncio
    @mock.patch.object(S3Hook, "head_object_async")
    async def test_deferrable_poke_multiple_files(self, mock_head_object_async):
        s3_key_trigger = S3KeyTrigger(
            bucket_key=["s3://test_bucket/file1", "s3://test_bucket/file2"],
            wildcard_match=False,
            aws_conn_id=TEST_CONN_ID,
            verify=TEST_VERIFY,
            poll_interval=TEST_POLL_INTERVAL,
        )
        mock_head_object_async.side_effect = [{"ContentLength": 0}, None]

        response = await s3_key_trigger.poke()
        assert response == [False, [[{"Size": 0}]]]

        mock_head_object_async.side_effect = [{"ContentLength": 0}, {"ContentLength": 0}]

        response = await s3_key_trigger.poke()
        assert response == [True, [[{"Size": 0}], [{"Size": 0}]]]

        mock_head_object_async.assert_any_call("file1", "test_bucket")
        mock_head_object_async.assert_any_call("file2", "test_bucket")

    @pytest.mark.asyncio
    @mock.patch.object(S3Hook, "get_file_metadata_async")
    async def test_poke_deferrable_wildcard(self, mock_get_file_metadata):
        op = S3KeyTrigger(bucket_key="s3://test_bucket/file*", wildcard_match=True)

        mock_get_file_metadata.return_value = []
        assert await op.poke() == [False, []]
        mock_get_file_metadata.assert_called_once_with("file", "test_bucket")

        mock_get_file_metadata.return_value = [{"Key": "dummyFile", "Size": 0}]
        assert await op.poke() == [False, []]

        mock_get_file_metadata.return_value = [{"Key": "file1", "Size": 12}]
        assert await op.poke() == [True, [[{"Size": 12}]]]

    @pytest.mark.asyncio
    @mock.patch.object(S3Hook, "get_file_metadata_async")
    async def test_poke_deferrable_wildcard_multiple_files(self, mock_get_file_metadata_async):
        op = S3KeyTrigger(
            bucket_key=["s3://test_bucket/file*", "s3://test_bucket/*.zip"],
            wildcard_match=True,
        )

        mock_get_file_metadata_async.side_effect = [[{"Key": "file1", "Size": 123}], []]
        assert await op.poke() == [False, [[{"Size": 123}]]]

        mock_get_file_metadata_async.side_effect = [
            [{"Key": "file1", "Size": 123}],
            [{"Key": "file2", "Size": 456}],
        ]
        assert await op.poke() == [False, [[{"Size": 123}]]]

        mock_get_file_metadata_async.side_effect = [
            [{"Key": "file1", "Size": 123}],
            [{"Key": "test.zip", "Size": 456}],
        ]
        assert await op.poke() == [True, [[{"Size": 123}], [{"Size": 456}]]]

        mock_get_file_metadata_async.assert_any_call("file", "test_bucket")
        mock_get_file_metadata_async.assert_any_call("", "test_bucket")

    @pytest.mark.asyncio
    @mock.patch.object(S3Hook, "head_object_async")
    async def test_poke_with_check_function(self, mock_head_object_async):
        def check_fn(files: list) -> bool:
            return all(f.get("Size", 0) > 0 for f in files)

        mock_head_object_async.return_value = {"ContentLength": 1}
        trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file")
        generator = trigger.run()
        trigger_response = await generator.asend(None)
        assert trigger_response == TriggerEvent(
            {
                "status": "success",
                "message": "S3KeyTrigger success",
                "files_list": [[{"Size": 1}]],
            }
        )
        op = S3KeySensor(
            task_id="test_poke_with_check_function",
            bucket_key="s3://test_bucket/file",
            check_fn=check_fn,
        )
        response = op.execute_complete(None, event=trigger_response.payload)

        assert response is True

        mock_head_object_async.return_value = {"ContentLength": 0}
        trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file")
        generator = trigger.run()
        trigger_response = await generator.asend(None)

        op = S3KeySensor(
            task_id="test_poke_with_check_function",
            bucket_key="s3://test_bucket/file",
            check_fn=check_fn,
        )
        response = op.execute_complete(None, event=trigger_response.payload)

        assert response is False

    @pytest.mark.asyncio
    @mock.patch.object(S3Hook, "head_object_async")
    async def test_poke_with_check_function_with_multiple_files(self, mock_head_object_async):
        def check_fn(files: list) -> bool:
            return all(f.get("Size", 0) > 0 for f in files)

        mock_head_object_async.side_effect = [{"ContentLength": 0}, {"ContentLength": 0}]
        trigger = S3KeyTrigger(bucket_key=["s3://test_bucket/file", "s3://test_bucket_2/file"])
        generator = trigger.run()
        trigger_response = await generator.asend(None)
        assert trigger_response == TriggerEvent(
            {
                "status": "success",
                "message": "S3KeyTrigger success",
                "files_list": [[{"Size": 0}], [{"Size": 0}]],
            }
        )
        op = S3KeySensor(
            task_id="test_poke_with_check_function",
            bucket_key=["s3://test_bucket/file", "s3://test_bucket_2/file"],
            check_fn=check_fn,
        )
        response = op.execute_complete(None, event=trigger_response.payload)

        assert response is False

        mock_head_object_async.side_effect = [{"ContentLength": 0}, {"ContentLength": 1}]
        trigger = S3KeyTrigger(bucket_key=["s3://test_bucket/file", "s3://test_bucket_2/file"])
        generator = trigger.run()
        trigger_response = await generator.asend(None)

        assert trigger_response == TriggerEvent(
            {
                "status": "success",
                "message": "S3KeyTrigger success",
                "files_list": [[{"Size": 0}], [{"Size": 1}]],
            }
        )
        op = S3KeySensor(
            task_id="test_poke_with_check_function",
            bucket_key=["s3://test_bucket/file", "s3://test_bucket_2/file"],
            check_fn=check_fn,
        )
        response = op.execute_complete(None, event=trigger_response.payload)

        assert response is False

        mock_head_object_async.side_effect = [{"ContentLength": 123}, {"ContentLength": 456}]
        trigger = S3KeyTrigger(bucket_key=["s3://test_bucket/file", "s3://test_bucket_2/file"])
        generator = trigger.run()
        trigger_response = await generator.asend(None)

        assert trigger_response == TriggerEvent(
            {
                "status": "success",
                "message": "S3KeyTrigger success",
                "files_list": [[{"Size": 123}], [{"Size": 456}]],
            }
        )
        op = S3KeySensor(
            task_id="test_poke_with_check_function",
            bucket_key=["s3://test_bucket/file", "s3://test_bucket_2/file"],
            check_fn=check_fn,
        )
        response = op.execute_complete(None, event=trigger_response.payload)

        assert response is True
