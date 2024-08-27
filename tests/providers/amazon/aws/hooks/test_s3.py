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

import gzip as gz
import inspect
import os
import re
from datetime import datetime as std_datetime, timezone
from unittest import mock, mock as async_mock
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from urllib.parse import parse_qs

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.amazon.aws.exceptions import S3HookUriParseFailure
from airflow.providers.amazon.aws.hooks.s3 import (
    NO_ACL,
    S3Hook,
    provide_bucket_name,
    unify_bucket_name_and_key,
)
from airflow.providers.common.compat.assets import Asset
from airflow.utils.timezone import datetime
from tests.test_utils.compat import AIRFLOW_V_2_10_PLUS


@pytest.fixture
def mocked_s3_res():
    with mock_aws():
        yield boto3.resource("s3")


@pytest.fixture
def s3_bucket(mocked_s3_res):
    bucket = "airflow-test-s3-bucket"
    mocked_s3_res.create_bucket(Bucket=bucket)
    return bucket


if AIRFLOW_V_2_10_PLUS:

    @pytest.fixture
    def hook_lineage_collector():
        from airflow.lineage import hook
        from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

        hook._hook_lineage_collector = None
        hook._hook_lineage_collector = hook.HookLineageCollector()

        yield get_hook_lineage_collector()

        hook._hook_lineage_collector = None


class TestAwsS3Hook:
    @mock_aws
    def test_get_conn(self):
        hook = S3Hook()
        assert hook.get_conn() is not None

    def test_resource(self):
        hook = S3Hook()
        assert hook.resource is not None

    def test_use_threads_default_value(self):
        hook = S3Hook()
        assert hook.transfer_config.use_threads is True

    def test_use_threads_set_value(self):
        hook = S3Hook(transfer_config_args={"use_threads": False})
        assert hook.transfer_config.use_threads is False

    @pytest.mark.parametrize("transfer_config_args", [1, True, '{"use_threads": false}'])
    def test_transfer_config_args_invalid(self, transfer_config_args):
        with pytest.raises(TypeError, match="transfer_config_args expected dict, got .*"):
            S3Hook(transfer_config_args=transfer_config_args)

    @pytest.mark.parametrize(
        "url, expected",
        [
            pytest.param(
                "s3://test/this/is/not/a-real-key.txt", ("test", "this/is/not/a-real-key.txt"), id="s3 style"
            ),
            pytest.param(
                "s3a://test/this/is/not/a-real-key.txt",
                ("test", "this/is/not/a-real-key.txt"),
                id="s3a style",
            ),
            pytest.param(
                "s3n://test/this/is/not/a-real-key.txt",
                ("test", "this/is/not/a-real-key.txt"),
                id="s3n style",
            ),
            pytest.param(
                "https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/test.jpg",
                ("DOC-EXAMPLE-BUCKET1", "test.jpg"),
                id="path style",
            ),
            pytest.param(
                "https://DOC-EXAMPLE-BUCKET1.s3.us-west-2.amazonaws.com/test.png",
                ("DOC-EXAMPLE-BUCKET1", "test.png"),
                id="virtual hosted style",
            ),
            pytest.param(
                "s3://test/this/is/not/a-real-key #2.txt",
                ("test", "this/is/not/a-real-key #2.txt"),
                id="s3 style with #",
            ),
            pytest.param(
                "s3a://test/this/is/not/a-real-key #2.txt",
                ("test", "this/is/not/a-real-key #2.txt"),
                id="s3a style with #",
            ),
            pytest.param(
                "s3n://test/this/is/not/a-real-key #2.txt",
                ("test", "this/is/not/a-real-key #2.txt"),
                id="s3n style with #",
            ),
            pytest.param(
                "https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/test #2.jpg",
                ("DOC-EXAMPLE-BUCKET1", "test #2.jpg"),
                id="path style with #",
            ),
            pytest.param(
                "https://DOC-EXAMPLE-BUCKET1.s3.us-west-2.amazonaws.com/test #2.png",
                ("DOC-EXAMPLE-BUCKET1", "test #2.png"),
                id="virtual hosted style with #",
            ),
        ],
    )
    def test_parse_s3_url(self, url: str, expected: tuple[str, str]):
        assert S3Hook.parse_s3_url(url) == expected, "Incorrect parsing of the s3 url"

    def test_parse_invalid_s3_url_virtual_hosted_style(self):
        with pytest.raises(
            S3HookUriParseFailure,
            match=(
                "Please provide a bucket name using a valid virtually hosted format which should "
                "be of the form: https://bucket-name.s3.region-code.amazonaws.com/key-name but "
                'provided: "https://DOC-EXAMPLE-BUCKET1.us-west-2.amazonaws.com/test.png"'
            ),
        ):
            S3Hook.parse_s3_url("https://DOC-EXAMPLE-BUCKET1.us-west-2.amazonaws.com/test.png")

    def test_parse_s3_object_directory(self):
        parsed = S3Hook.parse_s3_url("s3://test/this/is/not/a-real-s3-directory/")
        assert parsed == ("test", "this/is/not/a-real-s3-directory/"), "Incorrect parsing of the s3 url"

    def test_get_s3_bucket_key_valid_full_s3_url(self):
        bucket, key = S3Hook.get_s3_bucket_key(None, "s3://test/test.txt", "", "")
        assert bucket == "test"
        assert key == "test.txt"

    def test_get_s3_bucket_key_valid_bucket_and_key(self):
        bucket, key = S3Hook.get_s3_bucket_key("test", "test.txt", "", "")
        assert bucket == "test"
        assert key == "test.txt"

    def test_get_s3_bucket_key_incompatible(self):
        with pytest.raises(TypeError):
            S3Hook.get_s3_bucket_key("test", "s3://test/test.txt", "", "")

    def test_check_for_bucket(self, s3_bucket):
        hook = S3Hook()
        assert hook.check_for_bucket(s3_bucket) is True
        assert hook.check_for_bucket("not-a-bucket") is False

    @mock_aws
    def test_get_bucket(self):
        hook = S3Hook()
        assert hook.get_bucket("bucket") is not None

    @mock_aws
    def test_create_bucket_default_region(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        assert hook.get_bucket("new_bucket") is not None

    @mock_aws
    def test_create_bucket_us_standard_region(self, monkeypatch):
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)

        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket", region_name="us-east-1")
        bucket = hook.get_bucket("new_bucket")
        assert bucket is not None
        region = bucket.meta.client.get_bucket_location(Bucket=bucket.name).get("LocationConstraint")
        # https://github.com/spulec/moto/pull/1961
        # If location is "us-east-1", LocationConstraint should be None
        assert region is None

    @mock_aws
    def test_create_bucket_other_region(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket", region_name="us-east-2")
        bucket = hook.get_bucket("new_bucket")
        assert bucket is not None
        region = bucket.meta.client.get_bucket_location(Bucket=bucket.name).get("LocationConstraint")
        assert region == "us-east-2"

    @mock_aws
    @pytest.mark.parametrize("region_name", ["eu-west-1", "us-east-1"])
    def test_create_bucket_regional_endpoint(self, region_name, monkeypatch):
        conn = Connection(
            conn_id="regional-endpoint",
            conn_type="aws",
            extra={
                "config_kwargs": {"s3": {"us_east_1_regional_endpoint": "regional"}},
            },
        )
        with mock.patch.dict("os.environ", values={f"AIRFLOW_CONN_{conn.conn_id.upper()}": conn.get_uri()}):
            monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
            hook = S3Hook(aws_conn_id=conn.conn_id)
            bucket_name = f"regional-{region_name}"
            hook.create_bucket(bucket_name, region_name=region_name)
            bucket = hook.get_bucket(bucket_name)
            assert bucket is not None
            assert bucket.name == bucket_name
            region = bucket.meta.client.get_bucket_location(Bucket=bucket.name).get("LocationConstraint")
            assert region == (region_name if region_name != "us-east-1" else None)

    def test_create_bucket_no_region_regional_endpoint(self, monkeypatch):
        conn = Connection(
            conn_id="no-region-regional-endpoint",
            conn_type="aws",
            extra={"config_kwargs": {"s3": {"us_east_1_regional_endpoint": "regional"}}},
        )
        with mock.patch.dict("os.environ", values={f"AIRFLOW_CONN_{conn.conn_id.upper()}": conn.get_uri()}):
            monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
            hook = S3Hook(aws_conn_id=conn.conn_id)
            error_message = (
                "Unable to create bucket if `region_name` not set and boto3 "
                r"configured to use s3 regional endpoints\."
            )
            with pytest.raises(AirflowException, match=error_message):
                hook.create_bucket("unable-to-create")

    def test_check_for_prefix(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="a", Body=b"a")
        bucket.put_object(Key="dir/b", Body=b"b")

        assert hook.check_for_prefix(bucket_name=s3_bucket, prefix="dir/", delimiter="/") is True
        assert hook.check_for_prefix(bucket_name=s3_bucket, prefix="a", delimiter="/") is False

    def test_list_prefixes(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="a", Body=b"a")
        bucket.put_object(Key="dir/b", Body=b"b")
        bucket.put_object(Key="dir/sub_dir/c", Body=b"c")

        assert [] == hook.list_prefixes(s3_bucket, prefix="non-existent/")
        assert [] == hook.list_prefixes(s3_bucket)
        assert ["dir/"] == hook.list_prefixes(s3_bucket, delimiter="/")
        assert [] == hook.list_prefixes(s3_bucket, prefix="dir/")
        assert ["dir/sub_dir/"] == hook.list_prefixes(s3_bucket, delimiter="/", prefix="dir/")
        assert [] == hook.list_prefixes(s3_bucket, prefix="dir/sub_dir/")

    def test_list_prefixes_paged(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)

        # we don't need to test the paginator that's covered by boto tests
        keys = [f"{i}/b" for i in range(2)]
        dirs = [f"{i}/" for i in range(2)]
        for key in keys:
            bucket.put_object(Key=key, Body=b"a")

        assert sorted(dirs) == sorted(hook.list_prefixes(s3_bucket, delimiter="/", page_size=1))

    def test_list_keys(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="a", Body=b"a")
        bucket.put_object(Key="ba", Body=b"ab")
        bucket.put_object(Key="bxa", Body=b"axa")
        bucket.put_object(Key="bxb", Body=b"axb")
        bucket.put_object(Key="dir/b", Body=b"b")

        from_datetime = datetime(1992, 3, 8, 18, 52, 51)
        to_datetime = datetime(1993, 3, 14, 21, 52, 42)

        def dummy_object_filter(keys, from_datetime=None, to_datetime=None):
            return []

        assert [] == hook.list_keys(s3_bucket, prefix="non-existent/")
        assert ["a", "ba", "bxa", "bxb", "dir/b"] == hook.list_keys(s3_bucket)
        assert ["a", "ba", "bxa", "bxb"] == hook.list_keys(s3_bucket, delimiter="/")
        assert ["dir/b"] == hook.list_keys(s3_bucket, prefix="dir/")
        assert ["ba", "bxa", "bxb", "dir/b"] == hook.list_keys(s3_bucket, start_after_key="a")
        assert [] == hook.list_keys(s3_bucket, from_datetime=from_datetime, to_datetime=to_datetime)
        assert [] == hook.list_keys(
            s3_bucket, from_datetime=from_datetime, to_datetime=to_datetime, object_filter=dummy_object_filter
        )
        assert [] == hook.list_keys(s3_bucket, prefix="*a")
        assert ["a", "ba", "bxa"] == hook.list_keys(s3_bucket, prefix="*a", apply_wildcard=True)
        assert [] == hook.list_keys(s3_bucket, prefix="b*a")
        assert ["ba", "bxa"] == hook.list_keys(s3_bucket, prefix="b*a", apply_wildcard=True)
        assert [] == hook.list_keys(s3_bucket, prefix="b*")
        assert ["ba", "bxa", "bxb"] == hook.list_keys(s3_bucket, prefix="b*", apply_wildcard=True)

    def test_list_keys_paged(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)

        keys = [str(i) for i in range(2)]
        for key in keys:
            bucket.put_object(Key=key, Body=b"a")

        assert sorted(keys) == sorted(hook.list_keys(s3_bucket, delimiter="/", page_size=1))

    def test_get_file_metadata(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="test", Body=b"a")

        assert len(hook.get_file_metadata("t", s3_bucket)) == 1
        assert hook.get_file_metadata("t", s3_bucket)[0]["Size"] is not None
        assert len(hook.get_file_metadata("test", s3_bucket)) == 1
        assert len(hook.get_file_metadata("a", s3_bucket)) == 0

    def test_head_object(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="a", Body=b"a")

        assert hook.head_object("a", s3_bucket) is not None
        assert hook.head_object(f"s3://{s3_bucket}//a") is not None
        assert hook.head_object("b", s3_bucket) is None
        assert hook.head_object(f"s3://{s3_bucket}//b") is None

    def test_check_for_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="a", Body=b"a")

        assert hook.check_for_key("a", s3_bucket) is True
        assert hook.check_for_key(f"s3://{s3_bucket}//a") is True
        assert hook.check_for_key("b", s3_bucket) is False
        assert hook.check_for_key(f"s3://{s3_bucket}//b") is False

    def test_get_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="a", Body=b"a")

        assert hook.get_key("a", s3_bucket).key == "a"
        assert hook.get_key(f"s3://{s3_bucket}/a").key == "a"

    def test_read_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="my_key", Body=b"Cont\xc3\xa9nt")

        assert hook.read_key("my_key", s3_bucket) == "Contént"

    # As of 1.3.2, Moto doesn't support select_object_content yet.
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_client_type")
    def test_select_key(self, mock_get_client_type, s3_bucket):
        mock_get_client_type.return_value.select_object_content.return_value = {
            "Payload": [{"Records": {"Payload": b"Cont\xc3"}}, {"Records": {"Payload": b"\xa9nt"}}]
        }
        hook = S3Hook()
        assert hook.select_key("my_key", s3_bucket) == "Contént"

    def test_check_for_wildcard_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="abc", Body=b"a")
        bucket.put_object(Key="a/b", Body=b"a")
        bucket.put_object(Key="foo_5.txt", Body=b"a")

        assert hook.check_for_wildcard_key("a*", s3_bucket) is True
        assert hook.check_for_wildcard_key("abc", s3_bucket) is True
        assert hook.check_for_wildcard_key(f"s3://{s3_bucket}//a*") is True
        assert hook.check_for_wildcard_key(f"s3://{s3_bucket}//abc") is True

        assert hook.check_for_wildcard_key("a", s3_bucket) is False
        assert hook.check_for_wildcard_key("b", s3_bucket) is False
        assert hook.check_for_wildcard_key(f"s3://{s3_bucket}//a") is False
        assert hook.check_for_wildcard_key(f"s3://{s3_bucket}//b") is False

        assert hook.get_wildcard_key("a?b", s3_bucket).key == "a/b"
        assert hook.get_wildcard_key("a?c", s3_bucket, delimiter="/").key == "abc"
        assert hook.get_wildcard_key("foo_[0-9].txt", s3_bucket, delimiter="/").key == "foo_5.txt"
        assert hook.get_wildcard_key(f"s3://{s3_bucket}/foo_[0-9].txt", delimiter="/").key == "foo_5.txt"

    def test_get_wildcard_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key="abc", Body=b"a")
        bucket.put_object(Key="a/b", Body=b"a")
        bucket.put_object(Key="foo_5.txt", Body=b"a")

        # The boto3 Class API is _odd_, and we can't do an isinstance check as
        # each instance is a different class, so lets just check one property
        # on S3.Object. Not great but...
        assert hook.get_wildcard_key("a*", s3_bucket).key == "a/b"
        assert hook.get_wildcard_key("a*", s3_bucket, delimiter="/").key == "abc"
        assert hook.get_wildcard_key("abc", s3_bucket, delimiter="/").key == "abc"
        assert hook.get_wildcard_key(f"s3://{s3_bucket}/a*").key == "a/b"
        assert hook.get_wildcard_key(f"s3://{s3_bucket}/a*", delimiter="/").key == "abc"
        assert hook.get_wildcard_key(f"s3://{s3_bucket}/abc", delimiter="/").key == "abc"

        assert hook.get_wildcard_key("a", s3_bucket) is None
        assert hook.get_wildcard_key("b", s3_bucket) is None
        assert hook.get_wildcard_key(f"s3://{s3_bucket}/a") is None
        assert hook.get_wildcard_key(f"s3://{s3_bucket}/b") is None

        assert hook.get_wildcard_key("a?b", s3_bucket).key == "a/b"
        assert hook.get_wildcard_key("a?c", s3_bucket, delimiter="/").key == "abc"
        assert hook.get_wildcard_key("foo_[0-9].txt", s3_bucket, delimiter="/").key == "foo_5.txt"
        assert hook.get_wildcard_key(f"s3://{s3_bucket}/foo_[0-9].txt", delimiter="/").key == "foo_5.txt"

    def test_load_string(self, s3_bucket):
        hook = S3Hook()
        hook.load_string("Contént", "my_key", s3_bucket)
        resource = boto3.resource("s3").Object(s3_bucket, "my_key")
        assert resource.get()["Body"].read() == b"Cont\xc3\xa9nt"

    @pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Hook lineage works in Airflow >= 2.10.0")
    def test_load_string_exposes_lineage(self, s3_bucket, hook_lineage_collector):
        hook = S3Hook()

        hook.load_string("Contént", "my_key", s3_bucket)
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"s3://{s3_bucket}/my_key"
        )

    def test_load_string_compress(self, s3_bucket):
        hook = S3Hook()
        hook.load_string("Contént", "my_key", s3_bucket, compression="gzip")
        resource = boto3.resource("s3").Object(s3_bucket, "my_key")
        data = gz.decompress(resource.get()["Body"].read())
        assert data == b"Cont\xc3\xa9nt"

    def test_load_string_compress_exception(self, s3_bucket):
        hook = S3Hook()
        with pytest.raises(NotImplementedError):
            hook.load_string("Contént", "my_key", s3_bucket, compression="bad-compression")

    def test_load_string_acl(self, s3_bucket):
        hook = S3Hook()
        hook.load_string("Contént", "my_key", s3_bucket, acl_policy="public-read")
        response = boto3.client("s3").get_object_acl(Bucket=s3_bucket, Key="my_key", RequestPayer="requester")
        assert response["Grants"][1]["Permission"] == "READ"
        assert response["Grants"][0]["Permission"] == "FULL_CONTROL"

    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @pytest.mark.asyncio
    async def test_s3_key_hook_get_file_metadata_async(self, mock_client):
        """
        Test check_wildcard_key for a valid response
        :return:
        """
        test_resp_iter = [
            {
                "Contents": [
                    {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                    {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                ]
            }
        ]
        mock_paginator = mock.Mock()
        mock_paginate = mock.MagicMock()
        mock_paginate.__aiter__.return_value = test_resp_iter
        mock_paginator.paginate.return_value = mock_paginate

        s3_hook_async = S3Hook(client_type="S3")
        mock_client.get_paginator = mock.Mock(return_value=mock_paginator)
        keys = [x async for x in s3_hook_async.get_file_metadata_async(mock_client, "test_bucket", "test*")]

        assert keys == [
            {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
            {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
        ]

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    async def test_s3_key_hook_get_head_object_with_error_async(self, mock_client):
        """
        Test for 404 error if key not found and assert based on response.
        :return:
        """
        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")

        mock_client.head_object.side_effect = ClientError(
            {
                "Error": {
                    "Code": "SomeServiceException",
                    "Message": "Details/context around the exception or error",
                },
                "ResponseMetadata": {
                    "RequestId": "1234567890ABCDEF",
                    "HostId": "host ID data will appear here as a hash",
                    "HTTPStatusCode": 404,
                    "HTTPHeaders": {"header metadata key/values will appear here"},
                    "RetryAttempts": 0,
                },
            },
            operation_name="s3",
        )
        response = await s3_hook_async.get_head_object_async(
            mock_client, "s3://test_bucket/file", "test_bucket"
        )
        assert response is None

    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @pytest.mark.asyncio
    async def test_s3_key_hook_get_head_object_raise_exception_async(self, mock_client):
        """
        Test for 500 error if key not found and assert based on response.
        :return:
        """
        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")

        mock_client.head_object.side_effect = ClientError(
            {
                "Error": {
                    "Code": "SomeServiceException",
                    "Message": "Details/context around the exception or error",
                },
                "ResponseMetadata": {
                    "RequestId": "1234567890ABCDEF",
                    "HostId": "host ID data will appear here as a hash",
                    "HTTPStatusCode": 500,
                    "HTTPHeaders": {"header metadata key/values will appear here"},
                    "RetryAttempts": 0,
                },
            },
            operation_name="s3",
        )
        with pytest.raises(ClientError) as err:
            await s3_hook_async.get_head_object_async(mock_client, "s3://test_bucket/file", "test_bucket")
        assert isinstance(err.value, ClientError)

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    async def test_s3_key_hook_get_files_without_wildcard_async(self, mock_client):
        """
        Test get_files for a valid response
        :return:
        """
        test_resp_iter = [
            {
                "Contents": [
                    {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                    {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                ]
            }
        ]
        mock_paginator = mock.Mock()
        mock_paginate = mock.MagicMock()
        mock_paginate.__aiter__.return_value = test_resp_iter
        mock_paginator.paginate.return_value = mock_paginate

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        mock_client.get_paginator = mock.Mock(return_value=mock_paginator)
        response = await s3_hook_async.get_files_async(mock_client, "test_bucket", "test.txt", False)
        assert response == []

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    async def test_s3_key_hook_get_files_with_wildcard_async(self, mock_client):
        """
        Test get_files for a valid response
        :return:
        """
        test_resp_iter = [
            {
                "Contents": [
                    {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                    {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                ]
            }
        ]
        mock_paginator = mock.Mock()
        mock_paginate = mock.MagicMock()
        mock_paginate.__aiter__.return_value = test_resp_iter
        mock_paginator.paginate.return_value = mock_paginate

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        mock_client.get_paginator = mock.Mock(return_value=mock_paginator)
        response = await s3_hook_async.get_files_async(mock_client, "test_bucket", "test.txt", True)
        assert response == []

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    async def test_s3_key_hook_list_keys_async(self, mock_client):
        """
        Test _list_keys for a valid response
        :return:
        """
        test_resp_iter = [
            {
                "Contents": [
                    {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                    {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                ]
            }
        ]
        mock_paginator = mock.Mock()
        mock_paginate = mock.MagicMock()
        mock_paginate.__aiter__.return_value = test_resp_iter
        mock_paginator.paginate.return_value = mock_paginate

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        mock_client.get_paginator = mock.Mock(return_value=mock_paginator)
        response = await s3_hook_async._list_keys_async(mock_client, "test_bucket", "test*")
        assert response == ["test_key", "test_key2"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_first_prefix, test_second_prefix",
        [
            ("async-prefix1/", "async-prefix2/"),
        ],
    )
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.async_conn")
    async def test_s3_prefix_sensor_hook_list_prefixes_async(
        self, mock_client, test_first_prefix, test_second_prefix
    ):
        """
        Test list_prefixes whether it returns a valid response
        """
        test_resp_iter = [{"CommonPrefixes": [{"Prefix": test_first_prefix}, {"Prefix": test_second_prefix}]}]
        mock_paginator = mock.Mock()
        mock_paginate = mock.MagicMock()
        mock_paginate.__aiter__.return_value = test_resp_iter
        mock_paginator.paginate.return_value = mock_paginate

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        mock_client.get_paginator = mock.Mock(return_value=mock_paginator)

        actual_output = await s3_hook_async.list_prefixes_async(mock_client, "test_bucket", "test")
        expected_output = [test_first_prefix, test_second_prefix]
        assert expected_output == actual_output

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_prefix, mock_bucket",
        [
            ("async-prefix1", "test_bucket"),
        ],
    )
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.list_prefixes_async")
    async def test_s3_prefix_sensor_hook_check_for_prefix_async(
        self, mock_list_prefixes, mock_client, mock_prefix, mock_bucket
    ):
        """
        Test that _check_for_prefix method returns True when valid prefix is used and returns False
        when invalid prefix is used
        """
        mock_list_prefixes.return_value = ["async-prefix1/", "async-prefix2/"]

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")

        response = await s3_hook_async._check_for_prefix_async(
            client=mock_client.return_value, prefix=mock_prefix, bucket_name=mock_bucket, delimiter="/"
        )

        assert response is True

        response = await s3_hook_async._check_for_prefix_async(
            client=mock_client.return_value,
            prefix="non-existing-prefix",
            bucket_name=mock_bucket,
            delimiter="/",
        )

        assert response is False

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_s3_bucket_key")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.async_conn")
    async def test__check_key_async_without_wildcard_match(self, mock_get_conn, mock_get_bucket_key):
        """Test _check_key_async function without using wildcard_match"""
        mock_get_bucket_key.return_value = "test_bucket", "test.txt"
        mock_client = mock_get_conn.return_value
        mock_client.head_object = AsyncMock(return_value={"ContentLength": 0})
        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        response = await s3_hook_async._check_key_async(
            mock_client, "test_bucket", False, "s3://test_bucket/file/test.txt"
        )
        assert response is True

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_s3_bucket_key")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.async_conn")
    async def test_s3__check_key_async_without_wildcard_match_and_get_none(
        self, mock_get_conn, mock_get_bucket_key
    ):
        """Test _check_key_async function when get head object returns none"""
        mock_get_bucket_key.return_value = "test_bucket", "test.txt"
        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        mock_client = mock_get_conn.return_value
        mock_client.head_object = AsyncMock(return_value=None)
        response = await s3_hook_async._check_key_async(
            mock_client, "test_bucket", False, "s3://test_bucket/file/test.txt"
        )
        assert response is False

    # @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_s3_bucket_key")
    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.async_conn")
    @pytest.mark.parametrize(
        "contents, result",
        [
            (
                [
                    {
                        "Key": "test/example_s3_test_file.txt",
                        "ETag": "etag1",
                        "LastModified": datetime(2020, 8, 14, 17, 19, 34),
                        "Size": 0,
                    },
                    {
                        "Key": "test_key2",
                        "ETag": "etag2",
                        "LastModified": datetime(2020, 8, 14, 17, 19, 34),
                        "Size": 0,
                    },
                ],
                True,
            ),
            (
                [
                    {
                        "Key": "test/example_aeoua.txt",
                        "ETag": "etag1",
                        "LastModified": datetime(2020, 8, 14, 17, 19, 34),
                        "Size": 0,
                    },
                    {
                        "Key": "test_key2",
                        "ETag": "etag2",
                        "LastModified": datetime(2020, 8, 14, 17, 19, 34),
                        "Size": 0,
                    },
                ],
                False,
            ),
        ],
    )
    async def test_s3__check_key_async_with_wildcard_match(self, mock_get_conn, contents, result):
        """Test _check_key_async function"""
        client = mock_get_conn.return_value
        paginator = client.get_paginator.return_value
        r = paginator.paginate.return_value
        r.__aiter__.return_value = [{"Contents": contents}]
        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        response = await s3_hook_async._check_key_async(
            client=client,
            bucket_val="test_bucket",
            wildcard_match=True,
            key="test/example_s3_test_file.txt",
        )
        assert response is result

    @pytest.mark.parametrize(
        "key, pattern, expected",
        [
            ("test.csv", r"[a-z]+\.csv", True),
            ("test.txt", r"test/[a-z]+\.csv", False),
            ("test/test.csv", r"test/[a-z]+\.csv", True),
        ],
    )
    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_s3_bucket_key")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.async_conn")
    async def test__check_key_async_with_use_regex(
        self, mock_get_conn, mock_get_bucket_key, key, pattern, expected
    ):
        """Match AWS S3 key with regex expression"""
        mock_get_bucket_key.return_value = "test_bucket", pattern
        client = mock_get_conn.return_value
        paginator = client.get_paginator.return_value
        r = paginator.paginate.return_value
        r.__aiter__.return_value = [
            {
                "Contents": [
                    {
                        "Key": key,
                        "ETag": "etag1",
                        "LastModified": datetime(2020, 8, 14, 17, 19, 34),
                        "Size": 0,
                    },
                ]
            }
        ]

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        response = await s3_hook_async._check_key_async(
            client=client,
            bucket_val="test_bucket",
            wildcard_match=False,
            key=pattern,
            use_regex=True,
        )
        assert response is expected

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook._list_keys_async")
    async def test_s3_key_hook_is_keys_unchanged_false_async(self, mock_list_keys, mock_client):
        """
        Test is_key_unchanged gives False response when the key value is unchanged in specified period.
        """

        mock_list_keys.return_value = ["test"]

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        response = await s3_hook_async.is_keys_unchanged_async(
            client=mock_client.return_value,
            bucket_name="test_bucket",
            prefix="test",
            inactivity_period=1,
            min_objects=1,
            previous_objects=set(),
            inactivity_seconds=0,
            allow_delete=True,
            last_activity_time=None,
        )

        assert response.get("status") == "pending"

        # test for the case when current_objects < previous_objects
        mock_list_keys.return_value = []

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")
        response = await s3_hook_async.is_keys_unchanged_async(
            client=mock_client.return_value,
            bucket_name="test_bucket",
            prefix="test",
            inactivity_period=1,
            min_objects=1,
            previous_objects=set("test"),
            inactivity_seconds=0,
            allow_delete=True,
            last_activity_time=None,
        )

        assert response.get("status") == "pending"

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook._list_keys_async")
    async def test_s3_key_hook_is_keys_unchanged_exception_async(self, mock_list_keys, mock_client):
        """
        Test is_key_unchanged gives AirflowException.
        """
        mock_list_keys.return_value = []

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")

        response = await s3_hook_async.is_keys_unchanged_async(
            client=mock_client.return_value,
            bucket_name="test_bucket",
            prefix="test",
            inactivity_period=1,
            min_objects=1,
            previous_objects=set("test"),
            inactivity_seconds=0,
            allow_delete=False,
            last_activity_time=None,
        )

        assert response == {"message": "test_bucket/test between pokes.", "status": "error"}

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook._list_keys_async")
    async def test_s3_key_hook_is_keys_unchanged_async_handle_tzinfo(self, mock_list_keys, mock_client):
        """
        Test is_key_unchanged gives AirflowException.
        """
        mock_list_keys.return_value = []

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")

        response = await s3_hook_async.is_keys_unchanged_async(
            client=mock_client.return_value,
            bucket_name="test_bucket",
            prefix="test",
            inactivity_period=1,
            min_objects=0,
            previous_objects=set(),
            inactivity_seconds=0,
            allow_delete=False,
            last_activity_time=None,
        )

        assert response.get("status") == "pending"

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook._list_keys_async")
    async def test_s3_key_hook_is_keys_unchanged_inactivity_error_async(self, mock_list_keys, mock_client):
        """
        Test is_key_unchanged gives AirflowException.
        """
        mock_list_keys.return_value = []

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")

        response = await s3_hook_async.is_keys_unchanged_async(
            client=mock_client.return_value,
            bucket_name="test_bucket",
            prefix="test",
            inactivity_period=0,
            min_objects=5,
            previous_objects=set(),
            inactivity_seconds=5,
            allow_delete=False,
            last_activity_time=None,
        )

        assert response == {
            "status": "error",
            "message": "FAILURE: Inactivity Period passed, not enough objects found in test_bucket/test",
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook._list_keys_async")
    async def test_s3_key_hook_is_keys_unchanged_pending_async_without_tzinfo(
        self, mock_list_keys, mock_client
    ):
        """
        Test is_key_unchanged gives AirflowException.
        """
        mock_list_keys.return_value = []

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")

        response = await s3_hook_async.is_keys_unchanged_async(
            client=mock_client.return_value,
            bucket_name="test_bucket",
            prefix="test",
            inactivity_period=1,
            min_objects=0,
            previous_objects=set(),
            inactivity_seconds=0,
            allow_delete=False,
            last_activity_time=std_datetime.now(),
        )
        assert response.get("status") == "pending"

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook._list_keys_async")
    async def test_s3_key_hook_is_keys_unchanged_pending_async_with_tzinfo(self, mock_list_keys, mock_client):
        """
        Test is_key_unchanged gives AirflowException.
        """
        mock_list_keys.return_value = []

        s3_hook_async = S3Hook(client_type="S3", resource_type="S3")

        response = await s3_hook_async.is_keys_unchanged_async(
            client=mock_client.return_value,
            bucket_name="test_bucket",
            prefix="test",
            inactivity_period=1,
            min_objects=0,
            previous_objects=set(),
            inactivity_seconds=0,
            allow_delete=False,
            last_activity_time=std_datetime.now(timezone.utc),
        )
        assert response.get("status") == "pending"

    def test_load_bytes(self, s3_bucket):
        hook = S3Hook()
        hook.load_bytes(b"Content", "my_key", s3_bucket)
        resource = boto3.resource("s3").Object(s3_bucket, "my_key")
        assert resource.get()["Body"].read() == b"Content"

    def test_load_bytes_acl(self, s3_bucket):
        hook = S3Hook()
        hook.load_bytes(b"Content", "my_key", s3_bucket, acl_policy="public-read")
        response = boto3.client("s3").get_object_acl(Bucket=s3_bucket, Key="my_key", RequestPayer="requester")
        assert response["Grants"][1]["Permission"] == "READ"
        assert response["Grants"][0]["Permission"] == "FULL_CONTROL"

    def test_load_fileobj(self, s3_bucket, tmp_path):
        hook = S3Hook()
        path = tmp_path / "testfile"
        path.write_text("Content")
        hook.load_file_obj(path.open("rb"), "my_key", s3_bucket)
        resource = boto3.resource("s3").Object(s3_bucket, "my_key")
        assert resource.get()["Body"].read() == b"Content"

    def test_load_fileobj_acl(self, s3_bucket, tmp_path):
        hook = S3Hook()
        path = tmp_path / "testfile"
        path.write_text("Content")
        hook.load_file_obj(path.open("rb"), "my_key", s3_bucket, acl_policy="public-read")
        response = boto3.client("s3").get_object_acl(Bucket=s3_bucket, Key="my_key", RequestPayer="requester")
        assert response["Grants"][1]["Permission"] == "READ"
        assert response["Grants"][0]["Permission"] == "FULL_CONTROL"

    def test_load_file_gzip(self, s3_bucket, tmp_path):
        hook = S3Hook()
        path = tmp_path / "testfile"
        path.write_text("Content")
        hook.load_file(path, "my_key", s3_bucket, gzip=True)
        resource = boto3.resource("s3").Object(s3_bucket, "my_key")
        assert gz.decompress(resource.get()["Body"].read()) == b"Content"

    @pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Hook lineage works in Airflow >= 2.10.0")
    def test_load_file_exposes_lineage(self, s3_bucket, tmp_path, hook_lineage_collector):
        hook = S3Hook()
        path = tmp_path / "testfile"
        path.write_text("Content")
        hook.load_file(path, "my_key", s3_bucket)
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"s3://{s3_bucket}/my_key"
        )

    def test_load_file_acl(self, s3_bucket, tmp_path):
        hook = S3Hook()
        path = tmp_path / "testfile"
        path.write_text("Content")
        hook.load_file(path, "my_key", s3_bucket, gzip=True, acl_policy="public-read")
        response = boto3.client("s3").get_object_acl(Bucket=s3_bucket, Key="my_key", RequestPayer="requester")
        assert response["Grants"][1]["Permission"] == "READ"
        assert response["Grants"][0]["Permission"] == "FULL_CONTROL"

    def test_copy_object_acl(self, s3_bucket, tmp_path):
        hook = S3Hook()
        path = tmp_path / "testfile"
        path.write_text("Content")
        hook.load_file_obj(path.open("rb"), "my_key", s3_bucket)
        hook.copy_object("my_key", "my_key2", s3_bucket, s3_bucket)
        response = boto3.client("s3").get_object_acl(
            Bucket=s3_bucket, Key="my_key2", RequestPayer="requester"
        )
        assert response["Grants"][0]["Permission"] == "FULL_CONTROL"
        assert len(response["Grants"]) == 1

    @mock_aws
    def test_copy_object_no_acl(
        self,
        s3_bucket,
    ):
        mock_hook = S3Hook()

        with mock.patch.object(
            S3Hook,
            "get_conn",
        ) as patched_get_conn:
            mock_hook.copy_object("my_key", "my_key3", s3_bucket, s3_bucket, acl_policy=NO_ACL)

            # Check we're not passing ACLs
            patched_get_conn.return_value.copy_object.assert_called_once_with(
                Bucket="airflow-test-s3-bucket",
                Key="my_key3",
                CopySource={"Bucket": "airflow-test-s3-bucket", "Key": "my_key", "VersionId": None},
            )
            patched_get_conn.reset_mock()

            mock_hook.copy_object(
                "my_key",
                "my_key3",
                s3_bucket,
                s3_bucket,
            )

            # Check the default is "private"
            patched_get_conn.return_value.copy_object.assert_called_once_with(
                Bucket="airflow-test-s3-bucket",
                Key="my_key3",
                CopySource={"Bucket": "airflow-test-s3-bucket", "Key": "my_key", "VersionId": None},
                ACL="private",
            )

    @pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Hook lineage works in Airflow >= 2.10.0")
    @mock_aws
    def test_copy_object_ol_instrumentation(self, s3_bucket, hook_lineage_collector):
        mock_hook = S3Hook()

        with mock.patch.object(
            S3Hook,
            "get_conn",
        ):
            mock_hook.copy_object("my_key", "my_key3", s3_bucket, s3_bucket)
            assert len(hook_lineage_collector.collected_assets.inputs) == 1
            assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
                uri=f"s3://{s3_bucket}/my_key"
            )

            assert len(hook_lineage_collector.collected_assets.outputs) == 1
            assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
                uri=f"s3://{s3_bucket}/my_key3"
            )

    @mock_aws
    def test_delete_bucket_if_bucket_exist(self, s3_bucket):
        # assert if the bucket is created
        mock_hook = S3Hook()
        mock_hook.create_bucket(bucket_name=s3_bucket)
        assert mock_hook.check_for_bucket(bucket_name=s3_bucket)
        mock_hook.delete_bucket(bucket_name=s3_bucket, force_delete=True)
        assert not mock_hook.check_for_bucket(s3_bucket)

    @mock_aws
    def test_delete_bucket_if_bucket_not_exist(self, s3_bucket):
        # assert if exception is raised if bucket not present
        mock_hook = S3Hook(aws_conn_id=None)
        with pytest.raises(ClientError) as ctx:
            assert mock_hook.delete_bucket(bucket_name="not-exists-bucket-name", force_delete=True)
        assert ctx.value.response["Error"]["Code"] == "NoSuchBucket"

    @pytest.mark.db_test
    def test_provide_bucket_name(self):
        with mock.patch.object(
            S3Hook,
            "get_connection",
            return_value=Connection(extra={"service_config": {"s3": {"bucket_name": "bucket_name"}}}),
        ):

            class FakeS3Hook(S3Hook):
                @provide_bucket_name
                def test_function(self, bucket_name=None):
                    return bucket_name

            fake_s3_hook = FakeS3Hook()

            test_bucket_name = fake_s3_hook.test_function()
            assert test_bucket_name == "bucket_name"

            test_bucket_name = fake_s3_hook.test_function(bucket_name="bucket")
            assert test_bucket_name == "bucket"

    def test_delete_objects_key_does_not_exist(self, s3_bucket):
        # The behaviour of delete changed in recent version of s3 mock libraries.
        # It will succeed idempotently
        hook = S3Hook()
        hook.delete_objects(bucket=s3_bucket, keys=["key-1"])

    def test_delete_objects_one_key(self, mocked_s3_res, s3_bucket):
        key = "key-1"
        mocked_s3_res.Object(s3_bucket, key).put(Body=b"Data")
        hook = S3Hook()
        hook.delete_objects(bucket=s3_bucket, keys=[key])
        assert [o.key for o in mocked_s3_res.Bucket(s3_bucket).objects.all()] == []

    def test_delete_objects_many_keys(self, mocked_s3_res, s3_bucket):
        num_keys_to_remove = 1001
        keys = []
        for index in range(num_keys_to_remove):
            key = f"key-{index}"
            mocked_s3_res.Object(s3_bucket, key).put(Body=b"Data")
            keys.append(key)

        assert sum(1 for _ in mocked_s3_res.Bucket(s3_bucket).objects.all()) == num_keys_to_remove
        hook = S3Hook()
        hook.delete_objects(bucket=s3_bucket, keys=keys)
        assert [o.key for o in mocked_s3_res.Bucket(s3_bucket).objects.all()] == []

    def test_unify_bucket_name_and_key(self):
        class FakeS3Hook(S3Hook):
            @unify_bucket_name_and_key
            def test_function_with_wildcard_key(self, wildcard_key, bucket_name=None):
                return bucket_name, wildcard_key

            @unify_bucket_name_and_key
            def test_function_with_key(self, key, bucket_name=None):
                return bucket_name, key

            @unify_bucket_name_and_key
            def test_function_with_test_key(self, test_key, bucket_name=None):
                return bucket_name, test_key

        fake_s3_hook = FakeS3Hook()

        test_bucket_name_with_wildcard_key = fake_s3_hook.test_function_with_wildcard_key("s3://foo/bar*.csv")
        assert ("foo", "bar*.csv") == test_bucket_name_with_wildcard_key

        test_bucket_name_with_key = fake_s3_hook.test_function_with_key("s3://foo/bar.csv")
        assert ("foo", "bar.csv") == test_bucket_name_with_key

        with pytest.raises(ValueError) as ctx:
            fake_s3_hook.test_function_with_test_key("s3://foo/bar.csv")
        assert isinstance(ctx.value, ValueError)

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.NamedTemporaryFile")
    def test_download_file(self, mock_temp_file, tmp_path):
        path = tmp_path / "airflow_tmp_test_s3_hook"
        mock_temp_file.return_value = path
        s3_hook = S3Hook(aws_conn_id="s3_test")
        s3_hook.check_for_key = Mock(return_value=True)
        s3_obj = Mock()
        s3_obj.download_fileobj = Mock(return_value=None)
        s3_hook.get_key = Mock(return_value=s3_obj)
        key = "test_key"
        bucket = "test_bucket"

        output_file = s3_hook.download_file(key=key, bucket_name=bucket)

        s3_hook.get_key.assert_called_once_with(key, bucket)
        s3_obj.download_fileobj.assert_called_once_with(
            path,
            Config=s3_hook.transfer_config,
            ExtraArgs=s3_hook.extra_args,
        )

        assert path.name == output_file

    @pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Hook lineage works in Airflow >= 2.10.0")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.NamedTemporaryFile")
    def test_download_file_exposes_lineage(self, mock_temp_file, tmp_path, hook_lineage_collector):
        path = tmp_path / "airflow_tmp_test_s3_hook"
        mock_temp_file.return_value = path
        s3_hook = S3Hook(aws_conn_id="s3_test")
        s3_hook.check_for_key = Mock(return_value=True)
        s3_obj = Mock()
        s3_obj.download_fileobj = Mock(return_value=None)
        s3_hook.get_key = Mock(return_value=s3_obj)
        key = "test_key"
        bucket = "test_bucket"

        s3_hook.download_file(key=key, bucket_name=bucket)

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri="s3://test_bucket/test_key"
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.open")
    def test_download_file_with_preserve_name(self, mock_open, tmp_path):
        path = tmp_path / "test.log"
        bucket = "test_bucket"
        key = f"test_key/{path.name}"

        s3_hook = S3Hook(aws_conn_id="s3_test")
        s3_hook.check_for_key = Mock(return_value=True)
        s3_obj = Mock()
        s3_obj.key = f"s3://{bucket}/{key}"
        s3_obj.download_fileobj = Mock(return_value=None)
        s3_hook.get_key = Mock(return_value=s3_obj)
        local_path = os.fspath(path.parent)
        s3_hook.download_file(
            key=key,
            bucket_name=bucket,
            local_path=local_path,
            preserve_file_name=True,
            use_autogenerated_subdir=False,
        )

        mock_open.assert_called_once_with(path, "wb")

    @pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Hook lineage works in Airflow >= 2.10.0")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.open")
    def test_download_file_with_preserve_name_exposes_lineage(
        self, mock_open, tmp_path, hook_lineage_collector
    ):
        path = tmp_path / "test.log"
        bucket = "test_bucket"
        key = f"test_key/{path.name}"

        s3_hook = S3Hook(aws_conn_id="s3_test")
        s3_hook.check_for_key = Mock(return_value=True)
        s3_obj = Mock()
        s3_obj.key = f"s3://{bucket}/{key}"
        s3_obj.download_fileobj = Mock(return_value=None)
        s3_hook.get_key = Mock(return_value=s3_obj)
        local_path = os.fspath(path.parent)
        s3_hook.download_file(
            key=key,
            bucket_name=bucket,
            local_path=local_path,
            preserve_file_name=True,
            use_autogenerated_subdir=False,
        )

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri="s3://test_bucket/test_key/test.log"
        )

        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"file://{local_path}/test.log",
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.open")
    def test_download_file_with_preserve_name_with_autogenerated_subdir(self, mock_open, tmp_path):
        path = tmp_path / "test.log"
        bucket = "test_bucket"
        key = f"test_key/{path.name}"

        s3_hook = S3Hook(aws_conn_id="s3_test")
        s3_hook.check_for_key = Mock(return_value=True)
        s3_obj = Mock()
        s3_obj.key = f"s3://{bucket}/{key}"
        s3_obj.download_fileobj = Mock(return_value=None)
        s3_hook.get_key = Mock(return_value=s3_obj)
        result_file = s3_hook.download_file(
            key=key,
            bucket_name=bucket,
            local_path=os.fspath(path.parent),
            preserve_file_name=True,
            use_autogenerated_subdir=True,
        )

        assert result_file.rsplit("/", 1)[-2].startswith("airflow_tmp_dir_")

    def test_download_file_with_preserve_name_file_already_exists(self, tmp_path):
        path = tmp_path / "airflow_tmp_test_s3_hook"
        path.write_text("")
        bucket = "test_bucket"
        key = f"test_key/{path.name}"
        s3_hook = S3Hook(aws_conn_id="s3_test")
        s3_hook.check_for_key = Mock(return_value=True)
        s3_obj = Mock()
        s3_obj.key = f"s3://{bucket}/{key}"
        s3_obj.download_fileobj = Mock(return_value=None)
        s3_hook.get_key = Mock(return_value=s3_obj)
        with pytest.raises(FileExistsError):
            s3_hook.download_file(
                key=key,
                bucket_name=bucket,
                local_path=os.fspath(path.parent),
                preserve_file_name=True,
                use_autogenerated_subdir=False,
            )

    @mock.patch.object(S3Hook, "get_session")
    def test_download_file_with_extra_args_sanitizes_values(self, mock_session):
        bucket = "test_bucket"
        s3_key = "test_key"
        encryption_key = "abcd123"
        encryption_algorithm = "AES256"  # This is the only algorithm currently supported.

        s3_hook = S3Hook(
            extra_args={
                "SSECustomerKey": encryption_key,
                "SSECustomerAlgorithm": encryption_algorithm,
                "invalid_arg": "should be dropped",
            }
        )

        mock_obj = Mock(name="MockedS3Object")
        mock_resource = Mock(name="MockedBoto3Resource")
        mock_resource.return_value.Object = mock_obj
        mock_session.return_value.resource = mock_resource

        s3_hook.download_file(key=s3_key, bucket_name=bucket)

        mock_obj.assert_called_once_with(bucket, s3_key)
        mock_obj.return_value.load.assert_called_once_with(
            SSECustomerKey=encryption_key,
            SSECustomerAlgorithm=encryption_algorithm,
        )

    def test_generate_presigned_url(self, s3_bucket):
        hook = S3Hook()
        presigned_url = hook.generate_presigned_url(
            client_method="get_object", params={"Bucket": s3_bucket, "Key": "my_key"}
        )
        params = parse_qs(presigned_url.partition("?")[-1])
        assert {"AWSAccessKeyId", "Signature", "Expires"}.issubset(set(params.keys()))

    def test_should_throw_error_if_extra_args_is_not_dict(self):
        with pytest.raises(TypeError, match="extra_args expected dict, got .*"):
            S3Hook(extra_args=1)

    def test_should_throw_error_if_extra_args_contains_unknown_arg(self, s3_bucket, tmp_path):
        hook = S3Hook(extra_args={"unknown_s3_args": "value"})
        path = tmp_path / "testfile"
        path.write_text("Content")
        with pytest.raises(ValueError):
            hook.load_file_obj(path.open("rb"), "my_key", s3_bucket, acl_policy="public-read")

    def test_should_pass_extra_args(self, s3_bucket, tmp_path):
        hook = S3Hook(extra_args={"ContentLanguage": "value"})
        path = tmp_path / "testfile"
        path.write_text("Content")
        hook.load_file_obj(path.open("rb"), "my_key", s3_bucket, acl_policy="public-read")
        resource = boto3.resource("s3").Object(s3_bucket, "my_key")
        assert resource.get()["ContentLanguage"] == "value"

    def test_that_extra_args_not_changed_between_calls(self, s3_bucket):
        original = {
            "Metadata": {"metakey": "metaval"},
            "ACL": "private",
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "arn:aws:kms:region:acct-id:key/key-id",
        }
        s3_hook = S3Hook(aws_conn_id="s3_test", extra_args=original)
        # We're mocking all actual AWS calls and don't need a connection. This
        # avoids an Airflow warning about connection cannot be found.
        s3_hook.get_connection = lambda _: None
        assert s3_hook.extra_args == original
        assert s3_hook.extra_args is not original

        dummy = mock.MagicMock()
        s3_hook.check_for_key = Mock(return_value=False)
        mock_upload_fileobj = s3_hook.conn.upload_fileobj = Mock(return_value=None)
        mock_upload_file = s3_hook.conn.upload_file = Mock(return_value=None)

        # First Call - load_file_obj.
        s3_hook.load_file_obj(dummy, "mock_key", s3_bucket, encrypt=True, acl_policy="public-read")
        first_call_extra_args = mock_upload_fileobj.call_args_list[0][1]["ExtraArgs"]
        assert s3_hook.extra_args == original
        assert first_call_extra_args is not s3_hook.extra_args

        # Second Call - load_bytes.
        s3_hook.load_string("dummy", "mock_key", s3_bucket, acl_policy="bucket-owner-full-control")
        second_call_extra_args = mock_upload_fileobj.call_args_list[1][1]["ExtraArgs"]
        assert s3_hook.extra_args == original
        assert second_call_extra_args is not s3_hook.extra_args
        assert second_call_extra_args != first_call_extra_args

        # Third Call - load_string.
        s3_hook.load_bytes(b"dummy", "mock_key", s3_bucket, encrypt=True)
        third_call_extra_args = mock_upload_fileobj.call_args_list[2][1]["ExtraArgs"]
        assert s3_hook.extra_args == original
        assert third_call_extra_args is not s3_hook.extra_args
        assert third_call_extra_args not in [first_call_extra_args, second_call_extra_args]

        # Fourth Call - load_file.
        s3_hook.load_file("/dummy.png", "mock_key", s3_bucket, encrypt=True, acl_policy="bucket-owner-read")
        fourth_call_extra_args = mock_upload_file.call_args_list[0][1]["ExtraArgs"]
        assert s3_hook.extra_args == original
        assert fourth_call_extra_args is not s3_hook.extra_args
        assert fourth_call_extra_args not in [
            third_call_extra_args,
            first_call_extra_args,
            second_call_extra_args,
        ]

    @mock_aws
    def test_get_bucket_tagging_no_tags_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")

        with pytest.raises(ClientError, match=r".*NoSuchTagSet.*"):
            hook.get_bucket_tagging(bucket_name="new_bucket")

    @mock_aws
    def test_get_bucket_tagging_no_bucket_raises_error(self):
        hook = S3Hook()

        with pytest.raises(ClientError, match=r".*NoSuchBucket.*"):
            hook.get_bucket_tagging(bucket_name="new_bucket")

    @mock_aws
    def test_put_bucket_tagging_with_valid_set(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        hook.put_bucket_tagging(bucket_name="new_bucket", tag_set=tag_set)

        assert hook.get_bucket_tagging(bucket_name="new_bucket") == tag_set

    @mock_aws
    def test_put_bucket_tagging_with_dict(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = {"Color": "Green"}
        hook.put_bucket_tagging(bucket_name="new_bucket", tag_set=tag_set)

        assert hook.get_bucket_tagging(bucket_name="new_bucket") == [{"Key": "Color", "Value": "Green"}]

    @mock_aws
    def test_put_bucket_tagging_with_pair(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        key = "Color"
        value = "Green"
        hook.put_bucket_tagging(bucket_name="new_bucket", key=key, value=value)

        assert hook.get_bucket_tagging(bucket_name="new_bucket") == tag_set

    @mock_aws
    def test_put_bucket_tagging_with_pair_and_set(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        expected = [{"Key": "Color", "Value": "Green"}, {"Key": "Fruit", "Value": "Apple"}]
        tag_set = [{"Key": "Color", "Value": "Green"}]
        key = "Fruit"
        value = "Apple"
        hook.put_bucket_tagging(bucket_name="new_bucket", tag_set=tag_set, key=key, value=value)

        result = hook.get_bucket_tagging(bucket_name="new_bucket")
        assert len(result) == 2
        assert result == expected

    @mock_aws
    def test_put_bucket_tagging_with_key_but_no_value_raises_error(self):
        hook = S3Hook()

        hook.create_bucket(bucket_name="new_bucket")
        key = "Color"
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name="new_bucket", key=key)

    @mock_aws
    def test_put_bucket_tagging_with_value_but_no_key_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        value = "Color"
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name="new_bucket", value=value)

    @mock_aws
    def test_put_bucket_tagging_with_key_and_set_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        key = "Color"
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name="new_bucket", key=key, tag_set=tag_set)

    @mock_aws
    def test_put_bucket_tagging_with_value_and_set_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        value = "Green"
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name="new_bucket", value=value, tag_set=tag_set)

    @mock_aws
    def test_put_bucket_tagging_when_tags_exist_overwrites(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        initial_tag_set = [{"Key": "Color", "Value": "Green"}]
        hook.put_bucket_tagging(bucket_name="new_bucket", tag_set=initial_tag_set)
        assert len(hook.get_bucket_tagging(bucket_name="new_bucket")) == 1
        assert hook.get_bucket_tagging(bucket_name="new_bucket") == initial_tag_set

        new_tag_set = [{"Key": "Fruit", "Value": "Apple"}]
        hook.put_bucket_tagging(bucket_name="new_bucket", tag_set=new_tag_set)

        result = hook.get_bucket_tagging(bucket_name="new_bucket")
        assert len(result) == 1
        assert result == new_tag_set

    @mock_aws
    def test_delete_bucket_tagging(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        hook.put_bucket_tagging(bucket_name="new_bucket", tag_set=tag_set)
        hook.get_bucket_tagging(bucket_name="new_bucket")

        hook.delete_bucket_tagging(bucket_name="new_bucket")
        with pytest.raises(ClientError, match=r".*NoSuchTagSet.*"):
            hook.get_bucket_tagging(bucket_name="new_bucket")

    @mock_aws
    def test_delete_bucket_tagging_with_no_tags(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")

        hook.delete_bucket_tagging(bucket_name="new_bucket")

        with pytest.raises(ClientError, match=r".*NoSuchTagSet.*"):
            hook.get_bucket_tagging(bucket_name="new_bucket")


@pytest.mark.db_test
@pytest.mark.parametrize(
    "key_kind, has_conn, has_bucket, precedence, expected",
    [
        ("full_key", "no_conn", "no_bucket", "unify", ["key_bucket", "key.txt"]),
        ("full_key", "no_conn", "no_bucket", "provide", ["key_bucket", "key.txt"]),
        ("full_key", "no_conn", "with_bucket", "unify", ["kwargs_bucket", "s3://key_bucket/key.txt"]),
        ("full_key", "no_conn", "with_bucket", "provide", ["kwargs_bucket", "s3://key_bucket/key.txt"]),
        ("full_key", "with_conn", "no_bucket", "unify", ["key_bucket", "key.txt"]),
        ("full_key", "with_conn", "no_bucket", "provide", ["conn_bucket", "s3://key_bucket/key.txt"]),
        ("full_key", "with_conn", "with_bucket", "unify", ["kwargs_bucket", "s3://key_bucket/key.txt"]),
        ("full_key", "with_conn", "with_bucket", "provide", ["kwargs_bucket", "s3://key_bucket/key.txt"]),
        ("rel_key", "no_conn", "no_bucket", "unify", [None, "key.txt"]),
        ("rel_key", "no_conn", "no_bucket", "provide", [None, "key.txt"]),
        ("rel_key", "no_conn", "with_bucket", "unify", ["kwargs_bucket", "key.txt"]),
        ("rel_key", "no_conn", "with_bucket", "provide", ["kwargs_bucket", "key.txt"]),
        ("rel_key", "with_conn", "no_bucket", "unify", ["conn_bucket", "key.txt"]),
        ("rel_key", "with_conn", "no_bucket", "provide", ["conn_bucket", "key.txt"]),
        ("rel_key", "with_conn", "with_bucket", "unify", ["kwargs_bucket", "key.txt"]),
        ("rel_key", "with_conn", "with_bucket", "provide", ["kwargs_bucket", "key.txt"]),
    ],
)
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_unify_and_provide_bucket_name_combination(
    mock_base, key_kind, has_conn, has_bucket, precedence, expected, caplog
):
    """
    Verify what is the outcome when the unify_bucket_name_and_key and provide_bucket_name
    decorators are combined.

    The one case (at least in this test) where the order makes a difference is when
    user provides a full s3 key, and also has a connection with a bucket defined,
    and does not provide a bucket in the method call.  In this case, if we unify
    first, then we (desirably) get the bucket from the key.  If we provide bucket first,
    something undesirable happens.  The bucket from the connection is used, which means
    we don't respect the full key provided. Further, the full key is not made relative,
    which would cause the actual request to fail. For this reason we want to put unify
    first.
    """
    if has_conn == "with_conn":
        c = Connection(extra={"service_config": {"s3": {"bucket_name": "conn_bucket"}}})
    else:
        c = Connection()
    key = "key.txt" if key_kind == "rel_key" else "s3://key_bucket/key.txt"
    if has_bucket == "with_bucket":
        kwargs = {"bucket_name": "kwargs_bucket", "key": key}
    else:
        kwargs = {"key": key}

    mock_base.return_value = c
    if precedence == "unify":  # unify to be processed before provide

        class MyHook(S3Hook):
            @unify_bucket_name_and_key
            @provide_bucket_name
            def do_something(self, bucket_name=None, key=None):
                return bucket_name, key

    else:
        with caplog.at_level("WARNING"):

            class MyHook(S3Hook):
                @provide_bucket_name
                @unify_bucket_name_and_key
                def do_something(self, bucket_name=None, key=None):
                    return bucket_name, key

        assert caplog.records[0].message == "`unify_bucket_name_and_key` should wrap `provide_bucket_name`."
    hook = MyHook()
    assert list(hook.do_something(**kwargs)) == expected


@pytest.mark.parametrize(
    "key_kind, has_conn, has_bucket, expected",
    [
        ("full_key", "no_conn", "no_bucket", ["key_bucket", "key.txt"]),
        ("full_key", "no_conn", "with_bucket", ["kwargs_bucket", "s3://key_bucket/key.txt"]),
        ("full_key", "with_conn", "no_bucket", ["key_bucket", "key.txt"]),
        ("full_key", "with_conn", "with_bucket", ["kwargs_bucket", "s3://key_bucket/key.txt"]),
        ("rel_key", "no_conn", "no_bucket", [None, "key.txt"]),
        ("rel_key", "no_conn", "with_bucket", ["kwargs_bucket", "key.txt"]),
        ("rel_key", "with_conn", "no_bucket", ["conn_bucket", "key.txt"]),
        ("rel_key", "with_conn", "with_bucket", ["kwargs_bucket", "key.txt"]),
    ],
)
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_s3_head_object_decorated_behavior(mock_conn, has_conn, has_bucket, key_kind, expected):
    if has_conn == "with_conn":
        c = Connection(extra={"service_config": {"s3": {"bucket_name": "conn_bucket"}}})
    else:
        c = Connection()
    mock_conn.return_value = c
    key = "key.txt" if key_kind == "rel_key" else "s3://key_bucket/key.txt"
    if has_bucket == "with_bucket":
        kwargs = {"bucket_name": "kwargs_bucket", "key": key}
    else:
        kwargs = {"key": key}

    hook = S3Hook()
    m = MagicMock()
    hook.get_conn = m
    hook.head_object(**kwargs)
    assert list(m.mock_calls[1][2].values()) == expected


def test_unify_and_provide_ordered_properly():
    """
    If we unify first, then we (desirably) get the bucket from the key.  If we provide bucket first,
    something undesirable happens.  The bucket from the connection is used, which means
    we don't respect the full key provided. Further, the full key is not made relative,
    which would cause the actual request to fail. For this reason we want to put unify
    first.
    """
    code = inspect.getsource(S3Hook)
    matches = re.findall(r"@provide_bucket_name\s+@unify_bucket_name_and_key", code, re.MULTILINE)
    if matches:
        pytest.fail("@unify_bucket_name_and_key should be applied before @provide_bucket_name in S3Hook")
