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
import os
import tempfile
from pathlib import Path
from unittest import mock
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_s3

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, provide_bucket_name, unify_bucket_name_and_key
from airflow.utils.timezone import datetime


@pytest.fixture
def mocked_s3_res():
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture
def s3_bucket(mocked_s3_res):
    bucket = "airflow-test-s3-bucket"
    mocked_s3_res.create_bucket(Bucket=bucket)
    return bucket


class TestAwsS3Hook:
    @mock_s3
    def test_get_conn(self):
        hook = S3Hook()
        assert hook.get_conn() is not None

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

    def test_parse_s3_url(self):
        parsed = S3Hook.parse_s3_url("s3://test/this/is/not/a-real-key.txt")
        assert parsed == ("test", "this/is/not/a-real-key.txt"), "Incorrect parsing of the s3 url"

    def test_parse_s3_url_path_style(self):
        parsed = S3Hook.parse_s3_url("https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/test.jpg")
        assert parsed == ("DOC-EXAMPLE-BUCKET1", "test.jpg"), "Incorrect parsing of the s3 url"

    def test_parse_s3_url_virtual_hosted_style(self):
        parsed = S3Hook.parse_s3_url("https://DOC-EXAMPLE-BUCKET1.s3.us-west-2.amazonaws.com/test.png")
        assert parsed == ("DOC-EXAMPLE-BUCKET1", "test.png"), "Incorrect parsing of the s3 url"

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

    @mock_s3
    def test_get_bucket(self):
        hook = S3Hook()
        assert hook.get_bucket("bucket") is not None

    @mock_s3
    def test_create_bucket_default_region(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        assert hook.get_bucket("new_bucket") is not None

    @mock_s3
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

    @mock_s3
    def test_create_bucket_other_region(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket", region_name="us-east-2")
        bucket = hook.get_bucket("new_bucket")
        assert bucket is not None
        region = bucket.meta.client.get_bucket_location(Bucket=bucket.name).get("LocationConstraint")
        assert region == "us-east-2"

    @mock_s3
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
        bucket.put_object(Key="dir/b", Body=b"b")

        from_datetime = datetime(1992, 3, 8, 18, 52, 51)
        to_datetime = datetime(1993, 3, 14, 21, 52, 42)

        def dummy_object_filter(keys, from_datetime=None, to_datetime=None):
            return []

        assert [] == hook.list_keys(s3_bucket, prefix="non-existent/")
        assert ["a", "dir/b"] == hook.list_keys(s3_bucket)
        assert ["a"] == hook.list_keys(s3_bucket, delimiter="/")
        assert ["dir/b"] == hook.list_keys(s3_bucket, prefix="dir/")
        assert ["dir/b"] == hook.list_keys(s3_bucket, start_after_key="a")
        assert [] == hook.list_keys(s3_bucket, from_datetime=from_datetime, to_datetime=to_datetime)
        assert [] == hook.list_keys(
            s3_bucket, from_datetime=from_datetime, to_datetime=to_datetime, object_filter=dummy_object_filter
        )

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
        bucket.put_object(Key="my_key", Body=b"Cont\xC3\xA9nt")

        assert hook.read_key("my_key", s3_bucket) == "Contént"

    # As of 1.3.2, Moto doesn't support select_object_content yet.
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_client_type")
    def test_select_key(self, mock_get_client_type, s3_bucket):
        mock_get_client_type.return_value.select_object_content.return_value = {
            "Payload": [{"Records": {"Payload": b"Cont\xC3"}}, {"Records": {"Payload": b"\xA9nt"}}]
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
        assert resource.get()["Body"].read() == b"Cont\xC3\xA9nt"

    def test_load_string_compress(self, s3_bucket):
        hook = S3Hook()
        hook.load_string("Contént", "my_key", s3_bucket, compression="gzip")
        resource = boto3.resource("s3").Object(s3_bucket, "my_key")
        data = gz.decompress(resource.get()["Body"].read())
        assert data == b"Cont\xC3\xA9nt"

    def test_load_string_compress_exception(self, s3_bucket):
        hook = S3Hook()
        with pytest.raises(NotImplementedError):
            hook.load_string("Contént", "my_key", s3_bucket, compression="bad-compression")

    def test_load_string_acl(self, s3_bucket):
        hook = S3Hook()
        hook.load_string("Contént", "my_key", s3_bucket, acl_policy="public-read")
        response = boto3.client("s3").get_object_acl(Bucket=s3_bucket, Key="my_key", RequestPayer="requester")
        assert (response["Grants"][1]["Permission"] == "READ") and (
            response["Grants"][0]["Permission"] == "FULL_CONTROL"
        )

    def test_load_bytes(self, s3_bucket):
        hook = S3Hook()
        hook.load_bytes(b"Content", "my_key", s3_bucket)
        resource = boto3.resource("s3").Object(s3_bucket, "my_key")
        assert resource.get()["Body"].read() == b"Content"

    def test_load_bytes_acl(self, s3_bucket):
        hook = S3Hook()
        hook.load_bytes(b"Content", "my_key", s3_bucket, acl_policy="public-read")
        response = boto3.client("s3").get_object_acl(Bucket=s3_bucket, Key="my_key", RequestPayer="requester")
        assert (response["Grants"][1]["Permission"] == "READ") and (
            response["Grants"][0]["Permission"] == "FULL_CONTROL"
        )

    def test_load_fileobj(self, s3_bucket):
        hook = S3Hook()
        with tempfile.TemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", s3_bucket)
            resource = boto3.resource("s3").Object(s3_bucket, "my_key")
            assert resource.get()["Body"].read() == b"Content"

    def test_load_fileobj_acl(self, s3_bucket):
        hook = S3Hook()
        with tempfile.TemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", s3_bucket, acl_policy="public-read")
            response = boto3.client("s3").get_object_acl(
                Bucket=s3_bucket, Key="my_key", RequestPayer="requester"
            )
            assert (response["Grants"][1]["Permission"] == "READ") and (
                response["Grants"][0]["Permission"] == "FULL_CONTROL"
            )

    def test_load_file_gzip(self, s3_bucket):
        hook = S3Hook()
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file(temp_file.name, "my_key", s3_bucket, gzip=True)
            resource = boto3.resource("s3").Object(s3_bucket, "my_key")
            assert gz.decompress(resource.get()["Body"].read()) == b"Content"
            os.unlink(temp_file.name)

    def test_load_file_acl(self, s3_bucket):
        hook = S3Hook()
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file(temp_file.name, "my_key", s3_bucket, gzip=True, acl_policy="public-read")
            response = boto3.client("s3").get_object_acl(
                Bucket=s3_bucket, Key="my_key", RequestPayer="requester"
            )
            assert (response["Grants"][1]["Permission"] == "READ") and (
                response["Grants"][0]["Permission"] == "FULL_CONTROL"
            )
            os.unlink(temp_file.name)

    def test_copy_object_acl(self, s3_bucket):
        hook = S3Hook()
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", s3_bucket)
            hook.copy_object("my_key", "my_key", s3_bucket, s3_bucket)
            response = boto3.client("s3").get_object_acl(
                Bucket=s3_bucket, Key="my_key", RequestPayer="requester"
            )
            assert (response["Grants"][0]["Permission"] == "FULL_CONTROL") and (len(response["Grants"]) == 1)

    @mock_s3
    def test_delete_bucket_if_bucket_exist(self, s3_bucket):
        # assert if the bucket is created
        mock_hook = S3Hook()
        mock_hook.create_bucket(bucket_name=s3_bucket)
        assert mock_hook.check_for_bucket(bucket_name=s3_bucket)
        mock_hook.delete_bucket(bucket_name=s3_bucket, force_delete=True)
        assert not mock_hook.check_for_bucket(s3_bucket)

    @mock_s3
    def test_delete_bucket_if_not_bucket_exist(self, s3_bucket):
        # assert if exception is raised if bucket not present
        mock_hook = S3Hook()
        with pytest.raises(ClientError) as ctx:
            assert mock_hook.delete_bucket(bucket_name=s3_bucket, force_delete=True)
        assert ctx.value.response["Error"]["Code"] == "NoSuchBucket"

    @mock.patch.object(S3Hook, "get_connection", return_value=Connection(schema="test_bucket"))
    def test_provide_bucket_name(self, mock_get_connection):
        class FakeS3Hook(S3Hook):
            @provide_bucket_name
            def test_function(self, bucket_name=None):
                return bucket_name

        fake_s3_hook = FakeS3Hook()

        test_bucket_name = fake_s3_hook.test_function()
        assert test_bucket_name == mock_get_connection.return_value.schema

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
    def test_download_file(self, mock_temp_file):
        with tempfile.NamedTemporaryFile(dir="/tmp", prefix="airflow_tmp_test_s3_hook") as temp_file:
            mock_temp_file.return_value = temp_file
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
                temp_file,
                Config=s3_hook.transfer_config,
                ExtraArgs=s3_hook.extra_args,
            )

            assert temp_file.name == output_file

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.open")
    def test_download_file_with_preserve_name(self, mock_open):
        file_name = "test.log"
        bucket = "test_bucket"
        key = f"test_key/{file_name}"
        local_folder = "/tmp"

        s3_hook = S3Hook(aws_conn_id="s3_test")
        s3_hook.check_for_key = Mock(return_value=True)
        s3_obj = Mock()
        s3_obj.key = f"s3://{bucket}/{key}"
        s3_obj.download_fileobj = Mock(return_value=None)
        s3_hook.get_key = Mock(return_value=s3_obj)
        s3_hook.download_file(
            key=key,
            bucket_name=bucket,
            local_path=local_folder,
            preserve_file_name=True,
            use_autogenerated_subdir=False,
        )

        mock_open.assert_called_once_with(Path(local_folder, file_name), "wb")

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.open")
    def test_download_file_with_preserve_name_with_autogenerated_subdir(self, mock_open):
        file_name = "test.log"
        bucket = "test_bucket"
        key = f"test_key/{file_name}"
        local_folder = "/tmp"

        s3_hook = S3Hook(aws_conn_id="s3_test")
        s3_hook.check_for_key = Mock(return_value=True)
        s3_obj = Mock()
        s3_obj.key = f"s3://{bucket}/{key}"
        s3_obj.download_fileobj = Mock(return_value=None)
        s3_hook.get_key = Mock(return_value=s3_obj)
        result_file = s3_hook.download_file(
            key=key,
            bucket_name=bucket,
            local_path=local_folder,
            preserve_file_name=True,
            use_autogenerated_subdir=True,
        )

        assert result_file.rsplit("/", 1)[-2].startswith("airflow_tmp_dir_")

    def test_download_file_with_preserve_name_file_already_exists(self):
        with tempfile.NamedTemporaryFile(dir="/tmp", prefix="airflow_tmp_test_s3_hook") as file:
            file_name = file.name.rsplit("/", 1)[-1]
            bucket = "test_bucket"
            key = f"test_key/{file_name}"
            local_folder = "/tmp"
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
                    local_path=local_folder,
                    preserve_file_name=True,
                    use_autogenerated_subdir=False,
                )

    def test_generate_presigned_url(self, s3_bucket):
        hook = S3Hook()
        presigned_url = hook.generate_presigned_url(
            client_method="get_object", params={"Bucket": s3_bucket, "Key": "my_key"}
        )

        url = presigned_url.split("?")[1]
        params = {x[0]: x[1] for x in [x.split("=") for x in url[0:].split("&")]}

        assert {"AWSAccessKeyId", "Signature", "Expires"}.issubset(set(params.keys()))

    def test_should_throw_error_if_extra_args_is_not_dict(self):
        with pytest.raises(TypeError, match="extra_args expected dict, got .*"):
            S3Hook(extra_args=1)

    def test_should_throw_error_if_extra_args_contains_unknown_arg(self, s3_bucket):
        hook = S3Hook(extra_args={"unknown_s3_args": "value"})
        with tempfile.TemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            with pytest.raises(ValueError):
                hook.load_file_obj(temp_file, "my_key", s3_bucket, acl_policy="public-read")

    def test_should_pass_extra_args(self, s3_bucket):
        hook = S3Hook(extra_args={"ContentLanguage": "value"})
        with tempfile.TemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", s3_bucket, acl_policy="public-read")
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

    @mock_s3
    def test_get_bucket_tagging_no_tags_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")

        with pytest.raises(ClientError, match=r".*NoSuchTagSet.*"):
            hook.get_bucket_tagging(bucket_name="new_bucket")

    @mock_s3
    def test_get_bucket_tagging_no_bucket_raises_error(self):
        hook = S3Hook()

        with pytest.raises(ClientError, match=r".*NoSuchBucket.*"):
            hook.get_bucket_tagging(bucket_name="new_bucket")

    @mock_s3
    def test_put_bucket_tagging_with_valid_set(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        hook.put_bucket_tagging(bucket_name="new_bucket", tag_set=tag_set)

        assert hook.get_bucket_tagging(bucket_name="new_bucket") == tag_set

    @mock_s3
    def test_put_bucket_tagging_with_pair(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        key = "Color"
        value = "Green"
        hook.put_bucket_tagging(bucket_name="new_bucket", key=key, value=value)

        assert hook.get_bucket_tagging(bucket_name="new_bucket") == tag_set

    @mock_s3
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

    @mock_s3
    def test_put_bucket_tagging_with_key_but_no_value_raises_error(self):
        hook = S3Hook()

        hook.create_bucket(bucket_name="new_bucket")
        key = "Color"
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name="new_bucket", key=key)

    @mock_s3
    def test_put_bucket_tagging_with_value_but_no_key_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        value = "Color"
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name="new_bucket", value=value)

    @mock_s3
    def test_put_bucket_tagging_with_key_and_set_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        key = "Color"
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name="new_bucket", key=key, tag_set=tag_set)

    @mock_s3
    def test_put_bucket_tagging_with_value_and_set_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        value = "Green"
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name="new_bucket", value=value, tag_set=tag_set)

    @mock_s3
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

    @mock_s3
    def test_delete_bucket_tagging(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")
        tag_set = [{"Key": "Color", "Value": "Green"}]
        hook.put_bucket_tagging(bucket_name="new_bucket", tag_set=tag_set)
        hook.get_bucket_tagging(bucket_name="new_bucket")

        hook.delete_bucket_tagging(bucket_name="new_bucket")
        with pytest.raises(ClientError, match=r".*NoSuchTagSet.*"):
            hook.get_bucket_tagging(bucket_name="new_bucket")

    @mock_s3
    def test_delete_bucket_tagging_with_no_tags(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name="new_bucket")

        hook.delete_bucket_tagging(bucket_name="new_bucket")

        with pytest.raises(ClientError, match=r".*NoSuchTagSet.*"):
            hook.get_bucket_tagging(bucket_name="new_bucket")
