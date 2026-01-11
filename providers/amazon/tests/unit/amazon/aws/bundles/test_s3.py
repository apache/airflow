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

import os
from unittest.mock import MagicMock, call

import boto3
import pytest
from moto import mock_aws

import airflow.version
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import AirflowException

from tests_common.test_utils.config import conf_vars

AWS_CONN_ID_WITH_REGION = "s3_dags_connection"
AWS_CONN_ID_REGION = "eu-central-1"
AWS_CONN_ID_DEFAULT = "aws_default"
S3_BUCKET_NAME = "my-airflow-dags-bucket"
S3_BUCKET_PREFIX = "project1/dags"

if airflow.version.version.strip().startswith("3"):
    from airflow.providers.amazon.aws.bundles.s3 import S3DagBundle


@pytest.fixture
def mocked_s3_resource():
    with mock_aws():
        yield boto3.resource("s3")


@pytest.fixture
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture
def s3_bucket(mocked_s3_resource, s3_client):
    bucket = mocked_s3_resource.create_bucket(Bucket=S3_BUCKET_NAME)

    s3_client.put_object(Bucket=bucket.name, Key=S3_BUCKET_PREFIX + "/dag_01.py", Body=b"test data")
    s3_client.put_object(Bucket=bucket.name, Key=S3_BUCKET_PREFIX + "/dag_02.py", Body=b"test data")
    s3_client.put_object(
        Bucket=bucket.name, Key=S3_BUCKET_PREFIX + "/subproject1/dag_a.py", Body=b"test data"
    )
    s3_client.put_object(
        Bucket=bucket.name, Key=S3_BUCKET_PREFIX + "/subproject1/dag_b.py", Body=b"test data"
    )

    return bucket


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


@pytest.mark.skipif(not airflow.version.version.strip().startswith("3"), reason="Airflow >=3.0.0 test")
class TestS3DagBundle:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=AWS_CONN_ID_DEFAULT,
                conn_type="aws",
                extra={
                    "config_kwargs": {"s3": {"bucket_name": S3_BUCKET_NAME}},
                },
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=AWS_CONN_ID_WITH_REGION,
                conn_type="aws",
                extra={
                    "config_kwargs": {"s3": {"bucket_name": S3_BUCKET_NAME}},
                    "region_name": AWS_CONN_ID_REGION,
                },
            )
        )

    def test_view_url_generates_presigned_url(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1/dags", bucket_name=S3_BUCKET_NAME
        )

        url: str = bundle.view_url("test_version")
        assert url.startswith("https://my-airflow-dags-bucket.s3.amazonaws.com/project1/dags")

    def test_view_url_template_generates_presigned_url(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1/dags", bucket_name=S3_BUCKET_NAME
        )
        url: str = bundle.view_url_template()
        assert url.startswith("https://my-airflow-dags-bucket.s3.amazonaws.com/project1/dags")

    def test_supports_versioning(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1/dags", bucket_name=S3_BUCKET_NAME
        )
        assert S3DagBundle.supports_versioning is False

        # set version, it's not supported
        bundle.version = "test_version"

        with pytest.raises(AirflowException, match="Refreshing a specific version is not supported"):
            bundle.refresh()
        with pytest.raises(AirflowException, match="S3 url with version is not supported"):
            bundle.view_url("test_version")

    def test_correct_bundle_path_used(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1_dags", bucket_name="airflow_dags"
        )
        assert str(bundle.base_dir) == str(bundle.s3_dags_dir)

    def test_s3_bucket_and_prefix_validated(self, s3_bucket):
        hook = S3Hook(aws_conn_id=AWS_CONN_ID_DEFAULT)
        assert hook.check_for_bucket(s3_bucket.name) is True

        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix="project1_dags",
            bucket_name="non-existing-bucket",
        )
        with pytest.raises(AirflowException, match="S3 bucket.*non-existing-bucket.*does not exist.*"):
            bundle.initialize()

        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix="non-existing-prefix",
            bucket_name=S3_BUCKET_NAME,
        )
        with pytest.raises(AirflowException, match="S3 prefix.*non-existing-prefix.*does not exist.*"):
            bundle.initialize()

        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix=S3_BUCKET_PREFIX,
            bucket_name=S3_BUCKET_NAME,
        )
        # initialize succeeds, with correct prefix and bucket
        bundle.initialize()
        assert bundle.s3_hook.region_name == AWS_CONN_ID_REGION

        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix="",
            bucket_name=S3_BUCKET_NAME,
        )
        # initialize succeeds, with empty prefix
        bundle.initialize()
        assert bundle.s3_hook.region_name == AWS_CONN_ID_REGION

    def _upload_fixtures(self, bucket: str, fixtures_dir: str) -> None:
        client = boto3.client("s3")
        fixtures_paths = [
            os.path.join(path, filename) for path, _, files in os.walk(fixtures_dir) for filename in files
        ]
        for path in fixtures_paths:
            key = os.path.relpath(path, fixtures_dir)
            client.upload_file(Filename=path, Bucket=bucket, Key=key)

    def test_refresh(self, s3_bucket, s3_client):
        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix=S3_BUCKET_PREFIX,
            bucket_name=S3_BUCKET_NAME,
        )
        bundle._log.debug = MagicMock()
        # Create a pytest Call object to compare against the call_args_list of the _log.debug mock
        download_log_call = call(
            "Downloading DAGs from s3://%s/%s to %s", S3_BUCKET_NAME, S3_BUCKET_PREFIX, bundle.s3_dags_dir
        )
        bundle.initialize()
        assert bundle._log.debug.call_count == 1
        assert bundle._log.debug.call_args_list == [download_log_call]
        bundle.refresh()
        assert bundle._log.debug.call_count == 2
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call]
        bundle.refresh()
        assert bundle._log.debug.call_count == 3
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call, download_log_call]

    def test_refresh_without_prefix(self, s3_bucket, s3_client):
        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            bucket_name=S3_BUCKET_NAME,
        )
        bundle._log.debug = MagicMock()
        download_log_call = call(
            "Downloading DAGs from s3://%s/%s to %s", S3_BUCKET_NAME, "", bundle.s3_dags_dir
        )
        assert bundle.prefix == ""
        bundle.initialize()
        bundle.refresh()
        assert bundle._log.debug.call_count == 2
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call]
