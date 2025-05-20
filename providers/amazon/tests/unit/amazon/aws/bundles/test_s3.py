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

import logging
import os
import re

import boto3
import pytest
from moto import mock_aws

import airflow.version
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils import db

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections

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
    @classmethod
    def teardown_class(cls) -> None:
        clear_db_connections()

    @classmethod
    def setup_class(cls) -> None:
        db.merge_conn(
            Connection(
                conn_id=AWS_CONN_ID_DEFAULT,
                conn_type="aws",
                extra={
                    "config_kwargs": {"s3": {"bucket_name": S3_BUCKET_NAME}},
                },
            )
        )
        db.merge_conn(
            conn=Connection(
                conn_id=AWS_CONN_ID_WITH_REGION,
                conn_type="aws",
                extra={
                    "config_kwargs": {"s3": {"bucket_name": S3_BUCKET_NAME}},
                    "region_name": AWS_CONN_ID_REGION,
                },
            )
        )

    @pytest.mark.db_test
    def test_view_url_generates_presigned_url(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1/dags", bucket_name=S3_BUCKET_NAME
        )
        url: str = bundle.view_url("test_version")
        assert url.startswith("https://my-airflow-dags-bucket.s3.amazonaws.com/project1/dags")
        assert "AWSAccessKeyId=" in url
        assert "Signature=" in url
        assert "Expires=" in url

    @pytest.mark.db_test
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

    @pytest.mark.db_test
    def test_correct_bundle_path_used(self):
        bundle = S3DagBundle(
            name="test", aws_conn_id=AWS_CONN_ID_DEFAULT, prefix="project1_dags", bucket_name="aiflow_dags"
        )
        assert str(bundle.path) == str(bundle.base_dir) + "/s3/test"

    @pytest.mark.db_test
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

    @pytest.mark.db_test
    def test_refresh(self, s3_bucket, s3_client, caplog, cap_structlog):
        caplog.set_level(logging.ERROR)
        caplog.set_level(logging.DEBUG, logger="airflow.providers.amazon.aws.bundles.s3.S3DagBundle")
        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix=S3_BUCKET_PREFIX,
            bucket_name=S3_BUCKET_NAME,
        )

        bundle.initialize()
        # dags are downloaded once by initialize and once with refresh called post initialize
        assert cap_structlog.text.count("Downloading DAGs from s3") == 2
        self.assert_log_matches_regex(
            caplog=cap_structlog,
            level="DEBUG",
            regex=rf"Downloaded.*{S3_BUCKET_PREFIX}.*subproject1/dag_a.py.*{bundle.s3_dags_dir.as_posix()}.*subproject1/dag_a.py.*",
        )

        s3_client.put_object(Bucket=s3_bucket.name, Key=S3_BUCKET_PREFIX + "/dag_03.py", Body=b"test data")
        bundle.refresh()
        assert cap_structlog.text.count("Downloading DAGs from s3") == 3
        self.assert_log_matches_regex(
            caplog=cap_structlog,
            level="DEBUG",
            regex=rf"Local file.*/s3/{bundle.name}/subproject1/dag_a.py.*is up-to-date with S3 object.*{S3_BUCKET_PREFIX}.*subproject1/dag_a.py.*",
        )
        self.assert_log_matches_regex(
            caplog=cap_structlog,
            level="DEBUG",
            regex=rf"Downloaded.*{S3_BUCKET_PREFIX}.*dag_03.py.*/s3/{bundle.name}/dag_03.py",
        )
        assert bundle.s3_dags_dir.joinpath("dag_03.py").read_text() == "test data"
        bundle.s3_dags_dir.joinpath("dag_should_be_deleted.py").write_text("test dag")
        bundle.s3_dags_dir.joinpath("dag_should_be_deleted_folder").mkdir(exist_ok=True)
        s3_client.put_object(
            Bucket=s3_bucket.name, Key=S3_BUCKET_PREFIX + "/dag_03.py", Body=b"test data-changed"
        )
        bundle.refresh()
        assert cap_structlog.text.count("Downloading DAGs from s3") == 4
        self.assert_log_matches_regex(
            caplog=cap_structlog,
            level="DEBUG",
            regex=r"S3 object size.*and local file size.*differ.*Downloaded.*dag_03.py.*",
        )
        assert bundle.s3_dags_dir.joinpath("dag_03.py").read_text() == "test data-changed"
        self.assert_log_matches_regex(
            caplog=cap_structlog,
            level="DEBUG",
            regex=r"Deleted stale empty directory.*dag_should_be_deleted_folder.*",
        )
        self.assert_log_matches_regex(
            caplog=cap_structlog,
            level="DEBUG",
            regex=r"Deleted stale local file.*dag_should_be_deleted.py.*",
        )

    @pytest.mark.db_test
    def test_refresh_without_prefix(self, s3_bucket, s3_client, caplog, cap_structlog):
        caplog.set_level(logging.ERROR)
        caplog.set_level(logging.DEBUG, logger="airflow.providers.amazon.aws.bundles.s3.S3DagBundle")
        bundle = S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            bucket_name=S3_BUCKET_NAME,
        )
        assert bundle.prefix == ""
        bundle.initialize()

        self.assert_log_matches_regex(
            caplog=cap_structlog,
            level="DEBUG",
            regex=rf"Downloaded.*subproject1/dag_a.py.*{bundle.s3_dags_dir.as_posix()}.*subproject1/dag_a.py.*",
        )
        s3_client.put_object(Bucket=s3_bucket.name, Key=S3_BUCKET_PREFIX + "/dag_03.py", Body=b"test data")
        bundle.refresh()
        assert cap_structlog.text.count("Downloading DAGs from s3") == 3
        self.assert_log_matches_regex(
            caplog=cap_structlog,
            level="DEBUG",
            regex=rf"Local file.*/s3/{bundle.name}.*/dag_a.py.*is up-to-date with S3 object.*dag_a.py.*",
        )
        self.assert_log_matches_regex(
            caplog=cap_structlog,
            level="DEBUG",
            regex=rf"Downloaded.*dag_03.py.*/s3/{bundle.name}.*/dag_03.py",
        )
        # we are using s3 bucket rood but the dag file is in sub folder, project1/dags/dag_03.py
        assert bundle.s3_dags_dir.joinpath("project1/dags/dag_03.py").read_text() == "test data"
        bundle.s3_dags_dir.joinpath("dag_should_be_deleted.py").write_text("test dag")
        bundle.s3_dags_dir.joinpath("dag_should_be_deleted_folder").mkdir(exist_ok=True)

    def assert_log_matches_regex(self, caplog, level, regex):
        """Helper function to assert if a log message matches a regex."""
        matched = False
        for record in caplog.entries:
            if record.get("log_level", None) == level.lower() and re.search(regex, record.get("event", "")):
                matched = True
                break  # Stop searching once a match is found
        assert matched, f"No log message at level {level} matching regex '{regex}' found."
