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

import io
import os
import tarfile
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
S3_ARCHIVE_KEY = "bundle-archives/dags.tar.gz"


def _make_archive(files: dict[str, bytes]) -> bytes:
    """Build an in-memory .tar.gz with the given member name -> content mapping."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, data in files.items():
            tar_info = tarfile.TarInfo(name=name)
            tar_info.size = len(data)
            tar.addfile(tar_info, io.BytesIO(data))
    return buffer.getvalue()


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
            "Downloading Dags from s3://%s/%s to %s", S3_BUCKET_NAME, S3_BUCKET_PREFIX, bundle.s3_dags_dir
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
            "Downloading Dags from s3://%s/%s to %s", S3_BUCKET_NAME, "", bundle.s3_dags_dir
        )
        assert bundle.prefix == ""
        bundle.initialize()
        bundle.refresh()
        assert bundle._log.debug.call_count == 2
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call]

    def _archive_bundle(self) -> S3DagBundle:
        return S3DagBundle(
            name="test",
            aws_conn_id=AWS_CONN_ID_WITH_REGION,
            prefix=S3_BUCKET_PREFIX,
            archive_key=S3_ARCHIVE_KEY,
            bucket_name=S3_BUCKET_NAME,
        )

    def test_refresh_from_archive(self, s3_bucket, s3_client):
        archive = _make_archive({"dag_01.py": b"test data", "subproject1/dag_a.py": b"test data"})
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_ARCHIVE_KEY, Body=archive)

        bundle = self._archive_bundle()
        bundle.initialize()

        assert (bundle.path / "dag_01.py").is_file()
        assert (bundle.path / "subproject1" / "dag_a.py").is_file()
        assert (bundle.path / bundle.archive_etag_marker).is_file()
        # dag_02.py exists under the prefix but not in the archive: staging used
        # the archive, not the per-object sync
        assert not (bundle.path / "dag_02.py").exists()

    def test_refresh_from_archive_skips_staging_when_etag_unchanged(self, s3_bucket, s3_client):
        archive = _make_archive({"dag_01.py": b"test data"})
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_ARCHIVE_KEY, Body=archive)

        bundle = self._archive_bundle()
        bundle.initialize()

        # A local sentinel file would be wiped by the swap of a re-staged tree
        sentinel = bundle.path / "sentinel.txt"
        sentinel.write_text("still here")
        bundle.refresh()
        assert sentinel.is_file()

        # A changed archive (new ETag) re-stages: additions appear, removed
        # members and local strays disappear with the swapped tree
        updated = _make_archive({"dag_new.py": b"test data"})
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_ARCHIVE_KEY, Body=updated)
        bundle.refresh()
        assert (bundle.path / "dag_new.py").is_file()
        assert not (bundle.path / "dag_01.py").exists()
        assert not sentinel.exists()

    def test_refresh_from_archive_without_prefix_objects(self, mocked_s3_resource, s3_client):
        # Bucket contains only the archive - no objects under the prefix at all
        mocked_s3_resource.create_bucket(Bucket=S3_BUCKET_NAME)
        archive = _make_archive({"dag_01.py": b"test data"})
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_ARCHIVE_KEY, Body=archive)

        bundle = self._archive_bundle()
        # succeeds: the prefix-existence check does not apply when staging from the archive
        bundle.initialize()
        assert (bundle.path / "dag_01.py").is_file()

    def test_refresh_falls_back_to_sync_when_archive_corrupt(self, s3_bucket, s3_client):
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_ARCHIVE_KEY, Body=b"this is not a tarball")

        bundle = self._archive_bundle()
        bundle.initialize()

        # per-object sync of the prefix staged the Dags instead
        assert (bundle.path / "dag_01.py").is_file()
        assert (bundle.path / "dag_02.py").is_file()
        assert (bundle.path / "subproject1" / "dag_a.py").is_file()
        assert not (bundle.path / bundle.archive_etag_marker).exists()

    def test_refresh_falls_back_to_sync_when_archive_missing(self, s3_bucket, s3_client):
        bundle = self._archive_bundle()
        bundle.initialize()

        assert (bundle.path / "dag_01.py").is_file()
        assert (bundle.path / "dag_02.py").is_file()
        assert not (bundle.path / bundle.archive_etag_marker).exists()

    def test_fallback_sync_clears_etag_marker(self, s3_bucket, s3_client):
        archive = _make_archive({"dag_01.py": b"test data"})
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_ARCHIVE_KEY, Body=archive)

        bundle = self._archive_bundle()
        bundle.initialize()
        assert (bundle.path / bundle.archive_etag_marker).is_file()

        # Archive disappears: refresh falls back to the per-object sync, which
        # must remove the marker so a re-published identical archive is not
        # wrongly skipped later
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=S3_ARCHIVE_KEY)
        bundle.refresh()
        assert (bundle.path / "dag_02.py").is_file()
        assert not (bundle.path / bundle.archive_etag_marker).exists()
