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

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, call, patch

import boto3
import pytest
from moto import mock_aws

import airflow.version
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import AirflowException

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS, AIRFLOW_V_3_4_PLUS

AWS_CONN_ID_WITH_REGION = "s3_dags_connection"
AWS_CONN_ID_REGION = "eu-central-1"
AWS_CONN_ID_DEFAULT = "aws_default"
S3_BUCKET_NAME = "my-airflow-dags-bucket"
S3_BUCKET_PREFIX = "project1/dags"
S3_MANIFEST_KEY = "deployments/current.json"
TEST_SHA256 = "0" * 64

if airflow.version.version.strip().startswith("3"):
    from airflow.providers.amazon.aws.bundles.s3 import (
        S3DagBundle,
        S3DagBundleConfigError,
        S3DagBundleIntegrityError,
        S3DagBundleManifestError,
    )

if AIRFLOW_V_3_3_PLUS:
    from airflow.dag_processing.bundles.base import BundleVersion


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


@pytest.fixture
def versioned_s3_bucket(s3_client):
    s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
    s3_client.put_bucket_versioning(
        Bucket=S3_BUCKET_NAME,
        VersioningConfiguration={"Status": "Enabled"},
    )
    return S3_BUCKET_NAME


def manifest_version(manifest: dict) -> str:
    canonical_manifest = {
        "bucket_name": manifest["bucket_name"],
        "objects": sorted(manifest["objects"], key=lambda obj: obj["key"]),
        "prefix": manifest["prefix"],
        "schema_version": 1,
    }
    return hashlib.sha256(
        json.dumps(
            canonical_manifest,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


def publish_release(s3_client, manifest: dict, *, update_pointer: bool = True) -> str:
    version = manifest_version(manifest)
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=f"{S3_MANIFEST_KEY}.releases/{version}.json",
        Body=json.dumps(manifest).encode(),
    )
    if update_pointer:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_MANIFEST_KEY,
            Body=json.dumps({"schema_version": 1, "bundle_version": version}).encode(),
        )
    return version


def publish_manifest(s3_client, files: dict[str, bytes]) -> tuple[dict, str]:
    """Upload objects, their immutable release manifest, and the current pointer in that order."""
    objects = []
    for key, body in files.items():
        response = s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=body)
        objects.append(
            {
                "key": key,
                "version_id": response["VersionId"],
                "size": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
            }
        )
    manifest = {
        "schema_version": 1,
        "bucket_name": S3_BUCKET_NAME,
        "prefix": S3_BUCKET_PREFIX,
        "objects": objects,
    }
    version = publish_release(s3_client, manifest)
    return manifest, version


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
        assert bundle.s3_dags_dir == bundle.base_dir / "tracking"

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


@pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="S3 manifest versioning requires Airflow >=3.4")
class TestS3DagBundleManifest:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=AWS_CONN_ID_DEFAULT,
                conn_type="aws",
                extra={"config_kwargs": {"s3": {"bucket_name": S3_BUCKET_NAME}}},
            )
        )

    @staticmethod
    def _bundle(name="manifest-test", **kwargs):
        return S3DagBundle(
            name=name,
            aws_conn_id=AWS_CONN_ID_DEFAULT,
            bucket_name=S3_BUCKET_NAME,
            prefix=S3_BUCKET_PREFIX,
            manifest_key=S3_MANIFEST_KEY,
            **kwargs,
        )

    def test_manifest_mode_is_opt_in_per_instance(self):
        manifest_bundle = self._bundle()
        legacy_bundle = S3DagBundle(
            name="legacy",
            aws_conn_id=AWS_CONN_ID_DEFAULT,
            bucket_name=S3_BUCKET_NAME,
            prefix=S3_BUCKET_PREFIX,
        )

        assert manifest_bundle.supports_versioning is True
        assert legacy_bundle.supports_versioning is False
        assert S3DagBundle.supports_versioning is False

    def test_manifest_mode_requires_airflow_3_4(self):
        with (
            patch("airflow.providers.amazon.aws.bundles.s3.AIRFLOW_V_3_4_PLUS", False),
            pytest.raises(S3DagBundleConfigError, match="Airflow 3.4 or later"),
        ):
            self._bundle()

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"manifest_key": ""},
            {"manifest_key": "../current.json"},
            {"manifest_key": "nested//current.json"},
            {"manifest_key": "x" * 950},
            {"prefix": "dags//"},
            {"prefix": "../dags"},
        ],
    )
    def test_manifest_configuration_rejects_ambiguous_keys(self, kwargs):
        defaults = {
            "name": "invalid-config",
            "aws_conn_id": AWS_CONN_ID_DEFAULT,
            "bucket_name": S3_BUCKET_NAME,
            "prefix": S3_BUCKET_PREFIX,
            "manifest_key": S3_MANIFEST_KEY,
        }

        with pytest.raises(S3DagBundleConfigError):
            S3DagBundle(**{**defaults, **kwargs})

    def test_manifest_publishes_complete_generation(self, versioned_s3_bucket):
        publish_manifest(
            boto3.client("s3"),
            {
                f"{S3_BUCKET_PREFIX}/dag.py": b"from helpers import VALUE\n",
                f"{S3_BUCKET_PREFIX}/helpers.py": b"VALUE = 'v1'\n",
            },
        )
        bundle = self._bundle()

        bundle.initialize()
        current = bundle.get_current_version()

        assert isinstance(current, BundleVersion)
        assert bundle.path == bundle.versions_dir / current.version
        assert (bundle.path / "dag.py").read_bytes() == b"from helpers import VALUE\n"
        assert (bundle.path / "helpers.py").read_bytes() == b"VALUE = 'v1'\n"
        assert current.data is None

    def test_objects_are_not_published_until_manifest_changes(self, versioned_s3_bucket):
        s3_client = boto3.client("s3")
        publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        bundle = self._bundle()
        bundle.initialize()
        version_one = bundle.get_current_version()
        path_one = bundle.path

        response = s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"{S3_BUCKET_PREFIX}/dag.py",
            Body=b"VERSION = 2\n",
        )
        manifest = {
            "schema_version": 1,
            "bucket_name": S3_BUCKET_NAME,
            "prefix": S3_BUCKET_PREFIX,
            "objects": [
                {
                    "key": f"{S3_BUCKET_PREFIX}/dag.py",
                    "version_id": response["VersionId"],
                    "size": len(b"VERSION = 2\n"),
                    "sha256": hashlib.sha256(b"VERSION = 2\n").hexdigest(),
                }
            ],
        }
        version_two = publish_release(s3_client, manifest, update_pointer=False)
        bundle.refresh()

        assert bundle.path == path_one
        assert bundle.get_current_version() == version_one
        assert (bundle.path / "dag.py").read_bytes() == b"VERSION = 1\n"

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_MANIFEST_KEY,
            Body=json.dumps({"schema_version": 1, "bundle_version": version_two}).encode(),
        )
        bundle.refresh()

        assert bundle.path != path_one
        assert bundle.get_current_version() != version_one
        assert (bundle.path / "dag.py").read_bytes() == b"VERSION = 2\n"

    def test_pinned_bundle_downloads_historical_manifest_and_objects(self, versioned_s3_bucket):
        s3_client = boto3.client("s3")
        publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        current_bundle = self._bundle(name="current")
        current_bundle.initialize()
        version_one = current_bundle.get_current_version()

        publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 2\n"})
        pinned_bundle = S3DagBundle(
            name="pinned",
            aws_conn_id=AWS_CONN_ID_DEFAULT,
            bucket_name=S3_BUCKET_NAME,
            prefix=S3_BUCKET_PREFIX,
            manifest_key=S3_MANIFEST_KEY,
            version=version_one.version,
            version_data=None,
        )
        pinned_bundle.initialize()

        assert pinned_bundle.manifest_key == S3_MANIFEST_KEY
        assert pinned_bundle.get_current_version() == version_one
        assert (pinned_bundle.path / "dag.py").read_bytes() == b"VERSION = 1\n"

    def test_get_current_version_describes_published_path(self, versioned_s3_bucket):
        s3_client = boto3.client("s3")
        publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        bundle = self._bundle()
        bundle.initialize()
        published_version = bundle.get_current_version()

        publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 2\n"})

        assert bundle.get_current_version() == published_version
        assert bundle.path == bundle.versions_dir / published_version.version
        assert (bundle.path / "dag.py").read_bytes() == b"VERSION = 1\n"

    def test_partial_download_keeps_last_good_generation(self, versioned_s3_bucket):
        s3_client = boto3.client("s3")
        publish_manifest(
            s3_client,
            {
                f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n",
                f"{S3_BUCKET_PREFIX}/helper.py": b"VALUE = 1\n",
            },
        )
        bundle = self._bundle()
        bundle.initialize()
        previous_path = bundle.path
        previous_version = bundle.get_current_version()
        publish_manifest(
            s3_client,
            {
                f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 2\n",
                f"{S3_BUCKET_PREFIX}/helper.py": b"VALUE = 2\n",
            },
        )
        failed_generation_path = bundle.versions_dir / bundle._read_current_pointer()

        client = bundle.s3_hook.get_conn()
        original_download = client.download_file
        download_count = 0

        def fail_second_download(**kwargs):
            nonlocal download_count
            download_count += 1
            if download_count == 2:
                raise OSError("injected download failure")
            return original_download(**kwargs)

        client.download_file = fail_second_download
        with pytest.raises(S3DagBundleIntegrityError, match="Could not download S3 object"):
            bundle.refresh()

        assert bundle.path == previous_path
        assert bundle.get_current_version() == previous_version
        assert (bundle.path / "dag.py").read_bytes() == b"VERSION = 1\n"
        assert (bundle.path / "helper.py").read_bytes() == b"VALUE = 1\n"
        assert not list(bundle.versions_dir.glob(".s3-staging-*"))
        assert not failed_generation_path.exists()

    def test_missing_object_version_keeps_last_good_generation(self, versioned_s3_bucket):
        s3_client = boto3.client("s3")
        publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        bundle = self._bundle()
        bundle.initialize()
        previous_path = bundle.path
        previous_version = bundle.get_current_version()

        manifest, _ = publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 2\n"})
        obj = manifest["objects"][0]
        s3_client.delete_object(
            Bucket=S3_BUCKET_NAME,
            Key=obj["key"],
            VersionId=obj["version_id"],
        )

        with pytest.raises(S3DagBundleIntegrityError, match="Could not download S3 object"):
            bundle.refresh()

        assert bundle.path == previous_path
        assert bundle.get_current_version() == previous_version
        assert (bundle.path / "dag.py").read_bytes() == b"VERSION = 1\n"

    def test_pointer_to_missing_release_keeps_last_good_generation(self, versioned_s3_bucket):
        s3_client = boto3.client("s3")
        publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        bundle = self._bundle()
        bundle.initialize()
        previous_path = bundle.path
        previous_version = bundle.get_current_version()
        missing_version = "f" * 64
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_MANIFEST_KEY,
            Body=json.dumps({"schema_version": 1, "bundle_version": missing_version}).encode(),
        )

        with pytest.raises(S3DagBundleManifestError, match="Could not read.*release"):
            bundle.refresh()

        assert bundle.path == previous_path
        assert bundle.get_current_version() == previous_version
        assert (bundle.path / "dag.py").read_bytes() == b"VERSION = 1\n"
        assert not (bundle.versions_dir / missing_version).exists()
        assert not list(bundle.versions_dir.glob(".s3-staging-*"))

    def test_pinned_manifest_hash_mismatch_fails_before_download(self, versioned_s3_bucket):
        s3_client = boto3.client("s3")
        manifest, _ = publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        current_bundle = self._bundle(name="current")
        current_bundle.initialize()
        pinned_bundle = self._bundle(
            name="pinned",
            version="0" * 64,
            version_data=None,
        )
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"{S3_MANIFEST_KEY}.releases/{'0' * 64}.json",
            Body=json.dumps(manifest).encode(),
        )
        client = pinned_bundle.s3_hook.get_conn()
        client.download_file = MagicMock()

        with pytest.raises(S3DagBundleManifestError, match="does not match requested version"):
            pinned_bundle.initialize()

        client.download_file.assert_not_called()
        assert not pinned_bundle.path.exists()

    def test_existing_generation_is_reused_without_download(self, versioned_s3_bucket):
        publish_manifest(boto3.client("s3"), {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        first_bundle = self._bundle()
        first_bundle.initialize()
        current = first_bundle.get_current_version()
        pinned_bundle = self._bundle(version=current.version, version_data=None)
        pinned_bundle.s3_hook.check_for_bucket = MagicMock()
        client = pinned_bundle.s3_hook.get_conn()
        client.get_object = MagicMock()
        client.download_file = MagicMock()

        pinned_bundle.initialize()

        pinned_bundle.s3_hook.check_for_bucket.assert_not_called()
        client.get_object.assert_not_called()
        client.download_file.assert_not_called()
        assert pinned_bundle.path == first_bundle.path

    def test_current_instance_reuses_generation_after_reading_only_pointer(self, versioned_s3_bucket, mocker):
        publish_manifest(boto3.client("s3"), {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        first_bundle = self._bundle()
        first_bundle.initialize()
        second_bundle = self._bundle()
        client = second_bundle.s3_hook.get_conn()
        get_object = mocker.spy(client, "get_object")
        download_file = mocker.spy(client, "download_file")

        second_bundle.initialize()

        assert [call.kwargs["Key"] for call in get_object.call_args_list] == [S3_MANIFEST_KEY]
        download_file.assert_not_called()
        assert second_bundle.path == first_bundle.path

    def test_pinned_get_current_version_does_not_read_release(self):
        pinned_bundle = self._bundle(version=TEST_SHA256, version_data=None)
        pinned_bundle.s3_hook.get_conn().get_object = MagicMock()

        assert pinned_bundle.get_current_version() == BundleVersion(version=TEST_SHA256)
        pinned_bundle.s3_hook.get_conn().get_object.assert_not_called()

    def test_concurrent_initializers_publish_one_complete_generation(self, versioned_s3_bucket):
        publish_manifest(boto3.client("s3"), {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        bundles = [self._bundle(), self._bundle()]

        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(lambda bundle: bundle.initialize(), bundles))

        assert bundles[0].path == bundles[1].path
        assert (bundles[0].path / "dag.py").read_bytes() == b"VERSION = 1\n"
        assert not list(bundles[0].versions_dir.glob(".s3-staging-*"))

    def test_pinned_bundle_skips_pointer_and_downloads_exact_object_versions(
        self, versioned_s3_bucket, mocker
    ):
        s3_client = boto3.client("s3")
        manifest, version = publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"{S3_BUCKET_PREFIX}/dag.py",
            Body=b"VERSION = 2\n",
        )
        bundle = self._bundle(name="exact-version", version=version, version_data=None)
        client = bundle.s3_hook.get_conn()
        get_object = mocker.spy(client, "get_object")
        download_file = mocker.spy(client, "download_file")

        bundle.initialize()

        requested_keys = [item.kwargs["Key"] for item in get_object.call_args_list]
        assert requested_keys[0] == f"{S3_MANIFEST_KEY}.releases/{version}.json"
        assert S3_MANIFEST_KEY not in requested_keys
        download_file.assert_called_once()
        assert (
            download_file.call_args.kwargs["ExtraArgs"]["VersionId"] == manifest["objects"][0]["version_id"]
        )
        assert (bundle.path / "dag.py").read_bytes() == b"VERSION = 1\n"

    def test_manifest_mode_enforces_requester_pays_for_metadata_and_object_requests(
        self, versioned_s3_bucket, mocker
    ):
        s3_client = boto3.client("s3")
        manifest, version = publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        bundle = self._bundle(requester_pays=True)
        # Connection-level extras must not weaken the manifest's exact-version or requester-pays contract.
        bundle.s3_hook._extra_args = {"RequestPayer": "bucket-owner", "VersionId": "wrong-version"}
        client = bundle.s3_hook.get_conn()
        get_object = mocker.spy(client, "get_object")
        download_file = mocker.spy(client, "download_file")

        bundle.initialize()

        metadata_keys = {S3_MANIFEST_KEY, f"{S3_MANIFEST_KEY}.releases/{version}.json"}
        metadata_calls = [call for call in get_object.call_args_list if call.kwargs["Key"] in metadata_keys]
        assert {call.kwargs["Key"] for call in metadata_calls} == metadata_keys
        assert all(call.kwargs["RequestPayer"] == "requester" for call in metadata_calls)
        assert all("VersionId" not in call.kwargs for call in metadata_calls)
        assert download_file.call_args.kwargs["ExtraArgs"] == {
            "VersionId": manifest["objects"][0]["version_id"],
            "RequestPayer": "requester",
        }

    def test_orphaned_stage_is_removed(self, versioned_s3_bucket):
        publish_manifest(boto3.client("s3"), {})
        bundle = self._bundle()
        orphan = bundle.versions_dir / ".s3-staging-abandoned"
        orphan.mkdir(parents=True)
        (orphan / "partial.py").write_text("partial")

        bundle.initialize()

        assert not orphan.exists()
        assert bundle.path.is_dir()
        assert {path.name for path in bundle.path.iterdir()} == {".airflow-s3-generation.json"}

    def test_semantic_manifest_hash_is_order_independent(self):
        bundle = self._bundle()
        objects = [
            {
                "key": f"{S3_BUCKET_PREFIX}/b.py",
                "version_id": "b-version",
                "size": 2,
                "sha256": TEST_SHA256,
            },
            {
                "key": f"{S3_BUCKET_PREFIX}/a.py",
                "version_id": "a-version",
                "size": 1,
                "sha256": TEST_SHA256,
            },
        ]

        first = bundle._parse_manifest(
            {
                "schema_version": 1,
                "bucket_name": S3_BUCKET_NAME,
                "prefix": S3_BUCKET_PREFIX,
                "objects": objects,
            }
        )
        second = bundle._parse_manifest(
            {
                "objects": list(reversed(objects)),
                "prefix": S3_BUCKET_PREFIX,
                "bucket_name": S3_BUCKET_NAME,
                "schema_version": 1,
            }
        )

        assert first.version == second.version

    def test_semantic_manifest_hash_matches_canonical_unicode_vector(self):
        manifest = {
            "schema_version": 1,
            "bucket_name": S3_BUCKET_NAME,
            "prefix": S3_BUCKET_PREFIX,
            "objects": [
                {
                    "key": f"{S3_BUCKET_PREFIX}/café.py",
                    "version_id": "v1",
                    "size": 1,
                    "sha256": TEST_SHA256,
                }
            ],
        }

        expected_version = "42a81dc4cd69060b6eebcfb5a71af45c4a6528114b7ddb5c8e8120024bbb66d0"
        assert manifest_version(manifest) == expected_version
        assert self._bundle()._parse_manifest(manifest).version == expected_version

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("key", f"{S3_BUCKET_PREFIX}/changed.py"),
            ("version_id", "changed-version"),
            ("size", 2),
            ("sha256", "1" * 64),
        ],
    )
    def test_each_object_field_changes_semantic_version(self, field, value):
        bundle = self._bundle()
        original = {
            "key": f"{S3_BUCKET_PREFIX}/dag.py",
            "version_id": "v1",
            "size": 1,
            "sha256": TEST_SHA256,
        }
        changed = {**original, field: value}
        release = {
            "schema_version": 1,
            "bucket_name": S3_BUCKET_NAME,
            "prefix": S3_BUCKET_PREFIX,
            "objects": [original],
        }
        changed_release = {**release, "objects": [changed]}

        assert bundle._parse_manifest(release).version != bundle._parse_manifest(changed_release).version

    @pytest.mark.parametrize(
        ("manifest", "error"),
        [
            ({"schema_version": 2, "objects": []}, "schema_version"),
            ({"schema_version": True, "objects": []}, "schema_version"),
            ({"schema_version": 1, "objects": {}}, "objects must be a list"),
            (
                {"schema_version": 1, "objects": [], "unexpected": True},
                "schema_version",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/dag.py",
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                            "unexpected": True,
                        }
                    ],
                },
                "invalid fields",
            ),
            ({"schema_version": 1, "objects": ["not-an-object"]}, "must be an object"),
            ({"schema_version": 1, "bucket_name": "different", "objects": []}, "must match"),
            ({"schema_version": 1, "prefix": "different", "objects": []}, "must match"),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": S3_MANIFEST_KEY,
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "must not include bundle metadata",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_MANIFEST_KEY}.releases/{TEST_SHA256}.json",
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "must not include bundle metadata",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/duplicate.py",
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        },
                        {
                            "key": f"{S3_BUCKET_PREFIX}/duplicate.py",
                            "version_id": "v2",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        },
                    ],
                },
                "duplicate path",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": "outside/dag.py",
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "outside configured prefix",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/../escape.py",
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "safe relative path",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/a",
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        },
                        {
                            "key": f"{S3_BUCKET_PREFIX}/a/b.py",
                            "version_id": "v2",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        },
                    ],
                },
                "conflicts with file",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/dag.py",
                            "version_id": "null",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "invalid version_id",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": 123,
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "invalid key",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/dag.py",
                            "version_id": "v1",
                            "size": True,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "invalid size",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/dag.py",
                            "version_id": "v1",
                            "size": -1,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "invalid size",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/dag.py",
                            "version_id": "v1",
                            "size": 1,
                            "sha256": "not-a-sha256",
                        }
                    ],
                },
                "invalid sha256",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/.airflow-s3-generation.json",
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "reserved",
            ),
            (
                {
                    "schema_version": 1,
                    "objects": [
                        {
                            "key": f"{S3_BUCKET_PREFIX}/.airflow-s3-generation.json/child.py",
                            "version_id": "v1",
                            "size": 1,
                            "sha256": TEST_SHA256,
                        }
                    ],
                },
                "reserved",
            ),
        ],
    )
    def test_invalid_manifest_is_rejected(self, manifest, error):
        manifest.setdefault("bucket_name", S3_BUCKET_NAME)
        manifest.setdefault("prefix", S3_BUCKET_PREFIX)
        with pytest.raises(S3DagBundleManifestError, match=error):
            self._bundle()._parse_manifest(manifest)

    def test_pointer_to_missing_release_is_rejected(self, s3_client):
        s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_MANIFEST_KEY,
            Body=json.dumps({"schema_version": 1, "bundle_version": TEST_SHA256}).encode(),
        )

        with pytest.raises(S3DagBundleManifestError, match="Could not read.*release"):
            self._bundle().initialize()

    def test_corrupted_completion_marker_is_not_replaced(self, versioned_s3_bucket):
        publish_manifest(boto3.client("s3"), {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        bundle = self._bundle()
        bundle.initialize()
        current = bundle.get_current_version()
        marker = bundle.path / ".airflow-s3-generation.json"
        marker.chmod(0o644)
        marker.write_text("corrupt")
        pinned_bundle = self._bundle(version=current.version, version_data=None)

        with pytest.raises(S3DagBundleIntegrityError, match="incomplete or corrupted"):
            pinned_bundle.initialize()

        assert marker.read_text() == "corrupt"

    def test_boolean_completion_marker_schema_is_rejected(self, versioned_s3_bucket):
        publish_manifest(boto3.client("s3"), {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        bundle = self._bundle()
        bundle.initialize()
        current = bundle.get_current_version()
        marker = bundle.path / ".airflow-s3-generation.json"
        marker.chmod(0o644)
        marker.write_text(json.dumps({"schema_version": True, "bundle_version": current.version}))

        with pytest.raises(S3DagBundleIntegrityError, match="incomplete or corrupted"):
            self._bundle(version=current.version, version_data=None).initialize()

    def test_downloaded_object_checksum_must_match_manifest(self, versioned_s3_bucket):
        s3_client = boto3.client("s3")
        manifest, _ = publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        manifest["objects"][0]["sha256"] = TEST_SHA256
        publish_release(s3_client, manifest)
        bundle = self._bundle()

        with pytest.raises(S3DagBundleIntegrityError, match="SHA-256"):
            bundle.initialize()

        assert not list(bundle.versions_dir.glob(".s3-staging-*"))

    def test_downloaded_object_size_must_match_manifest(self, versioned_s3_bucket):
        s3_client = boto3.client("s3")
        manifest, _ = publish_manifest(s3_client, {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        manifest["objects"][0]["size"] += 1
        version = publish_release(s3_client, manifest)
        bundle = self._bundle()

        with pytest.raises(S3DagBundleIntegrityError, match="has size"):
            bundle.initialize()

        assert not (bundle.versions_dir / version).exists()
        assert not list(bundle.versions_dir.glob(".s3-staging-*"))

    @pytest.mark.parametrize("version_data", [None, {"arbitrary": "value"}, {"manifest_version_id": "old"}])
    def test_pinned_run_does_not_depend_on_version_data(self, versioned_s3_bucket, version_data):
        publish_manifest(boto3.client("s3"), {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        current_bundle = self._bundle(name="current")
        current_bundle.initialize()
        current = current_bundle.get_current_version()

        pinned_bundle = S3DagBundle(
            name="pinned",
            aws_conn_id=AWS_CONN_ID_DEFAULT,
            bucket_name=S3_BUCKET_NAME,
            prefix=S3_BUCKET_PREFIX,
            manifest_key=S3_MANIFEST_KEY,
            version=current.version,
            version_data=version_data,
        )
        pinned_bundle.initialize()

        assert pinned_bundle.bucket_name == S3_BUCKET_NAME
        assert pinned_bundle.prefix == S3_BUCKET_PREFIX
        assert pinned_bundle.manifest_key == S3_MANIFEST_KEY
        assert (pinned_bundle.path / "dag.py").read_bytes() == b"VERSION = 1\n"

    @pytest.mark.parametrize("version", ["../escape", "abc", "A" * 64, 123])
    def test_pinned_version_must_be_lowercase_sha256(self, version):
        with pytest.raises(S3DagBundleConfigError, match="lowercase SHA-256"):
            self._bundle(version=version)

    @pytest.mark.parametrize(
        "body",
        [
            b"not-json",
            json.dumps({"schema_version": 2, "bundle_version": TEST_SHA256}).encode(),
            json.dumps({"schema_version": True, "bundle_version": TEST_SHA256}).encode(),
            json.dumps({"schema_version": 1, "bundle_version": "short"}).encode(),
            json.dumps({"schema_version": 1, "bundle_version": TEST_SHA256, "unexpected": True}).encode(),
        ],
    )
    def test_invalid_current_pointer_is_rejected(self, versioned_s3_bucket, body):
        boto3.client("s3").put_object(Bucket=S3_BUCKET_NAME, Key=S3_MANIFEST_KEY, Body=body)

        with pytest.raises(S3DagBundleManifestError):
            self._bundle().initialize()

    def test_release_root_symlink_is_rejected(self, versioned_s3_bucket, tmp_path):
        _, version = publish_manifest(boto3.client("s3"), {f"{S3_BUCKET_PREFIX}/dag.py": b"VERSION = 1\n"})
        bundle = self._bundle(version=version)
        bundle.versions_dir.mkdir(parents=True)
        external = tmp_path / "external-generation"
        external.mkdir()
        (bundle.versions_dir / version).symlink_to(external, target_is_directory=True)

        with pytest.raises(S3DagBundleIntegrityError, match="incomplete or corrupted"):
            bundle.initialize()
