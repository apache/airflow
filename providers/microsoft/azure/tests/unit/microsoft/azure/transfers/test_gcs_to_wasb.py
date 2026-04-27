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

import os
import sys
from types import ModuleType
from unittest import mock

import pytest

from airflow.providers.microsoft.azure.transfers.gcs_to_wasb import GCSToAzureBlobStorageOperator

TASK_ID = "test-gcs-to-azure-blob"
GCS_BUCKET = "gcs-bucket"
CONTAINER = "container"
BLOB_PREFIX = "dest/prefix"
GCS_OBJECTS = ["data/a.txt", "data/b.txt"]


def _expected_blob_paths(blob_prefix: str, *relative_keys: str) -> list[str]:
    """Match operator dest_blob construction (os.path.join) with forward slashes for asserts."""
    return [os.path.join(blob_prefix, key).replace("\\", "/") for key in relative_keys]


def _norm_paths(paths: list[str]) -> list[str]:
    return [p.replace("\\", "/") for p in paths]


class TestGCSToAzureBlobStorageOperator:
    def test_init_defaults(self):
        op = GCSToAzureBlobStorageOperator(
            task_id=TASK_ID,
            gcs_bucket=GCS_BUCKET,
            container_name=CONTAINER,
            blob_prefix=BLOB_PREFIX,
        )
        assert op.gcp_conn_id == "google_cloud_default"
        assert op.wasb_conn_id == "wasb_default"
        assert op.replace is False
        assert op.keep_directory_structure is True
        assert op.flatten_structure is False
        assert op.create_container is False

    @mock.patch("airflow.providers.google.__version__", "10.2.0")
    def test_match_glob_requires_recent_google_provider(self):
        with pytest.raises(ValueError, match="match_glob"):
            GCSToAzureBlobStorageOperator(
                task_id=TASK_ID,
                gcs_bucket=GCS_BUCKET,
                container_name=CONTAINER,
                match_glob="**/*.csv",
            )

    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.WasbHook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.GCSHook")
    def test_execute_replace_uploads_all(self, mock_gcs_hook, mock_wasb_hook):
        mock_gcs_hook.return_value.list.return_value = list(GCS_OBJECTS)
        mock_file = mock.Mock()
        mock_file.name = "/tmp/local"
        mock_gcs_hook.return_value.provide_file.return_value.__enter__ = mock.Mock(return_value=mock_file)
        mock_gcs_hook.return_value.provide_file.return_value.__exit__ = mock.Mock(return_value=None)

        op = GCSToAzureBlobStorageOperator(
            task_id=TASK_ID,
            gcs_bucket=GCS_BUCKET,
            prefix="data/",
            container_name=CONTAINER,
            blob_prefix=BLOB_PREFIX,
            replace=True,
        )
        result = op.execute(context=None)

        assert _norm_paths(result) == _expected_blob_paths(BLOB_PREFIX, "data/a.txt", "data/b.txt")
        mock_wasb_hook.return_value.load_file.assert_called()
        assert mock_wasb_hook.return_value.load_file.call_count == 2
        first_kw = mock_wasb_hook.return_value.load_file.call_args_list[0].kwargs
        assert first_kw["container_name"] == CONTAINER
        assert first_kw["blob_name"].startswith(BLOB_PREFIX.replace("\\", "/"))
        assert first_kw["overwrite"] is True

    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.WasbHook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.GCSHook")
    def test_execute_returns_empty_when_no_objects(self, mock_gcs_hook, mock_wasb_hook):
        mock_gcs_hook.return_value.list.return_value = []

        op = GCSToAzureBlobStorageOperator(
            task_id=TASK_ID,
            gcs_bucket=GCS_BUCKET,
            container_name=CONTAINER,
            blob_prefix=BLOB_PREFIX,
            replace=True,
        )
        assert op.execute(context=None) == []
        mock_wasb_hook.return_value.load_file.assert_not_called()

    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.WasbHook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.GCSHook")
    def test_execute_skips_gcs_directory_placeholder_keys(self, mock_gcs_hook, mock_wasb_hook):
        mock_gcs_hook.return_value.list.return_value = ["src/airflow.png", "src/"]
        mock_file = mock.Mock()
        mock_file.name = "/tmp/local"
        mock_gcs_hook.return_value.provide_file.return_value.__enter__ = mock.Mock(return_value=mock_file)
        mock_gcs_hook.return_value.provide_file.return_value.__exit__ = mock.Mock(return_value=None)

        op = GCSToAzureBlobStorageOperator(
            task_id=TASK_ID,
            gcs_bucket=GCS_BUCKET,
            container_name=CONTAINER,
            blob_prefix="",
            replace=True,
        )
        result = op.execute(context=None)

        assert _norm_paths(result) == ["src/airflow.png"]
        mock_wasb_hook.return_value.load_file.assert_called_once()
        assert mock_wasb_hook.return_value.load_file.call_args.kwargs["blob_name"] == "src/airflow.png"
        assert mock_wasb_hook.return_value.load_file.call_args.kwargs["overwrite"] is True

    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.WasbHook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.GCSHook")
    def test_execute_skip_existing_when_replace_false(self, mock_gcs_hook, mock_wasb_hook):
        mock_gcs_hook.return_value.list.return_value = ["data/a.txt", "data/b.txt"]
        mock_wasb_hook.return_value.get_blobs_list_recursive.return_value = [
            f"{BLOB_PREFIX}/data/a.txt",
        ]
        mock_file = mock.Mock()
        mock_file.name = "/tmp/local"
        mock_gcs_hook.return_value.provide_file.return_value.__enter__ = mock.Mock(return_value=mock_file)
        mock_gcs_hook.return_value.provide_file.return_value.__exit__ = mock.Mock(return_value=None)

        op = GCSToAzureBlobStorageOperator(
            task_id=TASK_ID,
            gcs_bucket=GCS_BUCKET,
            container_name=CONTAINER,
            blob_prefix=BLOB_PREFIX,
            replace=False,
        )
        result = op.execute(context=None)

        assert _norm_paths(result) == _expected_blob_paths(BLOB_PREFIX, "data/b.txt")
        mock_wasb_hook.return_value.load_file.assert_called_once()
        assert mock_wasb_hook.return_value.load_file.call_args.kwargs["overwrite"] is False

    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.WasbHook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.GCSHook")
    def test_flatten_structure(self, mock_gcs_hook, mock_wasb_hook):
        mock_gcs_hook.return_value.list.return_value = ["data/a.txt"]
        mock_file = mock.Mock()
        mock_file.name = "/tmp/local"
        mock_gcs_hook.return_value.provide_file.return_value.__enter__ = mock.Mock(return_value=mock_file)
        mock_gcs_hook.return_value.provide_file.return_value.__exit__ = mock.Mock(return_value=None)

        op = GCSToAzureBlobStorageOperator(
            task_id=TASK_ID,
            gcs_bucket=GCS_BUCKET,
            container_name=CONTAINER,
            blob_prefix=BLOB_PREFIX,
            replace=True,
            flatten_structure=True,
        )
        result = op.execute(context=None)

        assert _norm_paths(result) == _expected_blob_paths(BLOB_PREFIX, "a.txt")
        kw = mock_wasb_hook.return_value.load_file.call_args.kwargs
        assert str(kw["blob_name"]).replace("\\", "/") == _expected_blob_paths(BLOB_PREFIX, "a.txt")[0]
        assert kw["overwrite"] is True

    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.WasbHook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.GCSHook")
    def test_execute_is_idempotent_on_retry(self, mock_gcs_hook, mock_wasb_hook):
        mock_gcs_hook.return_value.list.return_value = ["data/a.txt"]
        mock_file = mock.Mock()
        mock_file.name = "/tmp/local"
        mock_gcs_hook.return_value.provide_file.return_value.__enter__ = mock.Mock(return_value=mock_file)
        mock_gcs_hook.return_value.provide_file.return_value.__exit__ = mock.Mock(return_value=None)

        op = GCSToAzureBlobStorageOperator(
            task_id=TASK_ID,
            gcs_bucket=GCS_BUCKET,
            prefix="data/",
            container_name=CONTAINER,
            blob_prefix=BLOB_PREFIX,
            replace=True,
            keep_directory_structure=False,
        )

        first = op.execute(context=None)
        second = op.execute(context=None)

        assert _norm_paths(first) == _norm_paths(second)
        assert op.blob_prefix == BLOB_PREFIX

    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.WasbHook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.gcs_to_wasb.GCSHook")
    def test_openlineage_facets(self, mock_gcs_hook, mock_wasb_hook):
        injected: list[str] = []
        if "airflow.providers.openlineage.extractors" not in sys.modules:
            ol_pkg = ModuleType("airflow.providers.openlineage")
            ol_ext = ModuleType("airflow.providers.openlineage.extractors")

            class _OperatorLineage:
                __slots__ = ("inputs", "outputs")

                def __init__(self, *, inputs=None, outputs=None):
                    self.inputs = inputs
                    self.outputs = outputs

            ol_ext.OperatorLineage = _OperatorLineage
            sys.modules["airflow.providers.openlineage"] = ol_pkg
            sys.modules["airflow.providers.openlineage.extractors"] = ol_ext
            injected = [
                "airflow.providers.openlineage.extractors",
                "airflow.providers.openlineage",
            ]

        try:
            mock_wasb_hook.return_value.get_conn.return_value.account_name = "mystorage"
            op = GCSToAzureBlobStorageOperator(
                task_id=TASK_ID,
                gcs_bucket=GCS_BUCKET,
                prefix="p/",
                container_name=CONTAINER,
                blob_prefix=BLOB_PREFIX,
            )
            lineage = op.get_openlineage_facets_on_start()
            assert len(lineage.inputs) == 1
            assert lineage.inputs[0].namespace == "gs://gcs-bucket"
            assert "wasbs://" in lineage.outputs[0].namespace
        finally:
            for name in injected:
                sys.modules.pop(name, None)
