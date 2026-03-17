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

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("gcsfs")

TEST_CONN = "google_cloud_test_conn"


@pytest.fixture(scope="module", autouse=True)
def _setup_connections():
    with pytest.MonkeyPatch.context() as mp_ctx:
        mp_ctx.setenv(f"AIRFLOW_CONN_{TEST_CONN}".upper(), "google-cloud-platform://")
        yield


class TestGCSFilesystem:
    @patch("airflow.providers.google.cloud.fs.gcs.GoogleBaseHook")
    @patch("gcsfs.GCSFileSystem")
    def test_get_fs_passes_credentials_object(self, mock_gcsfs, mock_hook):
        """Test that get_fs passes a Credentials object to GCSFileSystem."""
        from airflow.providers.google.cloud.fs.gcs import get_fs

        mock_credentials = MagicMock()
        mock_hook_instance = MagicMock()
        mock_hook_instance.get_credentials.return_value = mock_credentials
        mock_hook_instance.project_id = "test-project"
        mock_hook_instance.extras = {}
        mock_hook.return_value = mock_hook_instance

        get_fs(conn_id=TEST_CONN)

        mock_hook_instance.get_credentials.assert_called_once()
        call_kwargs = mock_gcsfs.call_args.kwargs
        assert call_kwargs["token"] is mock_credentials

    @patch("gcsfs.GCSFileSystem")
    def test_get_fs_no_conn_id(self, mock_gcsfs):
        """Test that get_fs works without conn_id."""
        from airflow.providers.google.cloud.fs.gcs import get_fs

        get_fs(conn_id=None)

        mock_gcsfs.assert_called_once_with()

    @patch("airflow.providers.google.cloud.fs.gcs.GoogleBaseHook")
    @patch("gcsfs.GCSFileSystem")
    def test_get_fs_with_anonymous_credentials(self, mock_gcsfs, mock_hook):
        """Test that get_fs works with anonymous credentials."""
        from google.auth.credentials import AnonymousCredentials

        from airflow.providers.google.cloud.fs.gcs import get_fs

        anonymous_creds = AnonymousCredentials()
        mock_hook_instance = MagicMock()
        mock_hook_instance.get_credentials.return_value = anonymous_creds
        mock_hook_instance.project_id = None
        mock_hook_instance.extras = {}
        mock_hook.return_value = mock_hook_instance

        get_fs(conn_id=TEST_CONN)

        call_kwargs = mock_gcsfs.call_args.kwargs
        assert isinstance(call_kwargs["token"], AnonymousCredentials)
