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
import re
from typing import Dict, Sequence, Tuple
from unittest import TestCase, mock

import pytest
from google.api_core.retry import Retry

from airflow import AirflowException
from airflow.providers.google.cloud.hooks.os_login import OSLoginHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_DELEGATE_TO: str = "test-delegate-to"
TEST_PROJECT_ID: str = "test-project-id"
TEST_PROJECT_ID_2: str = "test-project-id-2"

TEST_USER: str = "test-user"
TEST_CREDENTIALS = mock.MagicMock()
TEST_BODY: Dict = mock.MagicMock()
TEST_RETRY: Retry = mock.MagicMock()
TEST_TIMEOUT: float = 4
TEST_METADATA: Sequence[Tuple[str, str]] = ()
TEST_PARENT: str = "users/test-user"


class TestOSLoginHook(TestCase):
    def setUp(
        self,
    ) -> None:
        with mock.patch(
            "airflow.providers.google.cloud.hooks.os_login.OSLoginHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = OSLoginHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.os_login.OSLoginHook.get_conn")
    def test_import_ssh_public_key(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.import_ssh_public_key(
            user=TEST_USER,
            ssh_public_key=TEST_BODY,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.import_ssh_public_key.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT,
                ssh_public_key=TEST_BODY,
                project_id=TEST_PROJECT_ID,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestOSLoginHookWithDefaultProjectIdHook(TestCase):
    def setUp(
        self,
    ) -> None:
        with mock.patch(
            "airflow.providers.google.cloud.hooks.os_login.OSLoginHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = OSLoginHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_2),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.os_login.OSLoginHook.get_conn")
    def test_import_ssh_public_key(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.import_ssh_public_key(
            user=TEST_USER,
            ssh_public_key=TEST_BODY,
            project_id=None,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.import_ssh_public_key.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT,
                ssh_public_key=TEST_BODY,
                project_id=TEST_PROJECT_ID_2,
            ),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestOSLoginHookWithoutDefaultProjectIdHook(TestCase):
    def setUp(
        self,
    ) -> None:
        with mock.patch(
            "airflow.providers.google.cloud.hooks.os_login.OSLoginHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = OSLoginHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, TEST_PROJECT_ID_2),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.os_login.OSLoginHook.get_conn")
    def test_import_ssh_public_key(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        self.hook.import_ssh_public_key(
            user=TEST_USER,
            ssh_public_key=TEST_BODY,
            project_id=TEST_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.return_value.import_ssh_public_key.assert_called_once_with(
            request=dict(parent=TEST_PARENT, ssh_public_key=TEST_BODY, project_id=TEST_PROJECT_ID),
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


TEST_MESSAGE = re.escape(
    "The project id must be passed either as keyword project_id parameter or as project_id extra in "
    "Google Cloud connection definition. Both are not set!"
)


class TestOSLoginHookMissingProjectIdHook(TestCase):
    def setUp(
        self,
    ) -> None:
        with mock.patch(
            "airflow.providers.google.cloud.hooks.os_login.OSLoginHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = OSLoginHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id",
        return_value=(TEST_CREDENTIALS, None),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.os_login.OSLoginHook.get_conn")
    def test_import_ssh_public_key(self, mock_get_conn, mock_get_creds_and_project_id) -> None:
        with pytest.raises(AirflowException, match=TEST_MESSAGE):
            self.hook.import_ssh_public_key(
                user=TEST_USER,
                ssh_public_key=TEST_BODY,
                project_id=None,
                retry=TEST_RETRY,
                timeout=TEST_TIMEOUT,
                metadata=TEST_METADATA,
            )
