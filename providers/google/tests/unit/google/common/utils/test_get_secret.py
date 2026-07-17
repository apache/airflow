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
from google.cloud.exceptions import NotFound

from airflow.providers.google.common.utils.get_secret import get_secret


class TestGetSecret:
    @mock.patch("airflow.providers.google.common.utils.get_secret.GoogleCloudSecretManagerHook")
    def test_get_secret_success(self, mock_hook_class):
        mock_hook_instance = mock.Mock()
        mock_hook_class.return_value = mock_hook_instance

        mock_hook_instance.secret_exists.return_value = True

        mock_payload = mock.Mock()
        mock_payload.data.decode.return_value = "test_secret_value"
        mock_access_secret_return = mock.Mock()
        mock_access_secret_return.payload = mock_payload
        mock_hook_instance.access_secret.return_value = mock_access_secret_return

        secret_id = "test-secret-id"
        expected_secret_value = "test_secret_value"

        result = get_secret(secret_id=secret_id)

        mock_hook_class.assert_called_once()
        mock_hook_instance.secret_exists.assert_called_once_with(secret_id=secret_id)
        mock_hook_instance.access_secret.assert_called_once_with(secret_id=secret_id)
        assert result == expected_secret_value

    @mock.patch("airflow.providers.google.common.utils.get_secret.GoogleCloudSecretManagerHook")
    def test_get_secret_not_found(self, mock_hook_class):
        mock_hook_instance = mock.Mock()
        mock_hook_class.return_value = mock_hook_instance

        mock_hook_instance.secret_exists.return_value = False

        secret_id = "non-existent-secret"

        with pytest.raises(NotFound):
            get_secret(secret_id=secret_id)

        mock_hook_class.assert_called_once()
        mock_hook_instance.secret_exists.assert_called_once_with(secret_id=secret_id)
        mock_hook_instance.access_secret.assert_not_called()
