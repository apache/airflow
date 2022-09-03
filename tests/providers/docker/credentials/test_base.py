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

from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.docker.credentials.base import BaseDockerCredentialHelper, DockerLoginCredentials


class TestDockerLoginCredentials:
    @mock.patch("airflow.providers.docker.credentials.base.mask_secret")
    def test_mask_password(self, mock_mask_secret):
        DockerLoginCredentials(username="username", password="test-password", registry="registry")
        mock_mask_secret.assert_called_once_with("test-password")


class TestBaseDockerCredentialHelper:
    @pytest.mark.parametrize("true_reauth_val", ['y', 'yes', 't', 'true', 'on', '1', 1, True])
    def test_reauth_from_connection(self, true_reauth_val):
        """Test get and parse `reauth=True` value from connection."""
        test_conn = Connection(extra={"reauth": true_reauth_val})
        ch = BaseDockerCredentialHelper(conn=test_conn)
        assert ch.reauth is True

    @pytest.mark.parametrize("false_reauth_val", ['n', 'no', 'f', 'false', 'off', '0', 0, False])
    def test_no_reauth_from_connection(self, false_reauth_val):
        """Test get and parse `reauth=False` value from connection."""
        test_conn = Connection(extra={"reauth": false_reauth_val})
        ch = BaseDockerCredentialHelper(conn=test_conn)
        assert ch.reauth is False

    @pytest.mark.parametrize("wrong_reauth_val", [42, None, "null"])
    def test_wrong_reauth_value_from_connection(self, wrong_reauth_val):
        """Test get and raise error if reauth value not a boolean or boolean-like string value."""
        test_conn = Connection(extra={"reauth": wrong_reauth_val})
        ch = BaseDockerCredentialHelper(conn=test_conn)
        error_message = r"'.*' is not a boolean-like string value\."
        with pytest.raises(ValueError, match=error_message):
            ch.reauth

    @pytest.mark.parametrize("email", ["John.Doe@apache.org", None])
    def test_email_from_connection(self, email):
        test_conn = Connection(extra={"email": email} if email else {})
        ch = BaseDockerCredentialHelper(conn=test_conn)
        assert ch.email == email
