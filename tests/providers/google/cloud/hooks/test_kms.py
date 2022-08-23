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

import unittest
from base64 import b64decode, b64encode
from collections import namedtuple
from unittest import mock

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.kms import CloudKMSHook
from airflow.providers.google.common.consts import CLIENT_INFO

Response = namedtuple("Response", ["plaintext", "ciphertext"])

PLAINTEXT = b"Test plaintext"
PLAINTEXT_b64 = b64encode(PLAINTEXT).decode("ascii")

CIPHERTEXT_b64 = b64encode(b"Test ciphertext").decode("ascii")
CIPHERTEXT = b64decode(CIPHERTEXT_b64.encode("utf-8"))

AUTH_DATA = b"Test authdata"

TEST_PROJECT = "test-project"
TEST_LOCATION = "global"
TEST_KEY_RING = "test-key-ring"
TEST_KEY = "test-key"
TEST_KEY_ID = (
    f"projects/{TEST_PROJECT}/locations/{TEST_LOCATION}/keyRings/{TEST_KEY_RING}/cryptoKeys/{TEST_KEY}"
)

RESPONSE = Response(PLAINTEXT, PLAINTEXT)


def mock_init(
    self,
    gcp_conn_id,
    delegate_to=None,
    impersonation_chain=None,
):
    pass


class TestCloudKMSHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_init,
        ):
            self.kms_hook = CloudKMSHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.kms.CloudKMSHook.get_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.kms.KeyManagementServiceClient")
    def test_kms_client_creation(self, mock_client, mock_get_creds):
        result = self.kms_hook.get_conn()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value, client_info=CLIENT_INFO)
        assert mock_client.return_value == result
        assert self.kms_hook._conn == result

    @mock.patch("airflow.providers.google.cloud.hooks.kms.CloudKMSHook.get_conn")
    def test_encrypt(self, mock_get_conn):
        mock_get_conn.return_value.encrypt.return_value = RESPONSE
        result = self.kms_hook.encrypt(TEST_KEY_ID, PLAINTEXT)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.encrypt.assert_called_once_with(
            request=dict(
                name=TEST_KEY_ID,
                plaintext=PLAINTEXT,
                additional_authenticated_data=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        assert PLAINTEXT_b64 == result

    @mock.patch("airflow.providers.google.cloud.hooks.kms.CloudKMSHook.get_conn")
    def test_encrypt_with_auth_data(self, mock_get_conn):
        mock_get_conn.return_value.encrypt.return_value = RESPONSE
        result = self.kms_hook.encrypt(TEST_KEY_ID, PLAINTEXT, AUTH_DATA)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.encrypt.assert_called_once_with(
            request=dict(
                name=TEST_KEY_ID,
                plaintext=PLAINTEXT,
                additional_authenticated_data=AUTH_DATA,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        assert PLAINTEXT_b64 == result

    @mock.patch("airflow.providers.google.cloud.hooks.kms.CloudKMSHook.get_conn")
    def test_decrypt(self, mock_get_conn):
        mock_get_conn.return_value.decrypt.return_value = RESPONSE
        result = self.kms_hook.decrypt(TEST_KEY_ID, CIPHERTEXT_b64)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.decrypt.assert_called_once_with(
            request=dict(
                name=TEST_KEY_ID,
                ciphertext=CIPHERTEXT,
                additional_authenticated_data=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        assert PLAINTEXT == result

    @mock.patch("airflow.providers.google.cloud.hooks.kms.CloudKMSHook.get_conn")
    def test_decrypt_with_auth_data(self, mock_get_conn):
        mock_get_conn.return_value.decrypt.return_value = RESPONSE
        result = self.kms_hook.decrypt(TEST_KEY_ID, CIPHERTEXT_b64, AUTH_DATA)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.decrypt.assert_called_once_with(
            request=dict(
                name=TEST_KEY_ID,
                ciphertext=CIPHERTEXT,
                additional_authenticated_data=AUTH_DATA,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        assert PLAINTEXT == result
