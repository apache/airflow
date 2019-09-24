# -*- coding: utf-8 -*-
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
from base64 import b64encode, b64decode
from collections import namedtuple

from airflow.gcp.hooks.kms import GoogleCloudKMSHook
from tests.compat import mock


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
TEST_KEY_ID = "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}".format(
    TEST_PROJECT, TEST_LOCATION, TEST_KEY_RING, TEST_KEY
)

RESPONSE = Response(PLAINTEXT, PLAINTEXT)


def mock_init(self, gcp_conn_id, delegate_to=None):  # pylint: disable=unused-argument
    pass


class TestGoogleCloudKMSHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_init,
        ):
            self.kms_hook = GoogleCloudKMSHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.gcp.hooks.kms.GoogleCloudKMSHook.client_info",
        new_callable=mock.PropertyMock,
    )
    @mock.patch("airflow.gcp.hooks.kms.GoogleCloudKMSHook._get_credentials")
    @mock.patch("airflow.gcp.hooks.kms.KeyManagementServiceClient")
    def test_kms_client_creation(self, mock_client, mock_get_creds, mock_client_info):
        result = self.kms_hook.get_conn()
        mock_client.assert_called_once_with(
            credentials=mock_get_creds.return_value,
            client_info=mock_client_info.return_value,
        )
        self.assertEqual(mock_client.return_value, result)
        self.assertEqual(self.kms_hook._conn, result)

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.kms.GoogleCloudKMSHook.get_conn",
        **{"return_value.encrypt.return_value": RESPONSE}
    )
    def test_encrypt(self, mock_get_conn):
        result = self.kms_hook.encrypt(TEST_KEY_ID, PLAINTEXT)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.encrypt.assert_called_once_with(
            name=TEST_KEY_ID,
            plaintext=PLAINTEXT,
            additional_authenticated_data=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual(PLAINTEXT_b64, result)

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.kms.GoogleCloudKMSHook.get_conn",
        **{"return_value.encrypt.return_value": RESPONSE}
    )
    def test_encrypt_with_auth_data(self, mock_get_conn):
        result = self.kms_hook.encrypt(TEST_KEY_ID, PLAINTEXT, AUTH_DATA)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.encrypt.assert_called_once_with(
            name=TEST_KEY_ID,
            plaintext=PLAINTEXT,
            additional_authenticated_data=AUTH_DATA,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual(PLAINTEXT_b64, result)

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.kms.GoogleCloudKMSHook.get_conn",
        **{"return_value.decrypt.return_value": RESPONSE}
    )
    def test_decrypt(self, mock_get_conn):
        result = self.kms_hook.decrypt(TEST_KEY_ID, CIPHERTEXT_b64)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.decrypt.assert_called_once_with(
            name=TEST_KEY_ID,
            ciphertext=CIPHERTEXT,
            additional_authenticated_data=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual(PLAINTEXT, result)

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.kms.GoogleCloudKMSHook.get_conn",
        **{"return_value.decrypt.return_value": RESPONSE}
    )
    def test_decrypt_with_auth_data(self, mock_get_conn):
        result = self.kms_hook.decrypt(TEST_KEY_ID, CIPHERTEXT_b64, AUTH_DATA)
        mock_get_conn.assert_called_once_with()
        mock_get_conn.return_value.decrypt.assert_called_once_with(
            name=TEST_KEY_ID,
            ciphertext=CIPHERTEXT,
            additional_authenticated_data=AUTH_DATA,
            retry=None,
            timeout=None,
            metadata=None,
        )
        self.assertEqual(PLAINTEXT, result)
