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
from base64 import b64encode

from airflow.gcp.hooks.kms import GoogleCloudKMSHook
from tests.compat import mock

BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
KMS_STRING = 'airflow.gcp.hooks.kms.{}'


TEST_PROJECT = 'test-project'
TEST_LOCATION = 'global'
TEST_KEY_RING = 'test-key-ring'
TEST_KEY = 'test-key'
TEST_KEY_ID = 'projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}'.format(
    TEST_PROJECT, TEST_LOCATION, TEST_KEY_RING, TEST_KEY)


def mock_init(self, gcp_conn_id, delegate_to=None):  # pylint: disable=unused-argument
    pass


class GoogleCloudKMSHookTest(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.kms_hook = GoogleCloudKMSHook(gcp_conn_id='test')

    @mock.patch(KMS_STRING.format('GoogleCloudKMSHook.get_conn'))
    def test_encrypt(self, mock_service):
        plaintext = b'Test plaintext'
        ciphertext = 'Test ciphertext'
        plaintext_b64 = b64encode(plaintext).decode('ascii')
        body = {'plaintext': plaintext_b64}
        response = {'ciphertext': ciphertext}

        encrypt_method = (mock_service.return_value
                          .projects.return_value
                          .locations.return_value
                          .keyRings.return_value
                          .cryptoKeys.return_value
                          .encrypt)
        execute_method = encrypt_method.return_value.execute
        execute_method.return_value = response

        ret_val = self.kms_hook.encrypt(TEST_KEY_ID, plaintext)
        encrypt_method.assert_called_with(name=TEST_KEY_ID,
                                          body=body)
        execute_method.assert_called_with(num_retries=mock.ANY)
        self.assertEqual(ciphertext, ret_val)

    @mock.patch(KMS_STRING.format('GoogleCloudKMSHook.get_conn'))
    def test_encrypt_authdata(self, mock_service):
        plaintext = b'Test plaintext'
        auth_data = b'Test authdata'
        ciphertext = 'Test ciphertext'
        plaintext_b64 = b64encode(plaintext).decode('ascii')
        auth_data_b64 = b64encode(auth_data).decode('ascii')
        body = {
            'plaintext': plaintext_b64,
            'additionalAuthenticatedData': auth_data_b64
        }
        response = {'ciphertext': ciphertext}

        encrypt_method = (mock_service.return_value
                          .projects.return_value
                          .locations.return_value
                          .keyRings.return_value
                          .cryptoKeys.return_value
                          .encrypt)
        execute_method = encrypt_method.return_value.execute
        execute_method.return_value = response

        ret_val = self.kms_hook.encrypt(TEST_KEY_ID, plaintext,
                                        authenticated_data=auth_data)
        encrypt_method.assert_called_with(name=TEST_KEY_ID,
                                          body=body)
        execute_method.assert_called_with(num_retries=mock.ANY)
        self.assertEqual(ciphertext, ret_val)

    @mock.patch(KMS_STRING.format('GoogleCloudKMSHook.get_conn'))
    def test_decrypt(self, mock_service):
        plaintext = b'Test plaintext'
        ciphertext = 'Test ciphertext'
        plaintext_b64 = b64encode(plaintext).decode('ascii')
        body = {'ciphertext': ciphertext}
        response = {'plaintext': plaintext_b64}

        decrypt_method = (mock_service.return_value
                          .projects.return_value
                          .locations.return_value
                          .keyRings.return_value
                          .cryptoKeys.return_value
                          .decrypt)
        execute_method = decrypt_method.return_value.execute
        execute_method.return_value = response

        ret_val = self.kms_hook.decrypt(TEST_KEY_ID, ciphertext)
        decrypt_method.assert_called_with(name=TEST_KEY_ID,
                                          body=body)
        execute_method.assert_called_with(num_retries=mock.ANY)
        self.assertEqual(plaintext, ret_val)

    @mock.patch(KMS_STRING.format('GoogleCloudKMSHook.get_conn'))
    def test_decrypt_authdata(self, mock_service):
        plaintext = b'Test plaintext'
        auth_data = b'Test authdata'
        ciphertext = 'Test ciphertext'
        plaintext_b64 = b64encode(plaintext).decode('ascii')
        auth_data_b64 = b64encode(auth_data).decode('ascii')
        body = {
            'ciphertext': ciphertext,
            'additionalAuthenticatedData': auth_data_b64
        }
        response = {'plaintext': plaintext_b64}

        decrypt_method = (mock_service.return_value
                          .projects.return_value
                          .locations.return_value
                          .keyRings.return_value
                          .cryptoKeys.return_value
                          .decrypt)
        execute_method = decrypt_method.return_value.execute
        execute_method.return_value = response

        ret_val = self.kms_hook.decrypt(TEST_KEY_ID, ciphertext,
                                        authenticated_data=auth_data)
        decrypt_method.assert_called_with(name=TEST_KEY_ID,
                                          body=body)
        execute_method.assert_called_with(num_retries=mock.ANY)
        self.assertEqual(plaintext, ret_val)
