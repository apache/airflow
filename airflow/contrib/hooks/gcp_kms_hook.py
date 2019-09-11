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
#

import base64

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

from googleapiclient.discovery import build


def _b64encode(s):
    """ Base 64 encodes a bytes object to a string """
    return base64.b64encode(s).decode('ascii')


def _b64decode(s):
    """ Base 64 decodes a string to bytes. """
    return base64.b64decode(s.encode('utf-8'))


class GoogleCloudKMSHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud KMS. This hook uses the Google Cloud Platform
    connection.
    """

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        super(GoogleCloudKMSHook, self).__init__(gcp_conn_id, delegate_to=delegate_to)

    def get_conn(self):
        """
        Returns a KMS service object.

        :rtype: googleapiclient.discovery.Resource
        """
        http_authorized = self._authorize()
        return build(
            'cloudkms', 'v1', http=http_authorized, cache_discovery=False)

    def encrypt(self, key_name, plaintext, authenticated_data=None):
        """
        Encrypts a plaintext message using Google Cloud KMS.

        :param key_name: The Resource Name for the key (or key version)
                         to be used for encyption. Of the form
                         ``projects/*/locations/*/keyRings/*/cryptoKeys/**``
        :type key_name: str
        :param plaintext: The message to be encrypted.
        :type plaintext: bytes
        :param authenticated_data: Optional additional authenticated data that
                                   must also be provided to decrypt the message.
        :type authenticated_data: bytes
        :return: The base 64 encoded ciphertext of the original message.
        :rtype: str
        """
        keys = self.get_conn().projects().locations().keyRings().cryptoKeys()
        body = {'plaintext': _b64encode(plaintext)}
        if authenticated_data:
            body['additionalAuthenticatedData'] = _b64encode(authenticated_data)

        request = keys.encrypt(name=key_name, body=body)
        response = request.execute(num_retries=self.num_retries)

        ciphertext = response['ciphertext']
        return ciphertext

    def decrypt(self, key_name, ciphertext, authenticated_data=None):
        """
        Decrypts a ciphertext message using Google Cloud KMS.

        :param key_name: The Resource Name for the key to be used for decyption.
                         Of the form ``projects/*/locations/*/keyRings/*/cryptoKeys/**``
        :type key_name: str
        :param ciphertext: The message to be decrypted.
        :type ciphertext: str
        :param authenticated_data: Any additional authenticated data that was
                                   provided when encrypting the message.
        :type authenticated_data: bytes
        :return: The original message.
        :rtype: bytes
        """
        keys = self.get_conn().projects().locations().keyRings().cryptoKeys()
        body = {'ciphertext': ciphertext}
        if authenticated_data:
            body['additionalAuthenticatedData'] = _b64encode(authenticated_data)

        request = keys.decrypt(name=key_name, body=body)
        response = request.execute(num_retries=self.num_retries)

        plaintext = _b64decode(response['plaintext'])
        return plaintext
