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
"""
This module contains a Google Cloud KMS hook.
"""


import base64
from typing import Optional, Sequence, Tuple

from google.api_core.retry import Retry
from google.cloud.kms_v1 import KeyManagementServiceClient

from airflow.gcp.hooks.base import GoogleCloudBaseHook


def _b64encode(s: bytes) -> str:
    """ Base 64 encodes a bytes object to a string """
    return base64.b64encode(s).decode("ascii")


def _b64decode(s: str) -> bytes:
    """ Base 64 decodes a string to bytes. """
    return base64.b64decode(s.encode("utf-8"))


# noinspection PyAbstractClass
class GoogleCloudKMSHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Key Management service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, delegate_to=delegate_to)
        self._conn = None  # type: Optional[KeyManagementServiceClient]

    def get_conn(self) -> KeyManagementServiceClient:
        """
        Retrieves connection to Cloud Key Management service.

        :return: Cloud Key Management service object
        :rtype: google.cloud.kms_v1.KeyManagementServiceClient
        """
        if not self._conn:
            self._conn = KeyManagementServiceClient(
                credentials=self._get_credentials(),
                client_info=self.client_info
            )
        return self._conn

    def encrypt(
        self,
        key_name: str,
        plaintext: bytes,
        authenticated_data: Optional[bytes] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> str:
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
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :rtype: str
        """
        response = self.get_conn().encrypt(
            name=key_name,
            plaintext=plaintext,
            additional_authenticated_data=authenticated_data,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        ciphertext = _b64encode(response.ciphertext)
        return ciphertext

    def decrypt(
        self,
        key_name: str,
        ciphertext: str,
        authenticated_data: Optional[bytes] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> bytes:
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
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: sequence[tuple[str, str]]]
        :return: The original message.
        :rtype: bytes
        """
        response = self.get_conn().decrypt(
            name=key_name,
            ciphertext=_b64decode(ciphertext),
            additional_authenticated_data=authenticated_data,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        plaintext = response.plaintext
        return plaintext
