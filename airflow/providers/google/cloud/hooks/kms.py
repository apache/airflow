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
"""This module contains a Google Cloud KMS hook"""


import base64
from typing import Optional, Sequence, Tuple, Union

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.kms_v1 import KeyManagementServiceClient

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


def _b64encode(s: bytes) -> str:
    """Base 64 encodes a bytes object to a string"""
    return base64.b64encode(s).decode("ascii")


def _b64decode(s: str) -> bytes:
    """Base 64 decodes a string to bytes"""
    return base64.b64decode(s.encode("utf-8"))


class CloudKMSHook(GoogleBaseHook):
    """
    Hook for Google Cloud Key Management service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._conn = None  # type: Optional[KeyManagementServiceClient]

    def get_conn(self) -> KeyManagementServiceClient:
        """
        Retrieves connection to Cloud Key Management service.

        :return: Cloud Key Management service object
        :rtype: google.cloud.kms_v1.KeyManagementServiceClient
        """
        if not self._conn:
            self._conn = KeyManagementServiceClient(
                credentials=self._get_credentials(), client_info=CLIENT_INFO
            )
        return self._conn

    def encrypt(
        self,
        key_name: str,
        plaintext: bytes,
        authenticated_data: Optional[bytes] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> str:
        """
        Encrypts a plaintext message using Google Cloud KMS.

        :param key_name: The Resource Name for the key (or key version)
                         to be used for encryption. Of the form
                         ``projects/*/locations/*/keyRings/*/cryptoKeys/**``
        :param plaintext: The message to be encrypted.
        :param authenticated_data: Optional additional authenticated data that
                                   must also be provided to decrypt the message.
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: The base 64 encoded ciphertext of the original message.
        :rtype: str
        """
        response = self.get_conn().encrypt(
            request={
                'name': key_name,
                'plaintext': plaintext,
                'additional_authenticated_data': authenticated_data,
            },
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
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> bytes:
        """
        Decrypts a ciphertext message using Google Cloud KMS.

        :param key_name: The Resource Name for the key to be used for decryption.
                         Of the form ``projects/*/locations/*/keyRings/*/cryptoKeys/**``
        :param ciphertext: The message to be decrypted.
        :param authenticated_data: Any additional authenticated data that was
                                   provided when encrypting the message.
        :param retry: A retry object used to retry requests. If None is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            retry is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: The original message.
        :rtype: bytes
        """
        response = self.get_conn().decrypt(
            request={
                'name': key_name,
                'ciphertext': _b64decode(ciphertext),
                'additional_authenticated_data': authenticated_data,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return response.plaintext
