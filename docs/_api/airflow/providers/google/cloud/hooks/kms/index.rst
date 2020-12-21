:mod:`airflow.providers.google.cloud.hooks.kms`
===============================================

.. py:module:: airflow.providers.google.cloud.hooks.kms

.. autoapi-nested-parse::

   This module contains a Google Cloud KMS hook



Module Contents
---------------

.. function:: _b64encode(s: bytes) -> str
   Base 64 encodes a bytes object to a string


.. function:: _b64decode(s: str) -> bytes
   Base 64 decodes a string to bytes


.. py:class:: CloudKMSHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud Key Management service.

   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account.
   :type impersonation_chain: Union[str, Sequence[str]]

   
   .. method:: get_conn(self)

      Retrieves connection to Cloud Key Management service.

      :return: Cloud Key Management service object
      :rtype: google.cloud.kms_v1.KeyManagementServiceClient



   
   .. method:: encrypt(self, key_name: str, plaintext: bytes, authenticated_data: Optional[bytes] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Encrypts a plaintext message using Google Cloud KMS.

      :param key_name: The Resource Name for the key (or key version)
                       to be used for encryption. Of the form
                       ``projects/*/locations/*/keyRings/*/cryptoKeys/**``
      :type key_name: str
      :param plaintext: The message to be encrypted.
      :type plaintext: bytes
      :param authenticated_data: Optional additional authenticated data that
                                 must also be provided to decrypt the message.
      :type authenticated_data: bytes
      :param retry: A retry object used to retry requests. If None is specified, requests will not be
          retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
          retry is specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: sequence[tuple[str, str]]]
      :return: The base 64 encoded ciphertext of the original message.
      :rtype: str



   
   .. method:: decrypt(self, key_name: str, ciphertext: str, authenticated_data: Optional[bytes] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Decrypts a ciphertext message using Google Cloud KMS.

      :param key_name: The Resource Name for the key to be used for decryption.
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




