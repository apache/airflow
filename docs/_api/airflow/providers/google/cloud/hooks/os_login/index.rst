:mod:`airflow.providers.google.cloud.hooks.os_login`
====================================================

.. py:module:: airflow.providers.google.cloud.hooks.os_login


Module Contents
---------------

.. py:class:: OSLoginHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google OS login APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: get_conn(self)

      Return OS Login service client



   
   .. method:: import_ssh_public_key(self, user: str, ssh_public_key: Dict, project_id: str, retry=None, timeout=None, metadata=None)

      Adds an SSH public key and returns the profile information. Default POSIX
      account information is set when no username and UID exist as part of the
      login profile.

      :param user: The unique ID for the user
      :type user: str
      :param ssh_public_key: The SSH public key and expiration time.
      :type ssh_public_key: dict
      :param project_id: The project ID of the Google Cloud project.
      :type project_id: str
      :param retry: A retry object used to retry requests. If ``None`` is specified, requests will
          be retried using a default configuration.
      :type retry: Optional[google.api_core.retry.Retry]
      :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that
          if ``retry`` is specified, the timeout applies to each individual attempt.
      :type timeout: Optional[float]
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: Optional[Sequence[Tuple[str, str]]]
      :return:  A :class:`~google.cloud.oslogin_v1.types.ImportSshPublicKeyResponse` instance.




