:mod:`airflow.providers.google.cloud.hooks.compute_ssh`
=======================================================

.. py:module:: airflow.providers.google.cloud.hooks.compute_ssh


Module Contents
---------------

.. py:class:: _GCloudAuthorizedSSHClient(google_hook, *args, **kwargs)

   Bases: :class:`paramiko.SSHClient`

   SSH Client that maintains the context for gcloud authorization during the connection

   
   .. method:: connect(self, *args, **kwargs)



   
   .. method:: close(self)



   
   .. method:: __exit__(self, type_, value, traceback)




.. py:class:: ComputeEngineSSHHook(gcp_conn_id: str = 'google_cloud_default', instance_name: Optional[str] = None, zone: Optional[str] = None, user: Optional[str] = 'root', project_id: Optional[str] = None, hostname: Optional[str] = None, use_internal_ip: bool = False, use_iap_tunnel: bool = False, use_oslogin: bool = True, expire_time: int = 300, delegate_to: Optional[str] = None)

   Bases: :class:`airflow.providers.ssh.hooks.ssh.SSHHook`

   Hook to connect to a remote instance in compute engine

   :param instance_name: The name of the Compute Engine instance
   :type instance_name: str
   :param zone: The zone of the Compute Engine instance
   :type zone: str
   :param user: The name of the user on which the login attempt will be made
   :type user: str
   :param project_id: The project ID of the remote instance
   :type project_id: str
   :param gcp_conn_id: The connection id to use when fetching connection info
   :type gcp_conn_id: str
   :param hostname: The hostname of the target instance. If it is not passed, it will be detected
       automatically.
   :type hostname: str
   :param use_iap_tunnel: Whether to connect through IAP tunnel
   :type use_iap_tunnel: bool
   :param use_internal_ip: Whether to connect using internal IP
   :type use_internal_ip: bool
   :param use_oslogin: Whether to manage keys using OsLogin API. If false,
       keys are managed using instance metadata
   :param expire_time: The maximum amount of time in seconds before the private key expires
   :type expire_time: int
   :param gcp_conn_id: The connection id to use when fetching connection information
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate, if any.
       For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str

   
   .. method:: _oslogin_hook(self)



   
   .. method:: _compute_hook(self)



   
   .. method:: _load_connection_config(self)



   
   .. method:: get_conn(self)

      Return SSH connection.



   
   .. method:: _connect_to_instance(self, user, hostname, pkey, proxy_command)



   
   .. method:: _authorize_compute_engine_instance_metadata(self, pubkey)



   
   .. method:: _authorize_os_login(self, pubkey)



   
   .. method:: _generate_ssh_key(self, user)




