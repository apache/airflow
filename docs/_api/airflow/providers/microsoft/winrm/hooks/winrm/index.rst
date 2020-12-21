:mod:`airflow.providers.microsoft.winrm.hooks.winrm`
====================================================

.. py:module:: airflow.providers.microsoft.winrm.hooks.winrm

.. autoapi-nested-parse::

   Hook for winrm remote execution.



Module Contents
---------------

.. py:class:: WinRMHook(ssh_conn_id: Optional[str] = None, endpoint: Optional[str] = None, remote_host: Optional[str] = None, remote_port: int = 5985, transport: str = 'plaintext', username: Optional[str] = None, password: Optional[str] = None, service: str = 'HTTP', keytab: Optional[str] = None, ca_trust_path: Optional[str] = None, cert_pem: Optional[str] = None, cert_key_pem: Optional[str] = None, server_cert_validation: str = 'validate', kerberos_delegation: bool = False, read_timeout_sec: int = 30, operation_timeout_sec: int = 20, kerberos_hostname_override: Optional[str] = None, message_encryption: Optional[str] = 'auto', credssp_disable_tlsv1_2: bool = False, send_cbt: bool = True)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Hook for winrm remote execution using pywinrm.

   :seealso: https://github.com/diyan/pywinrm/blob/master/winrm/protocol.py

   :param ssh_conn_id: connection id from airflow Connections from where
       all the required parameters can be fetched like username and password.
       Thought the priority is given to the param passed during init
   :type ssh_conn_id: str
   :param endpoint: When not set, endpoint will be constructed like this:
       'http://{remote_host}:{remote_port}/wsman'
   :type endpoint: str
   :param remote_host: Remote host to connect to. Ignored if `endpoint` is set.
   :type remote_host: str
   :param remote_port: Remote port to connect to. Ignored if `endpoint` is set.
   :type remote_port: int
   :param transport: transport type, one of 'plaintext' (default), 'kerberos', 'ssl', 'ntlm', 'credssp'
   :type transport: str
   :param username: username to connect to the remote_host
   :type username: str
   :param password: password of the username to connect to the remote_host
   :type password: str
   :param service: the service name, default is HTTP
   :type service: str
   :param keytab: the path to a keytab file if you are using one
   :type keytab: str
   :param ca_trust_path: Certification Authority trust path
   :type ca_trust_path: str
   :param cert_pem: client authentication certificate file path in PEM format
   :type cert_pem: str
   :param cert_key_pem: client authentication certificate key file path in PEM format
   :type cert_key_pem: str
   :param server_cert_validation: whether server certificate should be validated on
       Python versions that support it; one of 'validate' (default), 'ignore'
   :type server_cert_validation: str
   :param kerberos_delegation: if True, TGT is sent to target server to
       allow multiple hops
   :type kerberos_delegation: bool
   :param read_timeout_sec: maximum seconds to wait before an HTTP connect/read times out (default 30).
       This value should be slightly higher than operation_timeout_sec,
       as the server can block *at least* that long.
   :type read_timeout_sec: int
   :param operation_timeout_sec: maximum allowed time in seconds for any single wsman
       HTTP operation (default 20). Note that operation timeouts while receiving output
       (the only wsman operation that should take any significant time,
       and where these timeouts are expected) will be silently retried indefinitely.
   :type operation_timeout_sec: int
   :param kerberos_hostname_override: the hostname to use for the kerberos exchange
       (defaults to the hostname in the endpoint URL)
   :type kerberos_hostname_override: str
   :param message_encryption: Will encrypt the WinRM messages if set
       and the transport auth supports message encryption. (Default 'auto')
   :type message_encryption: str
   :param credssp_disable_tlsv1_2: Whether to disable TLSv1.2 support and work with older
       protocols like TLSv1.0, default is False
   :type credssp_disable_tlsv1_2: bool
   :param send_cbt: Will send the channel bindings over a HTTPS channel (Default: True)
   :type send_cbt: bool

   
   .. method:: get_conn(self)




