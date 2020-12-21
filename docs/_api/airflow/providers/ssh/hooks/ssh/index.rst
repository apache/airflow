:mod:`airflow.providers.ssh.hooks.ssh`
======================================

.. py:module:: airflow.providers.ssh.hooks.ssh

.. autoapi-nested-parse::

   Hook for SSH connections.



Module Contents
---------------

.. py:class:: SSHHook(ssh_conn_id: Optional[str] = None, remote_host: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None, key_file: Optional[str] = None, port: Optional[int] = None, timeout: int = 10, keepalive_interval: int = 30)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Hook for ssh remote execution using Paramiko.
   ref: https://github.com/paramiko/paramiko
   This hook also lets you create ssh tunnel and serve as basis for SFTP file transfer

   :param ssh_conn_id: connection id from airflow Connections from where all the required
       parameters can be fetched like username, password or key_file.
       Thought the priority is given to the param passed during init
   :type ssh_conn_id: str
   :param remote_host: remote host to connect
   :type remote_host: str
   :param username: username to connect to the remote_host
   :type username: str
   :param password: password of the username to connect to the remote_host
   :type password: str
   :param key_file: path to key file to use to connect to the remote_host
   :type key_file: str
   :param port: port of remote host to connect (Default is paramiko SSH_PORT)
   :type port: int
   :param timeout: timeout for the attempt to connect to the remote_host.
   :type timeout: int
   :param keepalive_interval: send a keepalive packet to remote host every
       keepalive_interval seconds
   :type keepalive_interval: int

   
   .. method:: get_conn(self)

      Opens a ssh connection to the remote host.

      :rtype: paramiko.client.SSHClient



   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, exc_type, exc_val, exc_tb)



   
   .. method:: get_tunnel(self, remote_port: int, remote_host: str = 'localhost', local_port: Optional[int] = None)

      Creates a tunnel between two hosts. Like ssh -L <LOCAL_PORT>:host:<REMOTE_PORT>.

      :param remote_port: The remote port to create a tunnel to
      :type remote_port: int
      :param remote_host: The remote host to create a tunnel to (default localhost)
      :type remote_host: str
      :param local_port:  The local port to attach the tunnel to
      :type local_port: int

      :return: sshtunnel.SSHTunnelForwarder object



   
   .. method:: create_tunnel(self, local_port: int, remote_port: int, remote_host: str = 'localhost')

      Creates tunnel for SSH connection [Deprecated].

      :param local_port: local port number
      :param remote_port: remote port number
      :param remote_host: remote host
      :return:




