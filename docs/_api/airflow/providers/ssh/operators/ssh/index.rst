:mod:`airflow.providers.ssh.operators.ssh`
==========================================

.. py:module:: airflow.providers.ssh.operators.ssh


Module Contents
---------------

.. py:class:: SSHOperator(*, ssh_hook: Optional[SSHHook] = None, ssh_conn_id: Optional[str] = None, remote_host: Optional[str] = None, command: Optional[str] = None, timeout: int = 10, environment: Optional[dict] = None, get_pty: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   SSHOperator to execute commands on given remote host using the ssh_hook.

   :param ssh_hook: predefined ssh_hook to use for remote execution.
       Either `ssh_hook` or `ssh_conn_id` needs to be provided.
   :type ssh_hook: airflow.providers.ssh.hooks.ssh.SSHHook
   :param ssh_conn_id: connection id from airflow Connections.
       `ssh_conn_id` will be ignored if `ssh_hook` is provided.
   :type ssh_conn_id: str
   :param remote_host: remote host to connect (templated)
       Nullable. If provided, it will replace the `remote_host` which was
       defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
   :type remote_host: str
   :param command: command to execute on remote host. (templated)
   :type command: str
   :param timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
   :type timeout: int
   :param environment: a dict of shell environment variables. Note that the
       server will reject them silently if `AcceptEnv` is not set in SSH config.
   :type environment: dict
   :param get_pty: request a pseudo-terminal from the server. Set to ``True``
       to have the remote process killed upon task timeout.
       The default is ``False`` but note that `get_pty` is forced to ``True``
       when the `command` starts with ``sudo``.
   :type get_pty: bool

   .. attribute:: template_fields
      :annotation: = ['command', 'remote_host']

      

   .. attribute:: template_ext
      :annotation: = ['.sh']

      

   
   .. method:: execute(self, context)



   
   .. method:: tunnel(self)

      Get ssh tunnel




