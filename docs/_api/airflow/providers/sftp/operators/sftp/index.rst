:mod:`airflow.providers.sftp.operators.sftp`
============================================

.. py:module:: airflow.providers.sftp.operators.sftp

.. autoapi-nested-parse::

   This module contains SFTP operator.



Module Contents
---------------

.. py:class:: SFTPOperation

   Operation that can be used with SFTP/

   .. attribute:: PUT
      :annotation: = put

      

   .. attribute:: GET
      :annotation: = get

      


.. py:class:: SFTPOperator(*, ssh_hook=None, ssh_conn_id=None, remote_host=None, local_filepath=None, remote_filepath=None, operation=SFTPOperation.PUT, confirm=True, create_intermediate_dirs=False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   SFTPOperator for transferring files from remote host to local or vice a versa.
   This operator uses ssh_hook to open sftp transport channel that serve as basis
   for file transfer.

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
   :param local_filepath: local file path to get or put. (templated)
   :type local_filepath: str
   :param remote_filepath: remote file path to get or put. (templated)
   :type remote_filepath: str
   :param operation: specify operation 'get' or 'put', defaults to put
   :type operation: str
   :param confirm: specify if the SFTP operation should be confirmed, defaults to True
   :type confirm: bool
   :param create_intermediate_dirs: create missing intermediate directories when
       copying from remote to local and vice-versa. Default is False.

       Example: The following task would copy ``file.txt`` to the remote host
       at ``/tmp/tmp1/tmp2/`` while creating ``tmp``,``tmp1`` and ``tmp2`` if they
       don't exist. If the parameter is not passed it would error as the directory
       does not exist. ::

           put_file = SFTPOperator(
               task_id="test_sftp",
               ssh_conn_id="ssh_default",
               local_filepath="/tmp/file.txt",
               remote_filepath="/tmp/tmp1/tmp2/file.txt",
               operation="put",
               create_intermediate_dirs=True,
               dag=dag
           )

   :type create_intermediate_dirs: bool

   .. attribute:: template_fields
      :annotation: = ['local_filepath', 'remote_filepath', 'remote_host']

      

   
   .. method:: execute(self, context: Any)




.. function:: _make_intermediate_dirs(sftp_client, remote_directory) -> None
   Create all the intermediate directories in a remote host

   :param sftp_client: A Paramiko SFTP client.
   :param remote_directory: Absolute Path of the directory containing the file
   :return:


