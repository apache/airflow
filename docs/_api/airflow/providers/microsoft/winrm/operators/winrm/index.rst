:mod:`airflow.providers.microsoft.winrm.operators.winrm`
========================================================

.. py:module:: airflow.providers.microsoft.winrm.operators.winrm


Module Contents
---------------

.. py:class:: WinRMOperator(*, winrm_hook: Optional[WinRMHook] = None, ssh_conn_id: Optional[str] = None, remote_host: Optional[str] = None, command: Optional[str] = None, timeout: int = 10, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   WinRMOperator to execute commands on given remote host using the winrm_hook.

   :param winrm_hook: predefined ssh_hook to use for remote execution
   :type winrm_hook: airflow.providers.microsoft.winrm.hooks.winrm.WinRMHook
   :param ssh_conn_id: connection id from airflow Connections
   :type ssh_conn_id: str
   :param remote_host: remote host to connect
   :type remote_host: str
   :param command: command to execute on remote host. (templated)
   :type command: str
   :param timeout: timeout for executing the command.
   :type timeout: int

   .. attribute:: template_fields
      :annotation: = ['command']

      

   
   .. method:: execute(self, context: dict)




