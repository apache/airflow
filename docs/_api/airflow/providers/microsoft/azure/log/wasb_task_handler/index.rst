:mod:`airflow.providers.microsoft.azure.log.wasb_task_handler`
==============================================================

.. py:module:: airflow.providers.microsoft.azure.log.wasb_task_handler


Module Contents
---------------

.. py:class:: WasbTaskHandler(base_log_folder: str, wasb_log_folder: str, wasb_container: str, filename_template: str, delete_local_copy: str)

   Bases: :class:`airflow.utils.log.file_task_handler.FileTaskHandler`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   WasbTaskHandler is a python log handler that handles and reads
   task instance logs. It extends airflow FileTaskHandler and
   uploads to and reads from Wasb remote storage.

   
   .. method:: hook(self)

      Returns WasbHook.



   
   .. method:: set_context(self, ti)



   
   .. method:: close(self)

      Close and upload local log file to remote storage Wasb.



   
   .. method:: _read(self, ti, try_number: str, metadata: Optional[str] = None)

      Read logs of given task instance and try_number from Wasb remote storage.
      If failed, read the log from task instance host machine.

      :param ti: task instance object
      :param try_number: task instance try_number to read logs from
      :param metadata: log metadata,
                       can be used for steaming log reading and auto-tailing.



   
   .. method:: wasb_log_exists(self, remote_log_location: str)

      Check if remote_log_location exists in remote storage

      :param remote_log_location: log's location in remote storage
      :return: True if location exists else False



   
   .. method:: wasb_read(self, remote_log_location: str, return_error: bool = False)

      Returns the log found at the remote_log_location. Returns '' if no
      logs are found or there is an error.

      :param remote_log_location: the log's location in remote storage
      :type remote_log_location: str (path)
      :param return_error: if True, returns a string error message if an
          error occurs. Otherwise returns '' when an error occurs.
      :type return_error: bool



   
   .. method:: wasb_write(self, log: str, remote_log_location: str, append: bool = True)

      Writes the log to the remote_log_location. Fails silently if no hook
      was created.

      :param log: the log to write to the remote_log_location
      :type log: str
      :param remote_log_location: the log's location in remote storage
      :type remote_log_location: str (path)
      :param append: if False, any existing log file is overwritten. If True,
          the new log is appended to any existing logs.
      :type append: bool




