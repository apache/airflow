:mod:`airflow.providers.google.cloud.log.gcs_task_handler`
==========================================================

.. py:module:: airflow.providers.google.cloud.log.gcs_task_handler


Module Contents
---------------

.. data:: _DEFAULT_SCOPESS
   

   

.. py:class:: GCSTaskHandler(*, base_log_folder: str, gcs_log_folder: str, filename_template: str, gcp_key_path: Optional[str] = None, gcp_keyfile_dict: Optional[dict] = None, gcp_scopes: Optional[Collection[str]] = _DEFAULT_SCOPESS, project_id: Optional[str] = None)

   Bases: :class:`airflow.utils.log.file_task_handler.FileTaskHandler`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   GCSTaskHandler is a python log handler that handles and reads
   task instance logs. It extends airflow FileTaskHandler and
   uploads to and reads from GCS remote storage. Upon log reading
   failure, it reads from host machine's local disk.

   :param base_log_folder: Base log folder to place logs.
   :type base_log_folder: str
   :param gcs_log_folder: Path to a remote location where logs will be saved. It must have the prefix
       ``gs://``. For example: ``gs://bucket/remote/log/location``
   :type gcs_log_folder: str
   :param filename_template: template filename string
   :type filename_template: str
   :param gcp_key_path: Path to Google Cloud Service Account file (JSON). Mutually exclusive with
       gcp_keyfile_dict.
       If omitted, authorization based on `the Application Default Credentials
       <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
       be used.
   :type gcp_key_path: str
   :param gcp_keyfile_dict: Dictionary of keyfile parameters. Mutually exclusive with gcp_key_path.
   :type gcp_keyfile_dict: dict
   :param gcp_scopes: Comma-separated string containing OAuth2 scopes
   :type gcp_scopes: str
   :param project_id: Project ID to read the secrets from. If not passed, the project ID from credentials
       will be used.
   :type project_id: str

   
   .. method:: client(self)

      Returns GCS Client.



   
   .. method:: set_context(self, ti)



   
   .. method:: close(self)

      Close and upload local log file to remote storage GCS.



   
   .. method:: _read(self, ti, try_number, metadata=None)

      Read logs of given task instance and try_number from GCS.
      If failed, read the log from task instance host machine.

      :param ti: task instance object
      :param try_number: task instance try_number to read logs from
      :param metadata: log metadata,
                       can be used for steaming log reading and auto-tailing.



   
   .. method:: gcs_write(self, log, remote_log_location)

      Writes the log to the remote_log_location. Fails silently if no log
      was created.

      :param log: the log to write to the remote_log_location
      :type log: str
      :param remote_log_location: the log's location in remote storage
      :type remote_log_location: str (path)




