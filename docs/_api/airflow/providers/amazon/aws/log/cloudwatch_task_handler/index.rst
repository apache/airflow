:mod:`airflow.providers.amazon.aws.log.cloudwatch_task_handler`
===============================================================

.. py:module:: airflow.providers.amazon.aws.log.cloudwatch_task_handler


Module Contents
---------------

.. py:class:: CloudwatchTaskHandler(base_log_folder: str, log_group_arn: str, filename_template: str)

   Bases: :class:`airflow.utils.log.file_task_handler.FileTaskHandler`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   CloudwatchTaskHandler is a python log handler that handles and reads task instance logs.

   It extends airflow FileTaskHandler and uploads to and reads from Cloudwatch.

   :param base_log_folder: base folder to store logs locally
   :type base_log_folder: str
   :param log_group_arn: ARN of the Cloudwatch log group for remote log storage
       with format ``arn:aws:logs:{region name}:{account id}:log-group:{group name}``
   :type log_group_arn: str
   :param filename_template: template for file name (local storage) or log stream name (remote)
   :type filename_template: str

   
   .. method:: hook(self)

      Returns AwsLogsHook.



   
   .. method:: _render_filename(self, ti, try_number)



   
   .. method:: set_context(self, ti)



   
   .. method:: close(self)

      Close the handler responsible for the upload of the local log file to Cloudwatch.



   
   .. method:: _read(self, task_instance, try_number, metadata=None)



   
   .. method:: get_cloudwatch_logs(self, stream_name: str)

      Return all logs from the given log stream.

      :param stream_name: name of the Cloudwatch log stream to get all logs from
      :return: string of all logs from the given log stream




