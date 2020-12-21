:mod:`airflow.utils.log.file_processor_handler`
===============================================

.. py:module:: airflow.utils.log.file_processor_handler


Module Contents
---------------

.. py:class:: FileProcessorHandler(base_log_folder, filename_template)

   Bases: :class:`logging.Handler`

   FileProcessorHandler is a python log handler that handles
   dag processor logs. It creates and delegates log handling
   to `logging.FileHandler` after receiving dag processor context.

   :param base_log_folder: Base log folder to place logs.
   :param filename_template: template filename string

   
   .. method:: set_context(self, filename)

      Provide filename context to airflow task handler.

      :param filename: filename in which the dag is located



   
   .. method:: emit(self, record)



   
   .. method:: flush(self)



   
   .. method:: close(self)



   
   .. method:: _render_filename(self, filename)



   
   .. method:: _get_log_directory(self)



   
   .. method:: _symlink_latest_log_directory(self)

      Create symbolic link to the current day's log directory to
      allow easy access to the latest scheduler log files.

      :return: None



   
   .. method:: _init_file(self, filename)

      Create log file and directory if required.

      :param filename: task instance object
      :return: relative log path of the given task instance




