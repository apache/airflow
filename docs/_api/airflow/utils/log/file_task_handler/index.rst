:mod:`airflow.utils.log.file_task_handler`
==========================================

.. py:module:: airflow.utils.log.file_task_handler

.. autoapi-nested-parse::

   File logging handler for tasks.



Module Contents
---------------

.. py:class:: FileTaskHandler(base_log_folder: str, filename_template: str)

   Bases: :class:`logging.Handler`

   FileTaskHandler is a python log handler that handles and reads
   task instance logs. It creates and delegates log handling
   to `logging.FileHandler` after receiving task instance context.
   It reads logs from task instance's host machine.

   :param base_log_folder: Base log folder to place logs.
   :param filename_template: template filename string

   
   .. method:: set_context(self, ti: TaskInstance)

      Provide task_instance context to airflow task handler.

      :param ti: task instance object



   
   .. method:: emit(self, record)



   
   .. method:: flush(self)



   
   .. method:: close(self)



   
   .. method:: _render_filename(self, ti, try_number)



   
   .. method:: _read_grouped_logs(self)



   
   .. method:: _read(self, ti, try_number, metadata=None)

      Template method that contains custom logic of reading
      logs given the try_number.

      :param ti: task instance record
      :param try_number: current try_number to read log from
      :param metadata: log metadata,
                       can be used for steaming log reading and auto-tailing.
      :return: log message as a string and metadata.



   
   .. method:: read(self, task_instance, try_number=None, metadata=None)

      Read logs of given task instance from local machine.

      :param task_instance: task instance object
      :param try_number: task instance try_number to read logs from. If None
                         it returns all logs separated by try_number
      :param metadata: log metadata,
                       can be used for steaming log reading and auto-tailing.
      :return: a list of listed tuples which order log string by host



   
   .. method:: _init_file(self, ti)

      Create log directory and give it correct permissions.

      :param ti: task instance object
      :return: relative log path of the given task instance




