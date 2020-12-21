:mod:`airflow.task.task_runner.base_task_runner`
================================================

.. py:module:: airflow.task.task_runner.base_task_runner

.. autoapi-nested-parse::

   Base task runner



Module Contents
---------------

.. data:: PYTHONPATH_VAR
   :annotation: = PYTHONPATH

   

.. py:class:: BaseTaskRunner(local_task_job)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Runs Airflow task instances by invoking the `airflow tasks run` command with raw
   mode enabled in a subprocess.

   :param local_task_job: The local task job associated with running the
       associated task instance.
   :type local_task_job: airflow.jobs.local_task_job.LocalTaskJob

   
   .. method:: _read_task_logs(self, stream)



   
   .. method:: run_command(self, run_with=None)

      Run the task command.

      :param run_with: list of tokens to run the task command with e.g. ``['bash', '-c']``
      :type run_with: list
      :return: the process that was run
      :rtype: subprocess.Popen



   
   .. method:: start(self)

      Start running the task instance in a subprocess.



   
   .. method:: return_code(self)

      :return: The return code associated with running the task instance or
          None if the task is not yet done.
      :rtype: int



   
   .. method:: terminate(self)

      Kill the running task instance.



   
   .. method:: on_finish(self)

      A callback that should be called when this is done running.




