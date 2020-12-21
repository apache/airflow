:mod:`airflow.task.task_runner`
===============================

.. py:module:: airflow.task.task_runner


Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   base_task_runner/index.rst
   cgroup_task_runner/index.rst
   standard_task_runner/index.rst


Package Contents
----------------

.. data:: conf
   

   

.. py:exception:: AirflowConfigException

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when there is configuration problem


.. function:: import_string(dotted_path)
   Import a dotted module path and return the attribute/class designated by the
   last name in the path. Raise ImportError if the import failed.


.. data:: log
   

   

.. data:: _TASK_RUNNER_NAME
   

   

.. data:: STANDARD_TASK_RUNNER
   :annotation: = StandardTaskRunner

   

.. data:: CGROUP_TASK_RUNNER
   :annotation: = CgroupTaskRunner

   

.. data:: CORE_TASK_RUNNERS
   

   

.. function:: get_task_runner(local_task_job)
   Get the task runner that can be used to run the given job.

   :param local_task_job: The LocalTaskJob associated with the TaskInstance
       that needs to be executed.
   :type local_task_job: airflow.jobs.local_task_job.LocalTaskJob
   :return: The task runner to use to run the task.
   :rtype: airflow.task.task_runner.base_task_runner.BaseTaskRunner


