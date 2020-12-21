:mod:`airflow.task.task_runner.standard_task_runner`
====================================================

.. py:module:: airflow.task.task_runner.standard_task_runner

.. autoapi-nested-parse::

   Standard task runner



Module Contents
---------------

.. py:class:: StandardTaskRunner(local_task_job)

   Bases: :class:`airflow.task.task_runner.base_task_runner.BaseTaskRunner`

   Standard runner for all tasks.

   
   .. method:: start(self)



   
   .. method:: _start_by_exec(self)



   
   .. method:: _start_by_fork(self)



   
   .. method:: return_code(self, timeout=0)



   
   .. method:: terminate(self)




