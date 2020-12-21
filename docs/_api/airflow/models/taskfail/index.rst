:mod:`airflow.models.taskfail`
==============================

.. py:module:: airflow.models.taskfail

.. autoapi-nested-parse::

   Taskfail tracks the failed run durations of each task instance



Module Contents
---------------

.. py:class:: TaskFail(task, execution_date, start_date, end_date)

   Bases: :class:`airflow.models.base.Base`

   TaskFail tracks the failed run durations of each task instance.

   .. attribute:: __tablename__
      :annotation: = task_fail

      

   .. attribute:: id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: duration
      

      

   .. attribute:: __table_args__
      

      


