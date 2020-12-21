:mod:`airflow.models.taskreschedule`
====================================

.. py:module:: airflow.models.taskreschedule

.. autoapi-nested-parse::

   TaskReschedule tracks rescheduled task instances.



Module Contents
---------------

.. py:class:: TaskReschedule(task, execution_date, try_number, start_date, end_date, reschedule_date)

   Bases: :class:`airflow.models.base.Base`

   TaskReschedule tracks rescheduled task instances.

   .. attribute:: __tablename__
      :annotation: = task_reschedule

      

   .. attribute:: id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: try_number
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: duration
      

      

   .. attribute:: reschedule_date
      

      

   .. attribute:: __table_args__
      

      

   
   .. staticmethod:: query_for_task_instance(task_instance, descending=False, session=None)

      Returns query for task reschedules for a given the task instance.

      :param session: the database session object
      :type session: sqlalchemy.orm.session.Session
      :param task_instance: the task instance to find task reschedules for
      :type task_instance: airflow.models.TaskInstance
      :param descending: If True then records are returned in descending order
      :type descending: bool



   
   .. staticmethod:: find_for_task_instance(task_instance, session=None)

      Returns all task reschedules for the task instance and try number,
      in ascending order.

      :param session: the database session object
      :type session: sqlalchemy.orm.session.Session
      :param task_instance: the task instance to find task reschedules for
      :type task_instance: airflow.models.TaskInstance




