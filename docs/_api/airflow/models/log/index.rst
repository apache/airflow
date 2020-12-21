:mod:`airflow.models.log`
=========================

.. py:module:: airflow.models.log


Module Contents
---------------

.. py:class:: Log(event, task_instance=None, owner=None, extra=None, **kwargs)

   Bases: :class:`airflow.models.base.Base`

   Used to actively log events to the database

   .. attribute:: __tablename__
      :annotation: = log

      

   .. attribute:: id
      

      

   .. attribute:: dttm
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: event
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: owner
      

      

   .. attribute:: extra
      

      

   .. attribute:: __table_args__
      

      


