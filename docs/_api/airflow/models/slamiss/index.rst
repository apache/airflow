:mod:`airflow.models.slamiss`
=============================

.. py:module:: airflow.models.slamiss


Module Contents
---------------

.. py:class:: SlaMiss

   Bases: :class:`airflow.models.base.Base`

   Model that stores a history of the SLA that have been missed.
   It is used to keep track of SLA failures over time and to avoid double
   triggering alert emails.

   .. attribute:: __tablename__
      :annotation: = sla_miss

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: email_sent
      

      

   .. attribute:: timestamp
      

      

   .. attribute:: description
      

      

   .. attribute:: notification_sent
      

      

   .. attribute:: __table_args__
      

      

   
   .. method:: __repr__(self)




