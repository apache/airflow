:mod:`airflow.models.errors`
============================

.. py:module:: airflow.models.errors


Module Contents
---------------

.. py:class:: ImportError

   Bases: :class:`airflow.models.base.Base`

   A table to store all Import Errors. The ImportErrors are recorded when parsing DAGs.
   This errors are displayed on the Webserver.

   .. attribute:: __tablename__
      :annotation: = import_error

      

   .. attribute:: id
      

      

   .. attribute:: timestamp
      

      

   .. attribute:: filename
      

      

   .. attribute:: stacktrace
      

      


