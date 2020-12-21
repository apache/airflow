:mod:`airflow.utils.timeout`
============================

.. py:module:: airflow.utils.timeout


Module Contents
---------------

.. py:class:: timeout(seconds=1, error_message='Timeout')

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   To be used in a ``with`` block and timeout its content.

   
   .. method:: handle_timeout(self, signum, frame)

      Logs information and raises AirflowTaskTimeout.



   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, type_, value, traceback)




