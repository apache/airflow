:mod:`airflow.utils.log.task_handler_with_custom_formatter`
===========================================================

.. py:module:: airflow.utils.log.task_handler_with_custom_formatter

.. autoapi-nested-parse::

   Custom logging formatter for Airflow



Module Contents
---------------

.. py:class:: TaskHandlerWithCustomFormatter(stream)

   Bases: :class:`logging.StreamHandler`

   Custom implementation of StreamHandler, a class which writes logging records for Airflow

   
   .. method:: set_context(self, ti)

      Accept the run-time context (i.e. the current task) and configure the formatter accordingly.

      :param ti:
      :return:



   
   .. method:: _render_prefix(self, ti)




