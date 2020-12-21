:mod:`airflow.utils.log.json_formatter`
=======================================

.. py:module:: airflow.utils.log.json_formatter

.. autoapi-nested-parse::

   json_formatter module stores all related to ElasticSearch specific logger classes



Module Contents
---------------

.. py:class:: JSONFormatter(fmt=None, datefmt=None, style='%', json_fields=None, extras=None)

   Bases: :class:`logging.Formatter`

   JSONFormatter instances are used to convert a log record to json.

   
   .. method:: usesTime(self)



   
   .. method:: format(self, record)




