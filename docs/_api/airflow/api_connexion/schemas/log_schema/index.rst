:mod:`airflow.api_connexion.schemas.log_schema`
===============================================

.. py:module:: airflow.api_connexion.schemas.log_schema


Module Contents
---------------

.. py:class:: LogsSchema

   Bases: :class:`marshmallow.Schema`

   Schema for logs

   .. attribute:: content
      

      

   .. attribute:: continuation_token
      

      


.. py:class:: LogResponseObject

   Bases: :class:`typing.NamedTuple`

   Log Response Object

   .. attribute:: content
      :annotation: :str

      

   .. attribute:: continuation_token
      :annotation: :str

      


.. data:: logs_schema
   

   

