:mod:`airflow.api_connexion.schemas.event_log_schema`
=====================================================

.. py:module:: airflow.api_connexion.schemas.event_log_schema


Module Contents
---------------

.. py:class:: EventLogSchema

   Bases: :class:`marshmallow_sqlalchemy.SQLAlchemySchema`

   Event log schema

   .. py:class:: Meta

      Meta

      .. attribute:: model
         

         


   .. attribute:: id
      

      

   .. attribute:: dttm
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: event
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: owner
      

      

   .. attribute:: extra
      

      


.. py:class:: EventLogCollection

   Bases: :class:`typing.NamedTuple`

   List of import errors with metadata

   .. attribute:: event_logs
      :annotation: :List[Log]

      

   .. attribute:: total_entries
      :annotation: :int

      


.. py:class:: EventLogCollectionSchema

   Bases: :class:`marshmallow.Schema`

   EventLog Collection Schema

   .. attribute:: event_logs
      

      

   .. attribute:: total_entries
      

      


.. data:: event_log_schema
   

   

.. data:: event_log_collection_schema
   

   

