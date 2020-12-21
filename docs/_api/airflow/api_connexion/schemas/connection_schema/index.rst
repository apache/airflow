:mod:`airflow.api_connexion.schemas.connection_schema`
======================================================

.. py:module:: airflow.api_connexion.schemas.connection_schema


Module Contents
---------------

.. py:class:: ConnectionCollectionItemSchema

   Bases: :class:`marshmallow_sqlalchemy.SQLAlchemySchema`

   Schema for a connection item

   .. py:class:: Meta

      Meta

      .. attribute:: model
         

         


   .. attribute:: connection_id
      

      

   .. attribute:: conn_type
      

      

   .. attribute:: host
      

      

   .. attribute:: login
      

      

   .. attribute:: schema
      

      

   .. attribute:: port
      

      


.. py:class:: ConnectionSchema

   Bases: :class:`airflow.api_connexion.schemas.connection_schema.ConnectionCollectionItemSchema`

   Connection schema

   .. attribute:: password
      

      

   .. attribute:: extra
      

      


.. py:class:: ConnectionCollection

   Bases: :class:`typing.NamedTuple`

   List of Connections with meta

   .. attribute:: connections
      :annotation: :List[Connection]

      

   .. attribute:: total_entries
      :annotation: :int

      


.. py:class:: ConnectionCollectionSchema

   Bases: :class:`marshmallow.Schema`

   Connection Collection Schema

   .. attribute:: connections
      

      

   .. attribute:: total_entries
      

      


.. data:: connection_schema
   

   

.. data:: connection_collection_item_schema
   

   

.. data:: connection_collection_schema
   

   

