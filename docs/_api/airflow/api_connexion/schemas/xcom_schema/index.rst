:mod:`airflow.api_connexion.schemas.xcom_schema`
================================================

.. py:module:: airflow.api_connexion.schemas.xcom_schema


Module Contents
---------------

.. py:class:: XComCollectionItemSchema

   Bases: :class:`marshmallow_sqlalchemy.SQLAlchemySchema`

   Schema for a xcom item

   .. py:class:: Meta

      Meta

      .. attribute:: model
         

         


   .. attribute:: key
      

      

   .. attribute:: timestamp
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      


.. py:class:: XComSchema

   Bases: :class:`airflow.api_connexion.schemas.xcom_schema.XComCollectionItemSchema`

   XCom schema

   .. attribute:: value
      

      


.. py:class:: XComCollection

   Bases: :class:`typing.NamedTuple`

   List of XComs with meta

   .. attribute:: xcom_entries
      :annotation: :List[XCom]

      

   .. attribute:: total_entries
      :annotation: :int

      


.. py:class:: XComCollectionSchema

   Bases: :class:`marshmallow.Schema`

   XCom Collection Schema

   .. attribute:: xcom_entries
      

      

   .. attribute:: total_entries
      

      


.. data:: xcom_schema
   

   

.. data:: xcom_collection_item_schema
   

   

.. data:: xcom_collection_schema
   

   

