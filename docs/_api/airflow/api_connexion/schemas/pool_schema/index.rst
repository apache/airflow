:mod:`airflow.api_connexion.schemas.pool_schema`
================================================

.. py:module:: airflow.api_connexion.schemas.pool_schema


Module Contents
---------------

.. py:class:: PoolSchema

   Bases: :class:`marshmallow_sqlalchemy.SQLAlchemySchema`

   Pool schema

   .. py:class:: Meta

      Meta

      .. attribute:: model
         

         


   .. attribute:: name
      

      

   .. attribute:: slots
      

      

   .. attribute:: occupied_slots
      

      

   .. attribute:: running_slots
      

      

   .. attribute:: queued_slots
      

      

   .. attribute:: open_slots
      

      

   
   .. staticmethod:: get_occupied_slots(obj: Pool)

      Returns the occupied slots of the pool.



   
   .. staticmethod:: get_running_slots(obj: Pool)

      Returns the running slots of the pool.



   
   .. staticmethod:: get_queued_slots(obj: Pool)

      Returns the queued slots of the pool.



   
   .. staticmethod:: get_open_slots(obj: Pool)

      Returns the open slots of the pool.




.. py:class:: PoolCollection

   Bases: :class:`typing.NamedTuple`

   List of Pools with metadata

   .. attribute:: pools
      :annotation: :List[Pool]

      

   .. attribute:: total_entries
      :annotation: :int

      


.. py:class:: PoolCollectionSchema

   Bases: :class:`marshmallow.Schema`

   Pool Collection schema

   .. attribute:: pools
      

      

   .. attribute:: total_entries
      

      


.. data:: pool_collection_schema
   

   

.. data:: pool_schema
   

   

