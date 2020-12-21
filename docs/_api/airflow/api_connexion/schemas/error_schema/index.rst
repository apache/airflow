:mod:`airflow.api_connexion.schemas.error_schema`
=================================================

.. py:module:: airflow.api_connexion.schemas.error_schema


Module Contents
---------------

.. py:class:: ImportErrorSchema

   Bases: :class:`marshmallow_sqlalchemy.SQLAlchemySchema`

   Import error schema

   .. py:class:: Meta

      Meta

      .. attribute:: model
         

         


   .. attribute:: import_error_id
      

      

   .. attribute:: timestamp
      

      

   .. attribute:: filename
      

      

   .. attribute:: stack_trace
      

      


.. py:class:: ImportErrorCollection

   Bases: :class:`typing.NamedTuple`

   List of import errors with metadata

   .. attribute:: import_errors
      :annotation: :List[ImportError]

      

   .. attribute:: total_entries
      :annotation: :int

      


.. py:class:: ImportErrorCollectionSchema

   Bases: :class:`marshmallow.Schema`

   Import error collection schema

   .. attribute:: import_errors
      

      

   .. attribute:: total_entries
      

      


.. data:: import_error_schema
   

   

.. data:: import_error_collection_schema
   

   

