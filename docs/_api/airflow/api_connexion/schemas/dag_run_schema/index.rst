:mod:`airflow.api_connexion.schemas.dag_run_schema`
===================================================

.. py:module:: airflow.api_connexion.schemas.dag_run_schema


Module Contents
---------------

.. py:class:: ConfObject

   Bases: :class:`marshmallow.fields.Field`

   The conf field

   
   .. method:: _serialize(self, value, attr, obj, **kwargs)



   
   .. method:: _deserialize(self, value, attr, data, **kwargs)




.. py:class:: DAGRunSchema

   Bases: :class:`marshmallow_sqlalchemy.SQLAlchemySchema`

   Schema for DAGRun

   .. py:class:: Meta

      Meta

      .. attribute:: model
         

         

      .. attribute:: dateformat
         :annotation: = iso

         


   .. attribute:: run_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: state
      

      

   .. attribute:: external_trigger
      

      

   .. attribute:: conf
      

      

   
   .. method:: autogenerate(self, data, **kwargs)

      Auto generate run_id and execution_date if they are not loaded




.. py:class:: DAGRunCollection

   Bases: :class:`typing.NamedTuple`

   List of DAGRuns with metadata

   .. attribute:: dag_runs
      :annotation: :List[DagRun]

      

   .. attribute:: total_entries
      :annotation: :int

      


.. py:class:: DAGRunCollectionSchema

   Bases: :class:`marshmallow.schema.Schema`

   DAGRun Collection schema

   .. attribute:: dag_runs
      

      

   .. attribute:: total_entries
      

      


.. py:class:: DagRunsBatchFormSchema

   Bases: :class:`marshmallow.schema.Schema`

   Schema to validate and deserialize the Form(request payload) submitted to DagRun Batch endpoint

   .. py:class:: Meta

      Meta

      .. attribute:: datetimeformat
         :annotation: = iso

         

      .. attribute:: strict
         :annotation: = True

         


   .. attribute:: page_offset
      

      

   .. attribute:: page_limit
      

      

   .. attribute:: dag_ids
      

      

   .. attribute:: execution_date_gte
      

      

   .. attribute:: execution_date_lte
      

      

   .. attribute:: start_date_gte
      

      

   .. attribute:: start_date_lte
      

      

   .. attribute:: end_date_gte
      

      

   .. attribute:: end_date_lte
      

      


.. data:: dagrun_schema
   

   

.. data:: dagrun_collection_schema
   

   

.. data:: dagruns_batch_form_schema
   

   

