:mod:`airflow.api_connexion.schemas.dag_schema`
===============================================

.. py:module:: airflow.api_connexion.schemas.dag_schema


Module Contents
---------------

.. py:class:: DagTagSchema

   Bases: :class:`marshmallow_sqlalchemy.SQLAlchemySchema`

   Dag Tag schema

   .. py:class:: Meta

      Meta

      .. attribute:: model
         

         


   .. attribute:: name
      

      


.. py:class:: DAGSchema

   Bases: :class:`marshmallow_sqlalchemy.SQLAlchemySchema`

   DAG schema

   .. py:class:: Meta

      Meta

      .. attribute:: model
         

         


   .. attribute:: dag_id
      

      

   .. attribute:: root_dag_id
      

      

   .. attribute:: is_paused
      

      

   .. attribute:: is_subdag
      

      

   .. attribute:: fileloc
      

      

   .. attribute:: owners
      

      

   .. attribute:: description
      

      

   .. attribute:: schedule_interval
      

      

   .. attribute:: tags
      

      

   
   .. staticmethod:: get_owners(obj: DagModel)

      Convert owners attribute to DAG representation




.. py:class:: DAGDetailSchema

   Bases: :class:`airflow.api_connexion.schemas.dag_schema.DAGSchema`

   DAG details

   .. attribute:: timezone
      

      

   .. attribute:: catchup
      

      

   .. attribute:: orientation
      

      

   .. attribute:: concurrency
      

      

   .. attribute:: start_date
      

      

   .. attribute:: dag_run_timeout
      

      

   .. attribute:: doc_md
      

      

   .. attribute:: default_view
      

      


.. py:class:: DAGCollection

   Bases: :class:`typing.NamedTuple`

   List of DAGs with metadata

   .. attribute:: dags
      :annotation: :List[DagModel]

      

   .. attribute:: total_entries
      :annotation: :int

      


.. py:class:: DAGCollectionSchema

   Bases: :class:`marshmallow.Schema`

   DAG Collection schema

   .. attribute:: dags
      

      

   .. attribute:: total_entries
      

      


.. data:: dags_collection_schema
   

   

.. data:: dag_schema
   

   

.. data:: dag_detail_schema
   

   

