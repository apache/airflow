:mod:`airflow.api_connexion.schemas.task_schema`
================================================

.. py:module:: airflow.api_connexion.schemas.task_schema


Module Contents
---------------

.. py:class:: TaskSchema

   Bases: :class:`marshmallow.Schema`

   Task schema

   .. attribute:: class_ref
      

      

   .. attribute:: task_id
      

      

   .. attribute:: owner
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: trigger_rule
      

      

   .. attribute:: extra_links
      

      

   .. attribute:: depends_on_past
      

      

   .. attribute:: wait_for_downstream
      

      

   .. attribute:: retries
      

      

   .. attribute:: queue
      

      

   .. attribute:: pool
      

      

   .. attribute:: pool_slots
      

      

   .. attribute:: execution_timeout
      

      

   .. attribute:: retry_delay
      

      

   .. attribute:: retry_exponential_backoff
      

      

   .. attribute:: priority_weight
      

      

   .. attribute:: weight_rule
      

      

   .. attribute:: ui_color
      

      

   .. attribute:: ui_fgcolor
      

      

   .. attribute:: template_fields
      

      

   .. attribute:: sub_dag
      

      

   .. attribute:: downstream_task_ids
      

      

   
   .. method:: _get_class_reference(self, obj)




.. py:class:: TaskCollection

   Bases: :class:`typing.NamedTuple`

   List of Tasks with metadata

   .. attribute:: tasks
      :annotation: :List[BaseOperator]

      

   .. attribute:: total_entries
      :annotation: :int

      


.. py:class:: TaskCollectionSchema

   Bases: :class:`marshmallow.Schema`

   Schema for TaskCollection

   .. attribute:: tasks
      

      

   .. attribute:: total_entries
      

      


.. data:: task_schema
   

   

.. data:: task_collection_schema
   

   

