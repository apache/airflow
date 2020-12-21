:mod:`airflow.api_connexion.schemas.task_instance_schema`
=========================================================

.. py:module:: airflow.api_connexion.schemas.task_instance_schema


Module Contents
---------------

.. py:class:: TaskInstanceSchema

   Bases: :class:`marshmallow.Schema`

   Task instance schema

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: duration
      

      

   .. attribute:: state
      

      

   .. attribute:: _try_number
      

      

   .. attribute:: max_tries
      

      

   .. attribute:: hostname
      

      

   .. attribute:: unixname
      

      

   .. attribute:: pool
      

      

   .. attribute:: pool_slots
      

      

   .. attribute:: queue
      

      

   .. attribute:: priority_weight
      

      

   .. attribute:: operator
      

      

   .. attribute:: queued_dttm
      

      

   .. attribute:: pid
      

      

   .. attribute:: executor_config
      

      

   .. attribute:: sla_miss
      

      

   
   .. method:: get_attribute(self, obj, attr, default)




.. py:class:: TaskInstanceCollection

   Bases: :class:`typing.NamedTuple`

   List of task instances with metadata

   .. attribute:: task_instances
      :annotation: :List[Tuple[TaskInstance, Optional[SlaMiss]]]

      

   .. attribute:: total_entries
      :annotation: :int

      


.. py:class:: TaskInstanceCollectionSchema

   Bases: :class:`marshmallow.Schema`

   Task instance collection schema

   .. attribute:: task_instances
      

      

   .. attribute:: total_entries
      

      


.. py:class:: TaskInstanceBatchFormSchema

   Bases: :class:`marshmallow.Schema`

   Schema for the request form passed to Task Instance Batch endpoint

   .. attribute:: page_offset
      

      

   .. attribute:: page_limit
      

      

   .. attribute:: dag_ids
      

      

   .. attribute:: execution_date_gte
      

      

   .. attribute:: execution_date_lte
      

      

   .. attribute:: start_date_gte
      

      

   .. attribute:: start_date_lte
      

      

   .. attribute:: end_date_gte
      

      

   .. attribute:: end_date_lte
      

      

   .. attribute:: duration_gte
      

      

   .. attribute:: duration_lte
      

      

   .. attribute:: state
      

      

   .. attribute:: pool
      

      

   .. attribute:: queue
      

      


.. py:class:: ClearTaskInstanceFormSchema

   Bases: :class:`marshmallow.Schema`

   Schema for handling the request of clearing task instance of a Dag

   .. attribute:: dry_run
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: only_failed
      

      

   .. attribute:: only_running
      

      

   .. attribute:: include_subdags
      

      

   .. attribute:: include_parentdag
      

      

   .. attribute:: reset_dag_runs
      

      

   
   .. method:: validate_form(self, data, **kwargs)

      Validates clear task instance form




.. py:class:: SetTaskInstanceStateFormSchema

   Bases: :class:`marshmallow.Schema`

   Schema for handling the request of setting state of task instance of a DAG

   .. attribute:: dry_run
      

      

   .. attribute:: task_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: include_upstream
      

      

   .. attribute:: include_downstream
      

      

   .. attribute:: include_future
      

      

   .. attribute:: include_past
      

      

   .. attribute:: new_state
      

      


.. py:class:: TaskInstanceReferenceSchema

   Bases: :class:`marshmallow.Schema`

   Schema for the task instance reference schema

   .. attribute:: task_id
      

      

   .. attribute:: dag_run_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   
   .. method:: get_attribute(self, obj, attr, default)

      Overwritten marshmallow function




.. py:class:: TaskInstanceReferenceCollection

   Bases: :class:`typing.NamedTuple`

   List of objects with metadata about taskinstance and dag_run_id

   .. attribute:: task_instances
      :annotation: :List[Tuple[TaskInstance, str]]

      


.. py:class:: TaskInstanceReferenceCollectionSchema

   Bases: :class:`marshmallow.Schema`

   Collection schema for task reference

   .. attribute:: task_instances
      

      


.. data:: task_instance_schema
   

   

.. data:: task_instance_collection_schema
   

   

.. data:: task_instance_batch_form
   

   

.. data:: clear_task_instance_form
   

   

.. data:: set_task_instance_state_form
   

   

.. data:: task_instance_reference_schema
   

   

.. data:: task_instance_reference_collection_schema
   

   

