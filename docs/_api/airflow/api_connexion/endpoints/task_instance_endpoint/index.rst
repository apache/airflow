:mod:`airflow.api_connexion.endpoints.task_instance_endpoint`
=============================================================

.. py:module:: airflow.api_connexion.endpoints.task_instance_endpoint


Module Contents
---------------

.. function:: get_task_instance(dag_id: str, dag_run_id: str, task_id: str, session=None)
   Get task instance


.. function:: _apply_array_filter(query, key, values)

.. function:: _apply_range_filter(query, key, value_range: Tuple[Any, Any])

.. function:: get_task_instances(limit: int, dag_id: Optional[str] = None, dag_run_id: Optional[str] = None, execution_date_gte: Optional[str] = None, execution_date_lte: Optional[str] = None, start_date_gte: Optional[str] = None, start_date_lte: Optional[str] = None, end_date_gte: Optional[str] = None, end_date_lte: Optional[str] = None, duration_gte: Optional[float] = None, duration_lte: Optional[float] = None, state: Optional[str] = None, pool: Optional[List[str]] = None, queue: Optional[List[str]] = None, offset: Optional[int] = None, session=None)
   Get list of task instances.


.. function:: get_task_instances_batch(session=None)
   Get list of task instances.


.. function:: post_clear_task_instances(dag_id: str, session=None)
   Clear task instances.


.. function:: post_set_task_instances_state(dag_id, session)
   Set a state of task instances.


