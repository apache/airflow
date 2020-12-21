:mod:`airflow.api_connexion.endpoints.dag_run_endpoint`
=======================================================

.. py:module:: airflow.api_connexion.endpoints.dag_run_endpoint


Module Contents
---------------

.. function:: delete_dag_run(dag_id, dag_run_id, session)
   Delete a DAG Run


.. function:: get_dag_run(dag_id, dag_run_id, session)
   Get a DAG Run.


.. function:: get_dag_runs(session, dag_id, start_date_gte=None, start_date_lte=None, execution_date_gte=None, execution_date_lte=None, end_date_gte=None, end_date_lte=None, offset=None, limit=None)
   Get all DAG Runs.


.. function:: _fetch_dag_runs(query, end_date_gte, end_date_lte, execution_date_gte, execution_date_lte, start_date_gte, start_date_lte, limit, offset)

.. function:: _apply_date_filters_to_query(query, end_date_gte, end_date_lte, execution_date_gte, execution_date_lte, start_date_gte, start_date_lte)

.. function:: get_dag_runs_batch(session)
   Get list of DAG Runs


.. function:: post_dag_run(dag_id, session)
   Trigger a DAG.


