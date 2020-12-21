:mod:`airflow.www.api.experimental.endpoints`
=============================================

.. py:module:: airflow.www.api.experimental.endpoints


Module Contents
---------------

.. data:: log
   

   

.. data:: T
   

   

.. function:: requires_authentication(function: T)
   Decorator for functions that require authentication


.. data:: api_experimental
   

   

.. function:: add_deprecation_headers(response: Response)
   Add `Deprecation HTTP Header Field
   <https://tools.ietf.org/id/draft-dalal-deprecation-header-03.html>`__.


.. function:: trigger_dag(dag_id)
   Trigger a new dag run for a Dag with an execution date of now unless
   specified in the data.


.. function:: delete_dag(dag_id)
   Delete all DB records related to the specified Dag.


.. function:: dag_runs(dag_id)
   Returns a list of Dag Runs for a specific DAG ID.
   :query param state: a query string parameter '?state=queued|running|success...'

   :param dag_id: String identifier of a DAG
   :return: List of DAG runs of a DAG with requested state,
       or all runs if the state is not specified


.. function:: test()
   Test endpoint to check authentication


.. function:: info()
   Get Airflow Version


.. function:: get_dag_code(dag_id)
   Return python code of a given dag_id.


.. function:: task_info(dag_id, task_id)
   Returns a JSON with a task's public instance variables


.. function:: dag_paused(dag_id, paused)
   (Un)pauses a dag


.. function:: dag_is_paused(dag_id)
   Get paused state of a dag


.. function:: task_instance_info(dag_id, execution_date, task_id)
   Returns a JSON with a task instance's public instance variables.
   The format for the exec_date is expected to be
   "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
   of course need to have been encoded for URL in the request.


.. function:: dag_run_status(dag_id, execution_date)
   Returns a JSON with a dag_run's public instance variables.
   The format for the exec_date is expected to be
   "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
   of course need to have been encoded for URL in the request.


.. function:: latest_dag_runs()
   Returns the latest DagRun for each DAG formatted for the UI


.. function:: get_pool(name)
   Get pool by a given name.


.. function:: get_pools()
   Get all pools.


.. function:: create_pool()
   Create a pool.


.. function:: delete_pool(name)
   Delete pool.


.. function:: get_lineage(dag_id: str, execution_date: str)
   Get Lineage details for a DagRun


