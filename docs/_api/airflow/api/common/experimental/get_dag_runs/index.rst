:mod:`airflow.api.common.experimental.get_dag_runs`
===================================================

.. py:module:: airflow.api.common.experimental.get_dag_runs

.. autoapi-nested-parse::

   DAG runs APIs.



Module Contents
---------------

.. function:: get_dag_runs(dag_id: str, state: Optional[str] = None) -> List[Dict[str, Any]]
   Returns a list of Dag Runs for a specific DAG ID.

   :param dag_id: String identifier of a DAG
   :param state: queued|running|success...
   :return: List of DAG runs of a DAG with requested state,
       or all runs if the state is not specified


