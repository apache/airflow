:mod:`airflow.api.common.experimental.get_dag_run_state`
========================================================

.. py:module:: airflow.api.common.experimental.get_dag_run_state

.. autoapi-nested-parse::

   DAG run APIs.



Module Contents
---------------

.. function:: get_dag_run_state(dag_id: str, execution_date: datetime) -> Dict[str, str]
   Return the task object identified by the given dag_id and task_id.

   :param dag_id: DAG id
   :param execution_date: execution date
   :return: Dictionary storing state of the object


