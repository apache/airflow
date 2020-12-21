:mod:`airflow.api.common.experimental.trigger_dag`
==================================================

.. py:module:: airflow.api.common.experimental.trigger_dag

.. autoapi-nested-parse::

   Triggering DAG runs APIs.



Module Contents
---------------

.. function:: _trigger_dag(dag_id: str, dag_bag: DagBag, run_id: Optional[str] = None, conf: Optional[Union[dict, str]] = None, execution_date: Optional[datetime] = None, replace_microseconds: bool = True) -> List[DagRun]
   Triggers DAG run.

   :param dag_id: DAG ID
   :param dag_bag: DAG Bag model
   :param run_id: ID of the dag_run
   :param conf: configuration
   :param execution_date: date of execution
   :param replace_microseconds: whether microseconds should be zeroed
   :return: list of triggered dags


.. function:: trigger_dag(dag_id: str, run_id: Optional[str] = None, conf: Optional[Union[dict, str]] = None, execution_date: Optional[datetime] = None, replace_microseconds: bool = True) -> Optional[DagRun]
   Triggers execution of DAG specified by dag_id

   :param dag_id: DAG ID
   :param run_id: ID of the dag_run
   :param conf: configuration
   :param execution_date: date of execution
   :param replace_microseconds: whether microseconds should be zeroed
   :return: first dag run triggered - even if more than one Dag Runs were triggered or None


