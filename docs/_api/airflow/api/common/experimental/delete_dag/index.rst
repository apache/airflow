:mod:`airflow.api.common.experimental.delete_dag`
=================================================

.. py:module:: airflow.api.common.experimental.delete_dag

.. autoapi-nested-parse::

   Delete DAGs APIs.



Module Contents
---------------

.. data:: log
   

   

.. function:: delete_dag(dag_id: str, keep_records_in_log: bool = True, session=None) -> int
   :param dag_id: the dag_id of the DAG to delete
   :param keep_records_in_log: whether keep records of the given dag_id
       in the Log table in the backend database (for reasons like auditing).
       The default value is True.
   :param session: session used
   :return count of deleted dags


