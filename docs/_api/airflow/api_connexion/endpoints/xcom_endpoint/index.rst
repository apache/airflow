:mod:`airflow.api_connexion.endpoints.xcom_endpoint`
====================================================

.. py:module:: airflow.api_connexion.endpoints.xcom_endpoint


Module Contents
---------------

.. function:: get_xcom_entries(dag_id: str, dag_run_id: str, task_id: str, session: Session, limit: Optional[int], offset: Optional[int] = None) -> XComCollectionSchema
   Get all XCom values


.. function:: get_xcom_entry(dag_id: str, task_id: str, dag_run_id: str, xcom_key: str, session: Session) -> XComCollectionItemSchema
   Get an XCom entry


