:mod:`airflow.utils.callback_requests`
======================================

.. py:module:: airflow.utils.callback_requests


Module Contents
---------------

.. py:class:: CallbackRequest(full_filepath: str, msg: Optional[str] = None)

   Base Class with information about the callback to be executed.

   :param full_filepath: File Path to use to run the callback
   :param msg: Additional Message that can be used for logging

   
   .. method:: __eq__(self, other)



   
   .. method:: __repr__(self)




.. py:class:: TaskCallbackRequest(full_filepath: str, simple_task_instance: SimpleTaskInstance, is_failure_callback: Optional[bool] = True, msg: Optional[str] = None)

   Bases: :class:`airflow.utils.callback_requests.CallbackRequest`

   A Class with information about the success/failure TI callback to be executed. Currently, only failure
   callbacks (when tasks are externally killed) and Zombies are run via DagFileProcessorProcess.

   :param full_filepath: File Path to use to run the callback
   :param simple_task_instance: Simplified Task Instance representation
   :param is_failure_callback: Flag to determine whether it is a Failure Callback or Success Callback
   :param msg: Additional Message that can be used for logging to determine failure/zombie


.. py:class:: DagCallbackRequest(full_filepath: str, dag_id: str, execution_date: datetime, is_failure_callback: Optional[bool] = True, msg: Optional[str] = None)

   Bases: :class:`airflow.utils.callback_requests.CallbackRequest`

   A Class with information about the success/failure DAG callback to be executed.

   :param full_filepath: File Path to use to run the callback
   :param dag_id: DAG ID
   :param execution_date: Execution Date for the DagRun
   :param is_failure_callback: Flag to determine whether it is a Failure Callback or Success Callback
   :param msg: Additional Message that can be used for logging


.. py:class:: SlaCallbackRequest(full_filepath: str, dag_id: str)

   Bases: :class:`airflow.utils.callback_requests.CallbackRequest`

   A class with information about the SLA callback to be executed.

   :param full_filepath: File Path to use to run the callback
   :param dag_id: DAG ID


