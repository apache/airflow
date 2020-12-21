:mod:`airflow.operators.dagrun_operator`
========================================

.. py:module:: airflow.operators.dagrun_operator


Module Contents
---------------

.. py:class:: TriggerDagRunLink

   Bases: :class:`airflow.models.BaseOperatorLink`

   Operator link for TriggerDagRunOperator. It allows users to access
   DAG triggered by task using TriggerDagRunOperator.

   .. attribute:: name
      :annotation: = Triggered DAG

      

   
   .. method:: get_link(self, operator, dttm)




.. py:class:: TriggerDagRunOperator(*, trigger_dag_id: str, conf: Optional[Dict] = None, execution_date: Optional[Union[str, datetime.datetime]] = None, reset_dag_run: bool = False, wait_for_completion: bool = False, poke_interval: int = 60, allowed_states: Optional[List] = None, failed_states: Optional[List] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Triggers a DAG run for a specified ``dag_id``

   :param trigger_dag_id: the dag_id to trigger (templated)
   :type trigger_dag_id: str
   :param conf: Configuration for the DAG run
   :type conf: dict
   :param execution_date: Execution date for the dag (templated)
   :type execution_date: str or datetime.datetime
   :param reset_dag_run: Whether or not clear existing dag run if already exists.
       This is useful when backfill or rerun an existing dag run.
       When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised.
       When reset_dag_run=True and dag run exists, existing dag run will be cleared to rerun.
   :type reset_dag_run: bool
   :param wait_for_completion: Whether or not wait for dag run completion. (default: False)
   :type wait_for_completion: bool
   :param poke_interval: Poke interval to check dag run status when wait_for_completion=True.
       (default: 60)
   :type poke_interval: int
   :param allowed_states: list of allowed states, default is ``['success']``
   :type allowed_states: list
   :param failed_states: list of failed or dis-allowed states, default is ``None``
   :type failed_states: list

   .. attribute:: template_fields
      :annotation: = ['trigger_dag_id', 'execution_date', 'conf']

      

   .. attribute:: ui_color
      :annotation: = #ffefeb

      

   .. attribute:: operator_extra_links
      

      Return operator extra links


   
   .. method:: execute(self, context: Dict)




