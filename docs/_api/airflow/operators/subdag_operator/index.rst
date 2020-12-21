:mod:`airflow.operators.subdag_operator`
========================================

.. py:module:: airflow.operators.subdag_operator

.. autoapi-nested-parse::

   The module which provides a way to nest your DAGs and so your levels of complexity.



Module Contents
---------------

.. py:class:: SkippedStatePropagationOptions

   Bases: :class:`enum.Enum`

   Available options for skipped state propagation of subdag's tasks to parent dag tasks.

   .. attribute:: ALL_LEAVES
      :annotation: = all_leaves

      

   .. attribute:: ANY_LEAF
      :annotation: = any_leaf

      


.. py:class:: SubDagOperator(*, subdag: DAG, session: Optional[Session] = None, conf: Optional[Dict] = None, propagate_skipped_state: Optional[SkippedStatePropagationOptions] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   This runs a sub dag. By convention, a sub dag's dag_id
   should be prefixed by its parent and a dot. As in `parent.child`.

   Although SubDagOperator can occupy a pool/concurrency slot,
   user can specify the mode=reschedule so that the slot will be
   released periodically to avoid potential deadlock.

   :param subdag: the DAG object to run as a subdag of the current DAG.
   :param session: sqlalchemy session
   :param conf: Configuration for the subdag
   :type conf: dict
   :param propagate_skipped_state: by setting this argument you can define
       whether the skipped state of leaf task(s) should be propagated to the parent dag's downstream task.

   .. attribute:: ui_color
      :annotation: = #555

      

   .. attribute:: ui_fgcolor
      :annotation: = #fff

      

   
   .. method:: _validate_dag(self, kwargs)



   
   .. method:: _validate_pool(self, session)



   
   .. method:: _get_dagrun(self, execution_date)



   
   .. method:: _reset_dag_run_and_task_instances(self, dag_run, execution_date)

      Set the DagRun state to RUNNING and set the failed TaskInstances to None state
      for scheduler to pick up.

      :param dag_run: DAG run
      :param execution_date: Execution date
      :return: None



   
   .. method:: pre_execute(self, context)



   
   .. method:: poke(self, context)



   
   .. method:: post_execute(self, context, result=None)



   
   .. method:: _check_skipped_states(self, context)



   
   .. method:: _get_leaves_tis(self, execution_date)



   
   .. method:: _skip_downstream_tasks(self, context)




