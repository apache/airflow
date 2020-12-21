:mod:`airflow.models.skipmixin`
===============================

.. py:module:: airflow.models.skipmixin


Module Contents
---------------

.. data:: XCOM_SKIPMIXIN_KEY
   :annotation: = skipmixin_key

   

.. data:: XCOM_SKIPMIXIN_SKIPPED
   :annotation: = skipped

   

.. data:: XCOM_SKIPMIXIN_FOLLOWED
   :annotation: = followed

   

.. py:class:: SkipMixin

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   A Mixin to skip Tasks Instances

   
   .. method:: _set_state_to_skipped(self, dag_run, execution_date, tasks, session)

      Used internally to set state of task instances to skipped from the same dag run.



   
   .. method:: skip(self, dag_run, execution_date, tasks, session=None)

      Sets tasks instances to skipped from the same dag run.

      If this instance has a `task_id` attribute, store the list of skipped task IDs to XCom
      so that NotPreviouslySkippedDep knows these tasks should be skipped when they
      are cleared.

      :param dag_run: the DagRun for which to set the tasks to skipped
      :param execution_date: execution_date
      :param tasks: tasks to skip (not task_ids)
      :param session: db session to use



   
   .. method:: skip_all_except(self, ti: TaskInstance, branch_task_ids: Union[str, Iterable[str]])

      This method implements the logic for a branching operator; given a single
      task ID or list of task IDs to follow, this skips all other tasks
      immediately downstream of this operator.

      branch_task_ids is stored to XCom so that NotPreviouslySkippedDep knows skipped tasks or
      newly added tasks should be skipped when they are cleared.




