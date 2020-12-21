:mod:`airflow.ti_deps.deps.trigger_rule_dep`
============================================

.. py:module:: airflow.ti_deps.deps.trigger_rule_dep


Module Contents
---------------

.. py:class:: TriggerRuleDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Determines if a task's upstream tasks are in a state that allows a given task instance
   to run.

   .. attribute:: NAME
      :annotation: = Trigger Rule

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   .. attribute:: IS_TASK_DEP
      :annotation: = True

      

   
   .. staticmethod:: _get_states_count_upstream_ti(ti, finished_tasks)

      This function returns the states of the upstream tis for a specific ti in order to determine
      whether this ti can run in this iteration

      :param ti: the ti that we want to calculate deps for
      :type ti: airflow.models.TaskInstance
      :param finished_tasks: all the finished tasks of the dag_run
      :type finished_tasks: list[airflow.models.TaskInstance]



   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)



   
   .. method:: _evaluate_trigger_rule(self, ti, successes, skipped, failed, upstream_failed, done, flag_upstream_failed, session)

      Yields a dependency status that indicate whether the given task instance's trigger
      rule was met.

      :param ti: the task instance to evaluate the trigger rule of
      :type ti: airflow.models.TaskInstance
      :param successes: Number of successful upstream tasks
      :type successes: int
      :param skipped: Number of skipped upstream tasks
      :type skipped: int
      :param failed: Number of failed upstream tasks
      :type failed: int
      :param upstream_failed: Number of upstream_failed upstream tasks
      :type upstream_failed: int
      :param done: Number of completed upstream tasks
      :type done: int
      :param flag_upstream_failed: This is a hack to generate
          the upstream_failed state creation while checking to see
          whether the task instance is runnable. It was the shortest
          path to add the feature
      :type flag_upstream_failed: bool
      :param session: database session
      :type session: sqlalchemy.orm.session.Session




