:mod:`airflow.ti_deps.deps.not_in_retry_period_dep`
===================================================

.. py:module:: airflow.ti_deps.deps.not_in_retry_period_dep


Module Contents
---------------

.. py:class:: NotInRetryPeriodDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Determines whether a task is not in retry period.

   .. attribute:: NAME
      :annotation: = Not In Retry Period

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   .. attribute:: IS_TASK_DEP
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




