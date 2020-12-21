:mod:`airflow.ti_deps.deps.runnable_exec_date_dep`
==================================================

.. py:module:: airflow.ti_deps.deps.runnable_exec_date_dep


Module Contents
---------------

.. py:class:: RunnableExecDateDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Determines whether a task's execution date is valid.

   .. attribute:: NAME
      :annotation: = Execution Date

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




