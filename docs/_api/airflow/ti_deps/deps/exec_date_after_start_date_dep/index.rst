:mod:`airflow.ti_deps.deps.exec_date_after_start_date_dep`
==========================================================

.. py:module:: airflow.ti_deps.deps.exec_date_after_start_date_dep


Module Contents
---------------

.. py:class:: ExecDateAfterStartDateDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Determines whether a task's execution date is after start date.

   .. attribute:: NAME
      :annotation: = Execution Date

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




