:mod:`airflow.ti_deps.deps.dagrun_exists_dep`
=============================================

.. py:module:: airflow.ti_deps.deps.dagrun_exists_dep


Module Contents
---------------

.. py:class:: DagrunRunningDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Determines whether a task's DagRun is in valid state.

   .. attribute:: NAME
      :annotation: = Dagrun Running

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




