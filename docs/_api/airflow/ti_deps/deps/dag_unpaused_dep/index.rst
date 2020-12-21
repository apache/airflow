:mod:`airflow.ti_deps.deps.dag_unpaused_dep`
============================================

.. py:module:: airflow.ti_deps.deps.dag_unpaused_dep


Module Contents
---------------

.. py:class:: DagUnpausedDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Determines whether a task's DAG is not paused.

   .. attribute:: NAME
      :annotation: = Dag Not Paused

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




