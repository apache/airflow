:mod:`airflow.ti_deps.deps.prev_dagrun_dep`
===========================================

.. py:module:: airflow.ti_deps.deps.prev_dagrun_dep


Module Contents
---------------

.. py:class:: PrevDagrunDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Is the past dagrun in a state that allows this task instance to run, e.g. did this
   task instance's task in the previous dagrun complete if we are depending on past.

   .. attribute:: NAME
      :annotation: = Previous Dagrun State

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   .. attribute:: IS_TASK_DEP
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




