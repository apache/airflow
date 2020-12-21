:mod:`airflow.ti_deps.deps.not_previously_skipped_dep`
======================================================

.. py:module:: airflow.ti_deps.deps.not_previously_skipped_dep


Module Contents
---------------

.. py:class:: NotPreviouslySkippedDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Determines if any of the task's direct upstream relatives have decided this task should
   be skipped.

   .. attribute:: NAME
      :annotation: = Not Previously Skipped

      

   .. attribute:: IGNORABLE
      :annotation: = True

      

   .. attribute:: IS_TASK_DEP
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




