:mod:`airflow.ti_deps.deps.ready_to_reschedule`
===============================================

.. py:module:: airflow.ti_deps.deps.ready_to_reschedule


Module Contents
---------------

.. py:class:: ReadyToRescheduleDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Determines whether a task is ready to be rescheduled.

   .. attribute:: NAME
      :annotation: = Ready To Reschedule

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   .. attribute:: IS_TASK_DEP
      :annotation: = True

      

   .. attribute:: RESCHEDULEABLE_STATES
      

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)

      Determines whether a task is ready to be rescheduled. Only tasks in
      NONE state with at least one row in task_reschedule table are
      handled by this dependency class, otherwise this dependency is
      considered as passed. This dependency fails if the latest reschedule
      request's reschedule date is still in future.




