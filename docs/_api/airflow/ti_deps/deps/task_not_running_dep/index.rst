:mod:`airflow.ti_deps.deps.task_not_running_dep`
================================================

.. py:module:: airflow.ti_deps.deps.task_not_running_dep

.. autoapi-nested-parse::

   Contains the TaskNotRunningDep.



Module Contents
---------------

.. py:class:: TaskNotRunningDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Ensures that the task instance's state is not running.

   .. attribute:: NAME
      :annotation: = Task Instance Not Running

      

   .. attribute:: IGNOREABLE
      :annotation: = False

      

   
   .. method:: __eq__(self, other)



   
   .. method:: __hash__(self)



   
   .. method:: _get_dep_statuses(self, ti, session, dep_context=None)




