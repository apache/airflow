:mod:`airflow.ti_deps.deps.task_concurrency_dep`
================================================

.. py:module:: airflow.ti_deps.deps.task_concurrency_dep


Module Contents
---------------

.. py:class:: TaskConcurrencyDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   This restricts the number of running task instances for a particular task.

   .. attribute:: NAME
      :annotation: = Task Concurrency

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   .. attribute:: IS_TASK_DEP
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




