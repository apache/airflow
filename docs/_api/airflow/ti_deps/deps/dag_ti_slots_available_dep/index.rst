:mod:`airflow.ti_deps.deps.dag_ti_slots_available_dep`
======================================================

.. py:module:: airflow.ti_deps.deps.dag_ti_slots_available_dep


Module Contents
---------------

.. py:class:: DagTISlotsAvailableDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Determines whether a DAG maximum number of running tasks has been reached.

   .. attribute:: NAME
      :annotation: = Task Instance Slots Available

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




