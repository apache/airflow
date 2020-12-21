:mod:`airflow.ti_deps.deps.valid_state_dep`
===========================================

.. py:module:: airflow.ti_deps.deps.valid_state_dep


Module Contents
---------------

.. py:class:: ValidStateDep(valid_states)

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Ensures that the task instance's state is in a given set of valid states.

   :param valid_states: A list of valid states that a task instance can have to meet
       this dependency.
   :type valid_states: set(str)
   :return: whether or not the task instance's state is valid

   .. attribute:: NAME
      :annotation: = Task Instance State

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   
   .. method:: __eq__(self, other)



   
   .. method:: __hash__(self)



   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)




