:mod:`airflow.ti_deps.deps.pool_slots_available_dep`
====================================================

.. py:module:: airflow.ti_deps.deps.pool_slots_available_dep

.. autoapi-nested-parse::

   This module defines dep for pool slots availability



Module Contents
---------------

.. py:class:: PoolSlotsAvailableDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Dep for pool slots availability.

   .. attribute:: NAME
      :annotation: = Pool Slots Available

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context=None)

      Determines if the pool task instance is in has available slots

      :param ti: the task instance to get the dependency status for
      :type ti: airflow.models.TaskInstance
      :param session: database session
      :type session: sqlalchemy.orm.session.Session
      :param dep_context: the context for which this dependency should be evaluated for
      :type dep_context: DepContext
      :return: True if there are available slots in the pool.




