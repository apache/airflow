:mod:`airflow.ti_deps.deps.dagrun_id_dep`
=========================================

.. py:module:: airflow.ti_deps.deps.dagrun_id_dep

.. autoapi-nested-parse::

   This module defines dep for DagRun ID validation



Module Contents
---------------

.. py:class:: DagrunIdDep

   Bases: :class:`airflow.ti_deps.deps.base_ti_dep.BaseTIDep`

   Dep for valid DagRun ID to schedule from scheduler

   .. attribute:: NAME
      :annotation: = Dagrun run_id is not backfill job ID

      

   .. attribute:: IGNOREABLE
      :annotation: = True

      

   
   .. method:: _get_dep_statuses(self, ti, session, dep_context=None)

      Determines if the DagRun ID is valid for scheduling from scheduler.

      :param ti: the task instance to get the dependency status for
      :type ti: airflow.models.TaskInstance
      :param session: database session
      :type session: sqlalchemy.orm.session.Session
      :param dep_context: the context for which this dependency should be evaluated for
      :type dep_context: DepContext
      :return: True if DagRun ID is valid for scheduling from scheduler.




