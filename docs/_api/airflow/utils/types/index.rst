:mod:`airflow.utils.types`
==========================

.. py:module:: airflow.utils.types


Module Contents
---------------

.. py:class:: DagRunType

   Bases: :class:`str`, :class:`enum.Enum`

   Class with DagRun types

   .. attribute:: BACKFILL_JOB
      :annotation: = backfill

      

   .. attribute:: SCHEDULED
      :annotation: = scheduled

      

   .. attribute:: MANUAL
      :annotation: = manual

      

   
   .. staticmethod:: from_run_id(run_id: str)

      Resolved DagRun type from run_id.




