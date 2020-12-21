:mod:`airflow.operators.latest_only`
====================================

.. py:module:: airflow.operators.latest_only

.. autoapi-nested-parse::

   This module contains an operator to run downstream tasks only for the
   latest scheduled DagRun



Module Contents
---------------

.. py:class:: LatestOnlyOperator

   Bases: :class:`airflow.operators.branch_operator.BaseBranchOperator`

   Allows a workflow to skip tasks that are not running during the most
   recent schedule interval.

   If the task is run outside of the latest schedule interval (i.e. external_trigger),
   all directly downstream tasks will be skipped.

   Note that downstream tasks are never skipped if the given DAG_Run is
   marked as externally triggered.

   .. attribute:: ui_color
      :annotation: = #e9ffdb

      

   
   .. method:: choose_branch(self, context: Dict)




