:mod:`airflow.operators.branch_operator`
========================================

.. py:module:: airflow.operators.branch_operator

.. autoapi-nested-parse::

   Branching operators



Module Contents
---------------

.. py:class:: BaseBranchOperator

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.models.skipmixin.SkipMixin`

   This is a base class for creating operators with branching functionality,
   similarly to BranchPythonOperator.

   Users should subclass this operator and implement the function
   `choose_branch(self, context)`. This should run whatever business logic
   is needed to determine the branch, and return either the task_id for
   a single task (as a str) or a list of task_ids.

   The operator will continue with the returned task_id(s), and all other
   tasks directly downstream of this operator will be skipped.

   
   .. method:: choose_branch(self, context: Dict)

      Subclasses should implement this, running whatever logic is
      necessary to choose a branch and returning a task_id or list of
      task_ids.

      :param context: Context dictionary as passed to execute()
      :type context: dict



   
   .. method:: execute(self, context: Dict)




