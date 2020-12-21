:mod:`airflow.operators.dummy_operator`
=======================================

.. py:module:: airflow.operators.dummy_operator


Module Contents
---------------

.. py:class:: DummyOperator(**kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Operator that does literally nothing. It can be used to group tasks in a
   DAG.

   The task is evaluated by the scheduler but never processed by the executor.

   .. attribute:: ui_color
      :annotation: = #e8f7e4

      

   
   .. method:: execute(self, context)




