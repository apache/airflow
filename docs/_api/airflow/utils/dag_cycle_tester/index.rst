:mod:`airflow.utils.dag_cycle_tester`
=====================================

.. py:module:: airflow.utils.dag_cycle_tester

.. autoapi-nested-parse::

   DAG Cycle tester



Module Contents
---------------

.. data:: CYCLE_NEW
   :annotation: = 0

   

.. data:: CYCLE_IN_PROGRESS
   :annotation: = 1

   

.. data:: CYCLE_DONE
   :annotation: = 2

   

.. function:: test_cycle(dag)
   Check to see if there are any cycles in the DAG. Returns False if no cycle found,
   otherwise raises exception.


