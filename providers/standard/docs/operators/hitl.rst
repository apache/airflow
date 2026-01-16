HITL Operators
==============

Human-in-the-Loop (HITL) operators allow workflows to pause and wait for
human input or approval before continuing execution. These operators are
useful for approval gates, validations, and manual decision points in DAGs.

Available Operators
-------------------

* ``airflow.providers.standard.operators.hitl.HitLTriggerOperator``
* ``airflow.providers.standard.operators.hitl.HitLResponseOperator``


For detailed parameter documentation, see the
`Python API reference <https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/hitl/index.html>`_.

Usage examples and conceptual background are available in the
`HITL tutorial <https://airflow.apache.org/docs/apache-airflow/stable/tutorial/hitl.html>`_.

     