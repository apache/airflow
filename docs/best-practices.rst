
Testing Operators with pytest
-----------------------------

The ``dag.test()`` example in the docs works when executed inside a DAG file,
but it may not work when used directly inside pytest because DAG serialization
is not active in standalone test files.

Here are two recommended patterns for testing operators in pytest:

1. Using ``TaskInstance.run()``
2. Using ``dag.create_dagrun()``

Both examples below are runnable and compatible with Airflow 3.x.

