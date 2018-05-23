Using the Test Mode Configuration
=================================

Airflow has a fixed set of "test mode" configuration options. You can load these
at any time by calling ``airflow.configuration.load_test_config()`` (note this
operation is not reversible!). However, some options (like the DAG_FOLDER) are
loaded before you have a chance to call load_test_config(). In order to eagerly load
the test configuration, set test_mode in airflow.cfg:

.. code-block:: bash

  [tests]
  unit_test_mode = True

Due to Airflow's automatic environment variable expansion (see :doc:`set-config`),
you can also set the env var ``AIRFLOW__CORE__UNIT_TEST_MODE`` to temporarily overwrite
airflow.cfg.
