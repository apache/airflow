Initializing a Database Backend
===============================

If you want to take a real test drive of Airflow, you should consider
setting up a real database backend and switching to the LocalExecutor.

As Airflow was built to interact with its metadata using the great SqlAlchemy
library, you should be able to use any database backend supported as a
SqlAlchemy backend. We recommend using **MySQL** or **Postgres**.

.. note:: We rely on more strict ANSI SQL settings for MySQL in order to have
   sane defaults. Make sure to have specified `explicit_defaults_for_timestamp=1`
   in your my.cnf under `[mysqld]`

.. note:: If you decide to use **Postgres**, we recommend using the ``psycopg2``
   driver and specifying it in your SqlAlchemy connection string.
   Also note that since SqlAlchemy does not expose a way to target a
   specific schema in the Postgres connection URI, you may
   want to set a default schema for your role with a
   command similar to ``ALTER ROLE username SET search_path = airflow, foobar;``

Once you've setup your database to host Airflow, you'll need to alter the
SqlAlchemy connection string located in your configuration file
``$AIRFLOW_HOME/airflow.cfg``. You should then also change the "executor"
setting to use "LocalExecutor", an executor that can parallelize task
instances locally.

.. code-block:: bash

    # initialize the database
    airflow initdb
