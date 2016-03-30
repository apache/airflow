Quick Start
-----------

The installation is quick and straightforward.

.. code-block:: bash

    # airflow needs a home, ~/airflow is the default,
    # but you can lay foundation somewhere else if you prefer
    # (optional)
    export AIRFLOW_HOME=~/airflow

    # install from pypi using pip
    pip install airflow

    # initialize the database
    airflow initdb

    # start the web server, default port is 8080
    airflow webserver -p 8080

Upon running these commands, Airflow will create the ``$AIRFLOW_HOME`` folder
and lay an "airflow.cfg" file with defaults that get you going fast. You can
inspect the file either in ``$AIRFLOW_HOME/airflow.cfg``, or through the UI in
the ``Admin->Configuration`` menu.

Out of the box, Airflow uses a sqlite database, which you should outgrow
fairly quickly since no parallelization is possible using this database
backend. It works in conjunction with the ``SequentialExecutor`` which will
only run task instances sequentially. While this is very limiting, it allows
you to get up and running quickly and take a tour of the UI and the
command line utilities.

Here are a few commands that will trigger a few task instances. You should
be able to see the status of the jobs change in the ``example1`` DAG as you
run the commands below.

.. code-block:: bash

    # run your first task instance
    airflow run example_bash_operator runme_0 2015-01-01
    # run a backfill over 2 days
    airflow backfill example_bash_operator -s 2015-01-01 -e 2015-01-02

What's Next?
''''''''''''
From this point, you can head to the :doc:`tutorial` section for further examples or the :doc:`configuation` section if you're ready to get your hands dirty.
