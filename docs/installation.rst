Installation
------------
Setting up the sandbox from the :doc:`start` section was easy, now
working towards a production grade environment is a bit more work.

As of August 2015, Airflow has experimental support for Python 3. Any issues should be reported (or fixed!).
The only major regression is that ``HDFSHooks`` do not work (due to a ``snakebite`` dependency)


Extra Packages
''''''''''''''
The ``airflow`` PyPI basic package only installs what's needed to get started.
Subpackages can be installed depending on what will be useful in your
environment. For instance, if you don't need connectivity with Postgres,
you won't have to go through the trouble of installing the ``postgres-devel``
yum package, or whatever equivalent applies on the distribution you are using.

Behind the scenes, we do conditional imports on operators that require
these extra dependencies.

Here's the list of the subpackages and what they enable:

+-------------+------------------------------------+------------------------------------------------+
| subpackage  |     install command                | enables                                        |
+=============+====================================+================================================+
|  mysql      |  ``pip install airflow[mysql]``    | MySQL operators and hook, support as           |
|             |                                    | an Airflow backend                             |
+-------------+------------------------------------+------------------------------------------------+
|  postgres   |  ``pip install airflow[postgres]`` | Postgres operators and hook, support           |
|             |                                    | as an Airflow backend                          |
+-------------+------------------------------------+------------------------------------------------+
|  samba      |  ``pip install airflow[samba]``    | ``Hive2SambaOperator``                         |
+-------------+------------------------------------+------------------------------------------------+
|  hive       |  ``pip install airflow[hive]``     | All Hive related operators                     |
+-------------+------------------------------------+------------------------------------------------+
|  jdbc       |  ``pip install airflow[jdbc]``     | JDBC hooks and operators                       |
+-------------+------------------------------------+------------------------------------------------+
|  hdfs       |  ``pip install airflow[hdfs]``     | HDFS hooks and operators                       |
+-------------+------------------------------------+------------------------------------------------+
|  s3         | ``pip install airflow[s3]``        | ``S3KeySensor``, ``S3PrefixSensor``            |
+-------------+------------------------------------+------------------------------------------------+
|  druid      | ``pip install airflow[druid]``     | Druid.io related operators & hooks             |
+-------------+------------------------------------+------------------------------------------------+
|  mssql      |  ``pip install airflow[mssql]``    | Microsoft SQL operators and hook,              |
|             |                                    | support as an Airflow backend                  |
+-------------+------------------------------------+------------------------------------------------+
|  vertica    |  ``pip install airflow[vertica]``  | Vertica hook                                   |
|             |                                    | support as an Airflow backend                  |
+-------------+------------------------------------+------------------------------------------------+
|  slack      | ``pip install airflow[slack]``     | ``SlackAPIPostOperator``                       |
+-------------+------------------------------------+------------------------------------------------+
|  all        | ``pip install airflow[all]``       | All Airflow features known to man              |
+-------------+------------------------------------+------------------------------------------------+
|  devel      | ``pip install airflow[devel]``     | All Airflow features + useful dev tools        |
+-------------+------------------------------------+------------------------------------------------+
|  crypto     | ``pip install airflow[crypto]``    | Encrypt connection passwords in metadata db    |
+-------------+------------------------------------+------------------------------------------------+
|  celery     | ``pip install airflow[celery]``    | CeleryExecutor                                 |
+-------------+------------------------------------+------------------------------------------------+
|  async      | ``pip install airflow[async]``     | Async worker classes for gunicorn              |
+-------------+------------------------------------+------------------------------------------------+
|  ldap       | ``pip install airflow[ldap]``      | ldap authentication for users                  |
+-------------+------------------------------------+------------------------------------------------+
|  kerberos   | ``pip install airflow[kerberos]``  | kerberos integration for kerberized hadoop     |
+-------------+------------------------------------+------------------------------------------------+
|  password   | ``pip install airflow[password]``  | Password Authentication for users              |
+-------------+------------------------------------+------------------------------------------------+


Configuration
'''''''''''''

The first time you run Airflow, it will create a file called ``airflow.cfg`` in
your ``$AIRFLOW_HOME`` directory (``~/airflow`` by
default). This file contains Airflow's configuration and you
can edit it to change any of the settings. You can also set options with environment variables by using this format:
``$AIRFLOW__{SECTION}__{KEY}`` (note the double underscores).

For example, the
metadata database connection string can either be set in ``airflow.cfg`` like this:

.. code-block:: bash

    [core]
    sql_alchemy_conn = my_conn_string

or by creating a corresponding environment variable:

.. code-block:: bash

    AIRFLOW__CORE__SQL_ALCHEMY_CONN=my_conn_string


Setting up a Backend
''''''''''''''''''''
If you want to take a real test drive of Airflow, you should consider
setting up a real database backend and switching to the LocalExecutor.

As Airflow was built to interact with its metadata using the great SqlAlchemy
library, you should be able to use any database backend supported as a
SqlAlchemy backend. We recommend using **MySQL** or **Postgres**.

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

Connections
'''''''''''
Airflow needs to know how to connect to your environment. Information
such as hostname, port, login and passwords to other systems and services is
handled in the ``Admin->Connection`` section of the UI. The pipeline code you
will author will reference the 'conn_id' of the Connection objects.

.. image:: img/connections.png

By default, Airflow will save the passwords for the connection in plain text
within the metadata database. The ``crypto`` package is highly recommended
during installation. The ``crypto`` package does require that your operating
system have libffi-dev installed.

Connections in Airflow pipelines can be created using environment variables.
The environment variable needs to have a prefix of ``AIRFLOW_CONN_`` for
Airflow with the value in a URI format to use the connection properly. Please
see the :doc:`concepts` documentation for more information on environment
variables and connections.

Scaling Out with Celery
'''''''''''''''''''''''
CeleryExecutor is the way you can scale out the number of workers. For this
to work, you need to setup a Celery backend (**RabbitMQ**, **Redis**, ...) and
change your ``airflow.cfg`` to point the executor parameter to
CeleryExecutor and provide the related Celery settings.

For more information about setting up a Celery broker, refer to the
exhaustive `Celery documentation on the topic <http://docs.celeryproject.org/en/latest/getting-started/brokers/index.html>`_.

To kick off a worker, you need to setup Airflow and kick off the worker
subcommand

.. code-block:: bash

    airflow worker

Your worker should start picking up tasks as soon as they get fired in
its direction.

Note that you can also run "Celery Flower", a web UI built on top of Celery,
to monitor your workers.

Logs
''''
Users can specify a logs folder in ``airflow.cfg``. By default, it is in the ``AIRFLOW_HOME`` directory.

In addition, users can supply an S3 location for storing log backups. If logs are not found in the local filesystem (for example, if a worker is lost or reset), the S3 logs will be displayed in the Airflow UI. Note that logs are only sent to S3 once a task completes (including failure).

.. code-block:: bash

    [core]
    base_log_folder = {AIRFLOW_HOME}/logs
    s3_log_folder = s3://{YOUR S3 LOG PATH}

Scaling Out on Mesos (community contributed)
''''''''''''''''''''''''''''''''''''''''''''
MesosExecutor allows you to schedule airflow tasks on a Mesos cluster.
For this to work, you need a running mesos cluster and perform following
steps -

1. Install airflow on a machine where webserver and scheduler will run,
   let's refer this as Airflow server.
2. On Airflow server, install mesos python eggs from `mesos downloads <http://open.mesosphere.com/downloads/mesos/>`_.
3. On Airflow server, use a database which can be accessed from mesos
   slave machines, for example mysql, and configure in ``airflow.cfg``.
4. Change your ``airflow.cfg`` to point executor parameter to
   MesosExecutor and provide related Mesos settings.
5. On all mesos slaves, install airflow. Copy the ``airflow.cfg`` from
   Airflow server (so that it uses same sql alchemy connection).
6. On all mesos slaves, run

.. code-block:: bash

    airflow serve_logs

for serving logs.

7. On Airflow server, run

.. code-block:: bash

    airflow scheduler -p

to start processing DAGs and scheduling them on mesos. We need -p parameter to pickle the DAGs.

You can now see the airflow framework and corresponding tasks in mesos UI.
The logs for airflow tasks can be seen in airflow UI as usual.

For more information about mesos, refer `mesos documentation <http://mesos.apache.org/documentation/latest/>`_.
For any queries/bugs on MesosExecutor, please contact `@kapil-malik <https://github.com/kapil-malik>`_.

Integration with systemd
''''''''''''''''''''''''
Airflow can integrate with systemd based systems. This makes watching your daemons easy as systemd
can take care restarting a daemon on failure. In the ``scripts/systemd`` directory you can find unit files that
have been tested on Redhat based systems. You can copy those ``/usr/lib/systemd/system``. It is assumed that
Airflow will run under ``airflow:airflow``. If not (or if you are running on a non Redhat based system) you
probably need adjust the unit files.

Environment configuration is picked up from ``/etc/sysconfig/airflow``. An example file is supplied
 . Make sure to specify the ``SCHEDULER_RUNS`` variable in this file when you run the schduler. You
 can also define here, for example, ``AIRFLOW_HOME`` or ``AIRFLOW_CONFIG``.