Installation
------------

Getting Airflow
'''''''''''''''

The easiest way to install the latest stable version of Airflow is with ``pip``:

.. code-block:: bash

    pip install apache-airflow

You can also install Airflow with support for extra features like ``s3`` or ``postgres``:

.. code-block:: bash

    pip install apache-airflow[postgres,s3]

.. note:: GPL dependency

    One of the dependencies of Apache Airflow by default pulls in a GPL library ('unidecode').
    
    If you are not concerned about the GPL dependency, export the following environment variable prior to installing airflow: ``export AIRFLOW_GPL_UNIDECODE=yes``.
    
    In case this is a concern you can force a non GPL library by issuing
    ``export SLUGIFY_USES_TEXT_UNIDECODE=yes`` and then proceed with the normal installation.
    Please note that this needs to be specified at every upgrade. Also note that if `unidecode`
    is already present on the system the dependency will still be used.

Extra Packages
''''''''''''''

The ``apache-airflow`` PyPI basic package only installs what's needed to get started.
Subpackages can be installed depending on what will be useful in your
environment. For instance, if you don't need connectivity with Postgres,
you won't have to go through the trouble of installing the ``postgres-devel``
yum package, or whatever equivalent applies on the distribution you are using.

Behind the scenes, Airflow does conditional imports of operators that require
these extra dependencies.

Here's the list of the subpackages and what they enable:

+---------------------+---------------------------------------------------+-------------------------------------------------+
| subpackage          | install command                                   | enables                                         |
+=====================+===================================================+=================================================+
| all                 | ``pip install apache-airflow[all]``               | All Airflow features known to man               |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| all_dbs             | ``pip install apache-airflow[all_dbs]``           | All databases integrations                      |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| async               | ``pip install apache-airflow[async]``             | Async worker classes for Gunicorn               |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| celery              | ``pip install apache-airflow[celery]``            | CeleryExecutor                                  |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| cloudant            | ``pip install apache-airflow[cloudant]``          | Cloudant hook                                   |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| crypto              | ``pip install apache-airflow[crypto]``            | Encrypt connection passwords in metadata db     |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| devel               | ``pip install apache-airflow[devel]``             | Minimum dev tools requirements                  |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| devel_hadoop        | ``pip install apache-airflow[devel_hadoop]``      | Airflow + dependencies on the Hadoop stack      |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| druid               | ``pip install apache-airflow[druid]``             | Druid related operators & hooks                 |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| gcp_api             | ``pip install apache-airflow[gcp_api]``           | Google Cloud Platform hooks and operators       |
|                     |                                                   | (using ``google-api-python-client``)            |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| github_enterprise   | ``pip install apache-airflow[github_enterprise]`` | Github Enterprise auth backend                  |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| google_auth         | ``pip install apache-airflow[google_auth]``       | Google auth backend                             |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| hdfs                | ``pip install apache-airflow[hdfs]``              | HDFS hooks and operators                        |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| hive                | ``pip install apache-airflow[hive]``              | All Hive related operators                      |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| jdbc                | ``pip install apache-airflow[jdbc]``              | JDBC hooks and operators                        |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| kerberos            | ``pip install apache-airflow[kerberos]``          | Kerberos integration for Kerberized Hadoop      |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| ldap                | ``pip install apache-airflow[ldap]``              | LDAP authentication for users                   |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| mssql               | ``pip install apache-airflow[mssql]``             | Microsoft SQL Server operators and hook,        |
|                     |                                                   | support as an Airflow backend                   |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| mysql               | ``pip install apache-airflow[mysql]``             | MySQL operators and hook, support as an Airflow |
|                     |                                                   | backend. The version of MySQL server has to be  |
|                     |                                                   | 5.6.4+. The exact version upper bound depends   |
|                     |                                                   | on version of ``mysqlclient`` package. For      |
|                     |                                                   | example, ``mysqlclient`` 1.3.12 can only be     |
|                     |                                                   | used with MySQL server 5.6.4 through 5.7.       |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| password            | ``pip install apache-airflow[password]``          | Password authentication for users               |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| postgres            | ``pip install apache-airflow[postgres]``          | PostgreSQL operators and hook, support as an    |
|                     |                                                   | Airflow backend                                 |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| qds                 | ``pip install apache-airflow[qds]``               | Enable QDS (Qubole Data Service) support        |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| rabbitmq            | ``pip install apache-airflow[rabbitmq]``          | RabbitMQ support as a Celery backend            |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| redis               | ``pip install apache-airflow[redis]``             | Redis hooks and sensors                         |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| s3                  | ``pip install apache-airflow[s3]``                | ``S3KeySensor``, ``S3PrefixSensor``             |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| samba               | ``pip install apache-airflow[samba]``             | ``Hive2SambaOperator``                          |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| slack               | ``pip install apache-airflow[slack]``             | ``SlackAPIPostOperator``                        |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| ssh                 | ``pip install apache-airflow[ssh]``               | SSH hooks and Operator                          |
+---------------------+---------------------------------------------------+-------------------------------------------------+
| vertica             | ``pip install apache-airflow[vertica]``           | Vertica hook support as an Airflow backend      |
+---------------------+---------------------------------------------------+-------------------------------------------------+

Initiating Airflow Database
'''''''''''''''''''''''''''

Airflow requires a database to be initiated before you can run tasks. If
you're just experimenting and learning Airflow, you can stick with the
default SQLite option. If you don't want to use SQLite, then take a look at
:doc:`howto/initialize-database` to setup a different database.

After configuration, you'll need to initialize the database before you can
run tasks:

.. code-block:: bash

    airflow initdb
