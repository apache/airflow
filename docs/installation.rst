 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



Installation
------------

Getting Airflow
'''''''''''''''

The easiest way to install the latest stable version of Airflow is with ``pip``:

.. code-block:: bash

    pip install apache-airflow

You can also install Airflow with support for extra features like ``gcp`` or ``postgres``:

.. code-block:: bash

    pip install 'apache-airflow[postgres,gcp]'

Airflow require that your operating system has ``libffi-dev`` installed.

If the ``airflow`` command is not getting recognized (can happen on Windows when using WSL), then
ensure that ``~/.local/bin`` is in your ``PATH`` environment variable, and add it in if necessary:

.. code-block:: bash

    PATH=$PATH:~/.local/bin

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


**Fundamentals:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                              |
+=====================+=====================================================+======================================================================+
| all                 | ``pip install 'apache-airflow[all]'``               | All Airflow features known to man                                    |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| all_dbs             | ``pip install 'apache-airflow[all_dbs]'``           | All databases integrations                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| crypto              | ``pip install 'apache-airflow[crypto]'``            | Encrypt connection passwords in metadata db                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| devel               | ``pip install 'apache-airflow[devel]'``             | Minimum dev tools requirements                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| devel_hadoop        | ``pip install 'apache-airflow[devel_hadoop]'``      | Airflow + dependencies on the Hadoop stack                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| doc                 | ``pip install 'apache-airflow[doc]'``               | Packages needed to build docs                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| password            | ``pip install 'apache-airflow[password]'``          | Password authentication for users                                    |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+


**Apache Software:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                              |
+=====================+=====================================================+======================================================================+
| atlas               | ``pip install 'apache-airflow[atlas]'``             | Apache Atlas to use Data Lineage feature                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| cassandra           | ``pip install 'apache-airflow[cassandra]'``         | Cassandra related operators & hooks                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| druid               | ``pip install 'apache-airflow[druid]'``             | Druid related operators & hooks                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| hdfs                | ``pip install 'apache-airflow[hdfs]'``              | HDFS hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| hive                | ``pip install 'apache-airflow[hive]'``              | All Hive related operators                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| presto              | ``pip install 'apache-airflow[presto]'``            | All Presto related operators & hooks                                 |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| webhdfs             | ``pip install 'apache-airflow[webhdfs]'``           | HDFS hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+


**Services:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                              |
+=====================+=====================================================+======================================================================+
| aws                 | ``pip install 'apache-airflow[aws]'``               | Amazon Web Services                                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| azure               | ``pip install 'apache-airflow[azure]'``             | Microsoft Azure                                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| cloudant            | ``pip install 'apache-airflow[cloudant]'``          | Cloudant hook                                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| databricks          | ``pip install 'apache-airflow[databricks]'``        | Databricks hooks and operators                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| datadog             | ``pip install 'apache-airflow[datadog]'``           | Datadog hooks and sensors                                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| gcp                 | ``pip install 'apache-airflow[gcp]'``               | Google Cloud Platform                                                |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| github_enterprise   | ``pip install 'apache-airflow[github_enterprise]'`` | GitHub Enterprise auth backend                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| google_auth         | ``pip install 'apache-airflow[google_auth]'``       | Google auth backend                                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| hashicorp           | ``pip install 'apache-airflow[hashicorp]'``         | Hashicorp Services (Vault)                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| jira                | ``pip install 'apache-airflow[jira]'``              | Jira hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| qds                 | ``pip install 'apache-airflow[qds]'``               | Enable QDS (Qubole Data Service) support                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| salesforce          | ``pip install 'apache-airflow[salesforce]'``        | Salesforce hook                                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| sendgrid            | ``pip install 'apache-airflow[sendgrid]'``          | Send email using sendgrid                                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| segment             | ``pip install 'apache-airflow[segment]'``           | Segment hooks and sensors                                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| slack               | ``pip install 'apache-airflow[slack]'``             | :class:`airflow.providers.slack.operators.slack.SlackAPIOperator`    |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| snowflake           | ``pip install 'apache-airflow[snowflake]'``         | Snowflake hooks and operators                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| vertica             | ``pip install 'apache-airflow[vertica]'``           | Vertica hook support as an Airflow backend                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+


**Software:**

+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                                           |
+=====================+=====================================================+===================================================================================+
| async               | ``pip install 'apache-airflow[async]'``             | Async worker classes for Gunicorn                                                 |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| celery              | ``pip install 'apache-airflow[celery]'``            | CeleryExecutor                                                                    |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| dask                | ``pip install 'apache-airflow[dask]'``              | DaskExecutor                                                                      |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| docker              | ``pip install 'apache-airflow[docker]'``            | Docker hooks and operators                                                        |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| elasticsearch       | ``pip install 'apache-airflow[elasticsearch]'``     | Elasticsearch hooks and Log Handler                                               |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| kubernetes          | ``pip install 'apache-airflow[kubernetes]'``        | Kubernetes Executor and operator                                                  |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| mongo               | ``pip install 'apache-airflow[mongo]'``             | Mongo hooks and operators                                                         |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| mssql (deprecated)  | ``pip install 'apache-airflow[mssql]'``             | Microsoft SQL Server operators and hook,                                          |
|                     |                                                     | support as an Airflow backend.  Uses pymssql.                                     |
|                     |                                                     | Will be replaced by subpackage ``odbc``.                                          |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| mysql               | ``pip install 'apache-airflow[mysql]'``             | MySQL operators and hook, support as an Airflow                                   |
|                     |                                                     | backend. The version of MySQL server has to be                                    |
|                     |                                                     | 5.6.4+. The exact version upper bound depends                                     |
|                     |                                                     | on version of ``mysqlclient`` package. For                                        |
|                     |                                                     | example, ``mysqlclient`` 1.3.12 can only be                                       |
|                     |                                                     | used with MySQL server 5.6.4 through 5.7.                                         |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| odbc                | ``pip install 'apache-airflow[odbc]'``              | ODBC data sources including MS SQL Server.  Can use MsSqlOperator,                |
|                     |                                                     | or as metastore database backend.  Uses pyodbc.                                   |
|                     |                                                     | See :ref:`howto/connection/odbc` for more info.                                   |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| oracle              | ``pip install 'apache-airflow[oracle]'``            | Oracle hooks and operators                                                        |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| pinot               | ``pip install 'apache-airflow[pinot]'``             | Pinot DB hook                                                                     |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| postgres            | ``pip install 'apache-airflow[postgres]'``          | PostgreSQL operators and hook, support as an                                      |
|                     |                                                     | Airflow backend                                                                   |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| rabbitmq            | ``pip install 'apache-airflow[rabbitmq]'``          | RabbitMQ support as a Celery backend                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| redis               | ``pip install 'apache-airflow[redis]'``             | Redis hooks and sensors                                                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| samba               | ``pip install 'apache-airflow[samba]'``             | :class:`airflow.providers.apache.hive.operators.hive_to_samba.Hive2SambaOperator` |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+
| statsd              | ``pip install 'apache-airflow[statsd]'``            | Needed by StatsD metrics                                                          |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------------------------+


**Other:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                              |
+=====================+=====================================================+======================================================================+
| cgroups             | ``pip install 'apache-airflow[cgroups]'``           | Needed To use CgroupTaskRunner                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| grpc                | ``pip install 'apache-airflow[grpc]'``              | Grpc hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| jdbc                | ``pip install 'apache-airflow[jdbc]'``              | JDBC hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| kerberos            | ``pip install 'apache-airflow[kerberos]'``          | Kerberos integration for Kerberized Hadoop                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| ldap                | ``pip install 'apache-airflow[ldap]'``              | LDAP authentication for users                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| papermill           | ``pip install 'apache-airflow[papermill]'``         | Papermill hooks and operators                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| ssh                 | ``pip install 'apache-airflow[ssh]'``               | SSH hooks and Operator                                               |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| winrm               | ``pip install 'apache-airflow[winrm]'``             | WinRM hooks and operators                                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+

Initiating Airflow Database
'''''''''''''''''''''''''''

Airflow requires a database to be initiated before you can run tasks. If
you're just experimenting and learning Airflow, you can stick with the
default SQLite option. If you don't want to use SQLite, then take a look at
:doc:`howto/initialize-database` to setup a different database.

After configuration, you'll need to initialize the database before you can
run tasks:

.. code-block:: bash

    airflow db init
