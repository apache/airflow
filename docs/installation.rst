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

Airflow is published as ``apache-airflow`` package in PyPI. Installing it however might be sometimes tricky
because Airflow is a bit of both a library and application. Libraries usually keep their dependencies open and
applications usually pin them, but we should do neither and both at the same time. We decided to keep
our dependencies as open as possible (in ``setup.py``) so users can install different version of libraries
if needed. This means that from time to time plain ``pip install apache-airflow`` will not work or will
produce unusable Airflow installation.

In order to have repeatable installation, however, starting from **Airflow 1.10.10** we also keep a set of
"known-to-be-working" requirement files in the ``requirements`` folder. Those "known-to-be-working"
requirements are per major/minor python version (3.6/3.7). You can use them as constraint
files when installing Airflow from PyPI. Note that you have to specify correct Airflow version
and python versions in the URL.

1. Installing just airflow

.. code-block:: bash

    pip install \
     apache-airflow==1.10.10 \
     --constraint \
            https://raw.githubusercontent.com/apache/airflow/1.10.10/requirements/requirements-python3.7.txt


You need certain system level requirements in order to install Airflow. Those are requirements that are known
to be needed for Linux system (Tested on Ubuntu Buster LTS) :

2. Installing with extras (for example postgres, gcp)

.. code-block:: bash

    pip install \
     apache-airflow[postgres,gcp]==1.10.10 \
     --constraint \
            https://raw.githubusercontent.com/apache/airflow/1.10.10/requirements/requirements-python3.7.txt


You need certain system level requirements in order to install Airflow. Those are requirements that are known
to be needed for Linux system (Tested on Ubuntu Buster LTS) :

.. code-block:: bash

   sudo apt-get install -y --no-install-recommends \
           freetds-bin \
           krb5-user \
           ldap-utils \
           libffi6 \
           libsasl2-2 \
           libsasl2-modules \
           libssl1.1 \
           locales  \
           lsb-release \
           sasl2-bin \
           sqlite3 \
           unixodbc

You also need database client packages (Postgres or MySQL) if you want to use those databases.

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

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                              |
+=====================+=====================================================+======================================================================+
| all                 | ``pip install 'apache-airflow[all]'``               | All Airflow features known to man                                    |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| all_dbs             | ``pip install 'apache-airflow[all_dbs]'``           | All databases integrations                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| async               | ``pip install 'apache-airflow[async]'``             | Async worker classes for Gunicorn                                    |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| aws                 | ``pip install 'apache-airflow[aws]'``               | Amazon Web Services                                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| azure               | ``pip install 'apache-airflow[azure]'``             | Microsoft Azure                                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| celery              | ``pip install 'apache-airflow[celery]'``            | CeleryExecutor                                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| cloudant            | ``pip install 'apache-airflow[cloudant]'``          | Cloudant hook                                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| crypto              | ``pip install 'apache-airflow[crypto]'``            | Encrypt connection passwords in metadata db                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| devel               | ``pip install 'apache-airflow[devel]'``             | Minimum dev tools requirements                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| devel_hadoop        | ``pip install 'apache-airflow[devel_hadoop]'``      | Airflow + dependencies on the Hadoop stack                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| druid               | ``pip install 'apache-airflow[druid]'``             | Druid related operators & hooks                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| gcp                 | ``pip install 'apache-airflow[gcp]'``               | Google Cloud Platform                                                |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| github_enterprise   | ``pip install 'apache-airflow[github_enterprise]'`` | GitHub Enterprise auth backend                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| google_auth         | ``pip install 'apache-airflow[google_auth]'``       | Google auth backend                                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| hashicorp           | ``pip install 'apache-airflow[hashicorp]'``         | Hashicorp Services (Vault)                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| hdfs                | ``pip install 'apache-airflow[hdfs]'``              | HDFS hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| hive                | ``pip install 'apache-airflow[hive]'``              | All Hive related operators                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| jdbc                | ``pip install 'apache-airflow[jdbc]'``              | JDBC hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| kerberos            | ``pip install 'apache-airflow[kerberos]'``          | Kerberos integration for Kerberized Hadoop                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| kubernetes          | ``pip install 'apache-airflow[kubernetes]'``        | Kubernetes Executor and operator                                     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| ldap                | ``pip install 'apache-airflow[ldap]'``              | LDAP authentication for users                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| mssql               | ``pip install 'apache-airflow[mssql]'``             | Microsoft SQL Server operators and hook,                             |
|                     |                                                     | support as an Airflow backend                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| mysql               | ``pip install 'apache-airflow[mysql]'``             | MySQL operators and hook, support as an Airflow                      |
|                     |                                                     | backend. The version of MySQL server has to be                       |
|                     |                                                     | 5.6.4+. The exact version upper bound depends                        |
|                     |                                                     | on version of ``mysqlclient`` package. For                           |
|                     |                                                     | example, ``mysqlclient`` 1.3.12 can only be                          |
|                     |                                                     | used with MySQL server 5.6.4 through 5.7.                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| oracle              | ``pip install 'apache-airflow[oracle]'``            | Oracle hooks and operators                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| password            | ``pip install 'apache-airflow[password]'``          | Password authentication for users                                    |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| postgres            | ``pip install 'apache-airflow[postgres]'``          | PostgreSQL operators and hook, support as an                         |
|                     |                                                     | Airflow backend                                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| presto              | ``pip install 'apache-airflow[presto]'``            | All Presto related operators & hooks                                 |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| qds                 | ``pip install 'apache-airflow[qds]'``               | Enable QDS (Qubole Data Service) support                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| rabbitmq            | ``pip install 'apache-airflow[rabbitmq]'``          | RabbitMQ support as a Celery backend                                 |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| redis               | ``pip install 'apache-airflow[redis]'``             | Redis hooks and sensors                                              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| samba               | ``pip install apache-airflow[samba]'``              | :class:`airflow.operators.hive_to_samba_operator.Hive2SambaOperator` |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| slack               | ``pip install 'apache-airflow[slack']``             | :class:`airflow.operators.slack_operator.SlackAPIOperator`           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| ssh                 | ``pip install 'apache-airflow[ssh]'``               | SSH hooks and Operator                                               |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| vertica             | ``pip install 'apache-airflow[vertica]'``           | Vertica hook support as an Airflow backend                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+

Initializing Airflow Database
'''''''''''''''''''''''''''''

Airflow requires a database to be initialized before you can run tasks. If
you're just experimenting and learning Airflow, you can stick with the
default SQLite option. If you don't want to use SQLite, then take a look at
:doc:`howto/initialize-database` to setup a different database.

After configuration, you'll need to initialize the database before you can
run tasks:

.. code-block:: bash

    airflow initdb
