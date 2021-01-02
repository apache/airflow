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

Extra Packages Reference
''''''''''''''''''''''''

Here's the list of the :ref:`subpackages <installation:extra_packages>` and what they enable.

The entries with ``*`` in the ``Providers`` column indicate that one or more provider will be installed
automatically when those extras are installed. In those cases, there is a dependency between corresponding
provider packages and ``apache-airflow`` package (the provider package depends on ``apache-airflow>=2.0.0``).
For ``provider`` extras - they usually install single ``provider`` package, but for extras that are groups
of other extras (for example ``all`` or ``devel_all`` or ``all_dbs``) there might be more than one provider
installed together with the extra.

The entries with ``*`` in the ``Preinstalled`` column indicate that those extras (with providers) are always
pre-installed when Airflow is installed. In this case dependencies are reverted - the ``apache-airflow``
package depends on the corresponding providers packages. This is in order to avoid circular dependency that
can be reported by some tools (even if it is harmless).

.. note::
  You can disable automated installation of the providers with extras when installing Airflow. You need to
  have ``INSTALL_PROVIDERS_FROM_SOURCES`` environment variable to ``true`` before running ``pip install``
  command. Contributors need to set it, if they are installing Airflow locally, and want to develop
  providers directly via Airflow sources. This variable is automatically set in ``Breeze``
  development environment. Setting this variable is not needed in editable mode (``pip install -e``).

**Fundamentals:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| extra               | install command                                     | enables                                                              | Providers |
+=====================+=====================================================+======================================================================+===========+
| all                 | ``pip install 'apache-airflow[all]'``               | All Airflow user facing features (no devel and doc requirements)     |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| all_dbs             | ``pip install 'apache-airflow[all_dbs]'``           | All databases integrations                                           |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| devel               | ``pip install 'apache-airflow[devel]'``             | Minimum dev tools requirements (without providers)                   |           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| devel_hadoop        | ``pip install 'apache-airflow[devel_hadoop]'``      | Same as ``devel`` + dependencies for developing the Hadoop stack     |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| devel_all           | ``pip install 'apache-airflow[devel_all]'``         | Everything needed for development (``devel_hadoop`` +  providers)    |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| devel_ci            | ``pip install 'apache-airflow[devel_ci]'``          | All dependencies required for CI build.                              |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| doc                 | ``pip install 'apache-airflow[doc]'``               | Packages needed to build docs (included in ``devel``)                |           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| password            | ``pip install 'apache-airflow[password]'``          | Password authentication for users                                    |           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+


**Apache Software:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| extra               | install command                                     | enables                                                              | Providers |
+=====================+=====================================================+======================================================================+===========+
| apache.atlas        | ``pip install 'apache-airflow[apache.atlas]'``      | Apache Atlas to use Data Lineage feature                             |           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.beam         | ``pip install 'apache-airflow[apache.beam]'``       | Apache Beam operators & hooks                                        |           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.cassandra    | ``pip install 'apache-airflow[apache.cassandra]'``  | Cassandra related operators & hooks                                  |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.druid        | ``pip install 'apache-airflow[apache.druid]'``      | Druid related operators & hooks                                      |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.hdfs         | ``pip install 'apache-airflow[apache.hdfs]'``       | HDFS hooks and operators                                             |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.hive         | ``pip install 'apache-airflow[apache.hive]'``       | All Hive related operators                                           |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.kylin        | ``pip install 'apache-airflow[apache.kylin]'``      | All Kylin related operators & hooks                                  |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.livy         | ``pip install 'apache-airflow[apache.livy]'``       | All Livy related operators, hooks & sensors                          |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.pig          | ``pip install 'apache-airflow[apache.pig]'``        | All Pig related operators & hooks                                    |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.pinot        | ``pip install 'apache-airflow[apache.pinot]'``      | All Pinot related hooks                                              |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.spark        | ``pip install 'apache-airflow[apache.spark]'``      | All Spark related operators & hooks                                  |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.sqoop        | ``pip install 'apache-airflow[apache.sqoop]'``      | All Sqoop related operators & hooks                                  |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+
| apache.webhdfs      | ``pip install 'apache-airflow[apache.webhdfs]'``    | HDFS hooks and operators                                             |           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+


**Services:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| extra               | install command                                     | enables                                                                    | Providers |
+=====================+=====================================================+============================================================================+===========+
| amazon              | ``pip install 'apache-airflow[amazon]'``            | Amazon Web Services                                                        |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| azure               | ``pip install 'apache-airflow[microsoft.azure]'``   | Microsoft Azure                                                            |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| cloudant            | ``pip install 'apache-airflow[cloudant]'``          | Cloudant hook                                                              |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| databricks          | ``pip install 'apache-airflow[databricks]'``        | Databricks hooks and operators                                             |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| datadog             | ``pip install 'apache-airflow[datadog]'``           | Datadog hooks and sensors                                                  |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| dingding            | ``pip install 'apache-airflow[dingding]'``          | Dingding hooks and sensors                                                 |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| discord             | ``pip install 'apache-airflow[discord]'``           | Discord hooks and sensors                                                  |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| facebook            | ``pip install 'apache-airflow[facebook]'``          | Facebook Social                                                            |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| google              | ``pip install 'apache-airflow[google]'``            | Google Cloud                                                               |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| github_enterprise   | ``pip install 'apache-airflow[github_enterprise]'`` | GitHub Enterprise auth backend                                             |           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| google_auth         | ``pip install 'apache-airflow[google_auth]'``       | Google auth backend                                                        |           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| hashicorp           | ``pip install 'apache-airflow[hashicorp]'``         | Hashicorp Services (Vault)                                                 |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| jira                | ``pip install 'apache-airflow[jira]'``              | Jira hooks and operators                                                   |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| opsgenie            | ``pip install 'apache-airflow[opsgenie]'``          | OpsGenie hooks and operators                                               |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| pagerduty           | ``pip install 'apache-airflow[pagerduty]'``         | Pagerduty hook                                                             |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| plexus              | ``pip install 'apache-airflow[plexus]'``            | Plexus service of CoreScientific.com AI platform                           |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| qubole              | ``pip install 'apache-airflow[qubole]'``            | Enable QDS (Qubole Data Service) support                                   |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| salesforce          | ``pip install 'apache-airflow[salesforce]'``        | Salesforce hook                                                            |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| sendgrid            | ``pip install 'apache-airflow[sendgrid]'``          | Send email using sendgrid                                                  |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| segment             | ``pip install 'apache-airflow[segment]'``           | Segment hooks and sensors                                                  |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| sentry              | ``pip install 'apache-airflow[sentry]'``            | Sentry service for application logging and monitoring                      |           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| slack               | ``pip install 'apache-airflow[slack]'``             | Slack hooks and operators                                                  |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| snowflake           | ``pip install 'apache-airflow[snowflake]'``         | Snowflake hooks and operators                                              |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| telegram            | ``pip install 'apache-airflow[telegram]'``          | Telegram hooks and operators                                               |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| vertica             | ``pip install 'apache-airflow[vertica]'``           | Vertica hook support as an Airflow backend                                 |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| yandex              | ``pip install 'apache-airflow[yandex]'``            | Yandex.cloud hooks and operators                                           |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+
| zendesk             | ``pip install 'apache-airflow[zendesk]'``           | Zendesk hooks                                                              |     *     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+-----------+


**Software:**

+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| extra               | install command                                     | enables                                                                            | Providers |
+=====================+=====================================================+====================================================================================+===========+
| async               | ``pip install 'apache-airflow[async]'``             | Async worker classes for Gunicorn                                                  |           |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| celery              | ``pip install 'apache-airflow[celery]'``            | CeleryExecutor                                                                     |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| dask                | ``pip install 'apache-airflow[dask]'``              | DaskExecutor                                                                       |           |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| docker              | ``pip install 'apache-airflow[docker]'``            | Docker hooks and operators                                                         |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| elasticsearch       | ``pip install 'apache-airflow[elasticsearch]'``     | Elasticsearch hooks and Log Handler                                                |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| exasol              | ``pip install 'apache-airflow[exasol]'``            | Exasol hooks and operators                                                         |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| jenkins             | ``pip install 'apache-airflow[jenkins]'``           | Jenkins hooks and operators                                                        |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| cncf.kubernetes     | ``pip install 'apache-airflow[cncf.kubernetes]'``   | Kubernetes Executor and operator                                                   |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| mongo               | ``pip install 'apache-airflow[mongo]'``             | Mongo hooks and operators                                                          |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| microsoft.mssql     | ``pip install 'apache-airflow[microsoft.mssql]'``   | Microsoft SQL Server operators and hook.                                           |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| mysql               | ``pip install 'apache-airflow[mysql]'``             | MySQL operators and hook                                                           |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| odbc                | ``pip install 'apache-airflow[odbc]'``              | ODBC data sources including MS SQL Server                                          |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| openfaas            | ``pip install 'apache-airflow[openfaas]'``          | OpenFaaS hooks                                                                     |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| oracle              | ``pip install 'apache-airflow[oracle]'``            | Oracle hooks and operators                                                         |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| postgres            | ``pip install 'apache-airflow[postgres]'``          | PostgreSQL operators and hook                                                      |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| presto              | ``pip install 'apache-airflow[presto]'``            | All Presto related operators & hooks                                               |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| rabbitmq            | ``pip install 'apache-airflow[rabbitmq]'``          | RabbitMQ support as a Celery backend                                               |           |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| redis               | ``pip install 'apache-airflow[redis]'``             | Redis hooks and sensors                                                            |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| samba               | ``pip install 'apache-airflow[samba]'``             | Samba hooks and operators                                                          |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| singularity         | ``pip install 'apache-airflow[singularity]'``       | Singularity container operator                                                     |     *     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| statsd              | ``pip install 'apache-airflow[statsd]'``            | Needed by StatsD metrics                                                           |           |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| tableau             | ``pip install 'apache-airflow[tableau]'``           | Tableau visualization integration                                                  |           |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+
| virtualenv          | ``pip install 'apache-airflow[virtualenv]'``        | Running python tasks in local virtualenv                                           |           |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+-----------+


**Other:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| extra               | install command                                     | enables                                                              | Providers | Preinstalled |
+=====================+=====================================================+======================================================================+===========+==============+
| cgroups             | ``pip install 'apache-airflow[cgroups]'``           | Needed To use CgroupTaskRunner                                       |           |              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| ftp                 | ``pip install 'apache-airflow[ftp]'``               | FTP hooks and operators                                              |     *     |      *       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| grpc                | ``pip install 'apache-airflow[grpc]'``              | Grpc hooks and operators                                             |     *     |              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| http                | ``pip install 'apache-airflow[http]'``              | HTTP hooks, operators and sensors                                    |     *     |      *       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| imap                | ``pip install 'apache-airflow[imap]'``              | IMAP hooks and sensors                                               |     *     |      *       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| jdbc                | ``pip install 'apache-airflow[jdbc]'``              | JDBC hooks and operators                                             |     *     |              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| kerberos            | ``pip install 'apache-airflow[kerberos]'``          | Kerberos integration for Kerberized Hadoop                           |           |              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| ldap                | ``pip install 'apache-airflow[ldap]'``              | LDAP authentication for users                                        |           |              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| papermill           | ``pip install 'apache-airflow[papermill]'``         | Papermill hooks and operators                                        |     *     |              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| sftp                | ``pip install 'apache-airflow[sftp]'``              | SFTP hooks, operators and sensors                                    |     *     |              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| sqlite              | ``pip install 'apache-airflow[sqlite]'``            | SQLite hooks and operators                                           |     *     |      *       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| ssh                 | ``pip install 'apache-airflow[ssh]'``               | SSH hooks and operators                                              |     *     |              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+
| microsoft.winrm     | ``pip install 'apache-airflow[microsoft.winrm]'``   | WinRM hooks and operators                                            |     *     |              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+-----------+--------------+


**Deprecated 1.10 Extras**

Those are the extras that have been deprecated in 2.0 and will be removed in Airflow 3.0.0. They were
all replaced by new extras, which have naming consistent with the names of provider packages.

The ``crypto`` extra is not needed any more, because all crypto dependencies are part of airflow package,
so there is no replacement for ``crypto`` extra.

+---------------------+-----------------------------+
| Deprecated extra    | Extra to be used instead    |
+=====================+=============================+
| atlas               | apache.atlas                |
+---------------------+-----------------------------+
| aws                 | amazon                      |
+---------------------+-----------------------------+
| azure               | microsoft.azure             |
+---------------------+-----------------------------+
| cassandra           | apache.cassandra            |
+---------------------+-----------------------------+
| crypto              |                             |
+---------------------+-----------------------------+
| druid               | apache.druid                |
+---------------------+-----------------------------+
| gcp                 | google                      |
+---------------------+-----------------------------+
| gcp_api             | google                      |
+---------------------+-----------------------------+
| hdfs                | apache.hdfs                 |
+---------------------+-----------------------------+
| hive                | apache.hive                 |
+---------------------+-----------------------------+
| kubernetes          | cncf.kubernetes             |
+---------------------+-----------------------------+
| mssql               | microsoft.mssql             |
+---------------------+-----------------------------+
| pinot               | apache.pinot                |
+---------------------+-----------------------------+
| qds                 | qubole                      |
+---------------------+-----------------------------+
| s3                  | amazon                      |
+---------------------+-----------------------------+
| spark               | apache.spark                |
+---------------------+-----------------------------+
| webhdfs             | apache.webhdfs              |
+---------------------+-----------------------------+
| winrm               | microsoft.winrm             |
+---------------------+-----------------------------+
