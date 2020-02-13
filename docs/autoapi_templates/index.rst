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



Python API Reference
====================

.. _pythonapi:operators:

Operators
---------
Operators allow for generation of certain types of tasks that become nodes in
the DAG when instantiated. All operators derive from :class:`~airflow.models.BaseOperator` and
inherit many attributes and methods that way.

There are 3 main types of operators:

- Operators that performs an **action**, or tell another system to
  perform an action
- **Transfer** operators move data from one system to another
- **Sensors** are a certain type of operator that will keep running until a
  certain criterion is met. Examples include a specific file landing in HDFS or
  S3, a partition appearing in Hive, or a specific time of the day. Sensors
  are derived from :class:`~airflow.sensors.base_sensor_operator.BaseSensorOperator` and run a poke
  method at a specified :attr:`~airflow.sensors.base_sensor_operator.BaseSensorOperator.poke_interval` until it returns ``True``.

BaseOperator
''''''''''''
All operators are derived from :class:`~airflow.models.BaseOperator` and acquire much
functionality through inheritance. Since this is the core of the engine,
it's worth taking the time to understand the parameters of :class:`~airflow.models.BaseOperator`
to understand the primitive features that can be leveraged in your
DAGs.

BaseSensorOperator
''''''''''''''''''
All sensors are derived from :class:`~airflow.sensors.base_sensor_operator.BaseSensorOperator`. All sensors inherit
the :attr:`~airflow.sensors.base_sensor_operator.BaseSensorOperator.timeout` and :attr:`~airflow.sensors.base_sensor_operator.BaseSensorOperator.poke_interval` on top of the :class:`~airflow.models.BaseOperator`
attributes.

Operators packages
''''''''''''''''''
All operators are in the following packages:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  airflow/operators/index

  airflow/sensors/index

  airflow/providers/amazon/aws/operators/index

  airflow/providers/amazon/aws/sensors/index

  airflow/providers/apache/cassandra/sensors/index

  airflow/providers/apache/druid/operators/index

  airflow/providers/apache/hdfs/sensors/index

  airflow/providers/apache/hive/operators/index

  airflow/providers/apache/hive/sensors/index

  airflow/providers/apache/livy/operators/index

  airflow/providers/apache/livy/sensors/index

  airflow/providers/apache/pig/operators/index

  airflow/providers/apache/spark/operators/index

  airflow/providers/apache/sqoop/operators/index

  airflow/providers/celery/sensors/index

  airflow/providers/cncf/kubernetes/operators/index

  airflow/providers/databricks/operators/index

  airflow/providers/datadog/sensors/index

  airflow/providers/dingding/operators/index

  airflow/providers/discord/operators/index

  airflow/providers/docker/operators/index

  airflow/providers/email/operators/index

  airflow/providers/ftp/sensors/index

  airflow/providers/google/cloud/operators/index

  airflow/providers/google/cloud/sensors/index

  airflow/providers/google/marketing_platform/operators/index

  airflow/providers/google/marketing_platform/sensors/index

  airflow/providers/google/suite/operators/index

  airflow/providers/grpc/operators/index

  airflow/providers/http/operators/index

  airflow/providers/http/sensors/index

  airflow/providers/imap/sensors/index

  airflow/providers/jdbc/operators/index

  airflow/providers/jenkins/operators/index

  airflow/providers/jira/operators/index

  airflow/providers/jira/sensors/index

  airflow/providers/microsoft/azure/operators/index

  airflow/providers/microsoft/azure/sensors/index

  airflow/providers/microsoft/mssql/operators/index

  airflow/providers/microsoft/winrm/operators/index

  airflow/providers/mongo/sensors/index

  airflow/providers/mysql/operators/index

  airflow/providers/opsgenie/operators/index

  airflow/providers/oracle/operators/index

  airflow/providers/papermill/operators/index

  airflow/providers/postgres/operators/index

  airflow/providers/presto/operators/index

  airflow/providers/qubole/operators/index

  airflow/providers/qubole/sensors/index

  airflow/providers/redis/operators/index

  airflow/providers/redis/sensors/index

  airflow/providers/salesforce/operators/index

  airflow/providers/salesforce/sensors/index

  airflow/providers/segment/operators/index

  airflow/providers/sftp/operators/index

  airflow/providers/sftp/sensors/index

  airflow/providers/slack/operators/index

  airflow/providers/snowflake/operators/index

  airflow/providers/sqlite/operators/index

  airflow/providers/ssh/operators/index

  airflow/providers/vertica/operators/index

  airflow/providers/yandex/operators/index

.. _pythonapi:hooks:

Hooks
-----
Hooks are interfaces to external platforms and databases, implementing a common
interface when possible and acting as building blocks for operators. All hooks
are derived from :class:`~airflow.hooks.base_hook.BaseHook`.

Hooks packages
''''''''''''''
All hooks are in the following packages:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  airflow/hooks/index

  airflow/providers/amazon/aws/hooks/index

  airflow/providers/apache/cassandra/hooks/index

  airflow/providers/apache/druid/hooks/index

  airflow/providers/apache/hdfs/hooks/index

  airflow/providers/apache/hive/hooks/index

  airflow/providers/apache/livy/hooks/index

  airflow/providers/apache/pig/hooks/index

  airflow/providers/apache/pinot/hooks/index

  airflow/providers/apache/spark/hooks/index

  airflow/providers/apache/sqoop/hooks/index

  airflow/providers/cloudant/hooks/index

  airflow/providers/databricks/hooks/index

  airflow/providers/datadog/hooks/index

  airflow/providers/discord/hooks/index

  airflow/providers/dingding/hooks/index

  airflow/providers/docker/hooks/index

  airflow/providers/elasticsearch/hooks/index

  airflow/providers/ftp/hooks/index

  airflow/providers/google/cloud/hooks/index

  airflow/providers/google/marketing_platform/hooks/index

  airflow/providers/google/suite/hooks/index

  airflow/providers/grpc/hooks/index

  airflow/providers/http/hooks/index

  airflow/providers/imap/hooks/index

  airflow/providers/jdbc/hooks/index

  airflow/providers/jenkins/hooks/index

  airflow/providers/jira/hooks/index

  airflow/providers/microsoft/azure/hooks/index

  airflow/providers/microsoft/mssql/hooks/index

  airflow/providers/microsoft/winrm/hooks/index

  airflow/providers/mongo/hooks/index

  airflow/providers/mysql/hooks/index

  airflow/providers/odbc/hooks/index

  airflow/providers/openfass/hooks/index

  airflow/providers/opsgenie/hooks/index

  airflow/providers/oracle/hooks/index

  airflow/providers/pagerduty/hooks/index

  airflow/providers/postgres/hooks/index

  airflow/providers/presto/hooks/index

  airflow/providers/qubole/hooks/index

  airflow/providers/redis/hooks/index

  airflow/providers/salesforce/hooks/index

  airflow/providers/samba/hooks/index

  airflow/providers/segment/hooks/index

  airflow/providers/sftp/hooks/index

  airflow/providers/slack/hooks/index

  airflow/providers/snowflake/hooks/index

  airflow/providers/sqlite/hooks/index

  airflow/providers/ssh/hooks/index

  airflow/providers/vertica/hooks/index

  airflow/providers/zendesk/hooks/index

  airflow/providers/yandex/hooks/index

Executors
---------
Executors are the mechanism by which task instances get run. All executors are
derived from :class:`~airflow.executors.base_executor.BaseExecutor`.

Executors packages
''''''''''''''''''
All executors are in the following packages:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  airflow/executors/index

Models
------
Models are built on top of the SQLAlchemy ORM Base class, and instances are
persisted in the database.

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  airflow/models/index
