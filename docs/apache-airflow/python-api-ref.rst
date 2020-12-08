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
  are derived from :class:`~airflow.sensors.base.BaseSensorOperator` and run a poke
  method at a specified :attr:`~airflow.sensors.base.BaseSensorOperator.poke_interval` until it returns ``True``.

BaseOperator
''''''''''''
All operators are derived from :class:`~airflow.models.BaseOperator` and acquire much
functionality through inheritance. Since this is the core of the engine,
it's worth taking the time to understand the parameters of :class:`~airflow.models.BaseOperator`
to understand the primitive features that can be leveraged in your
DAGs.

BaseSensorOperator
''''''''''''''''''
All sensors are derived from :class:`~airflow.sensors.base.BaseSensorOperator`. All sensors inherit
the :attr:`~airflow.sensors.base.BaseSensorOperator.timeout` and :attr:`~airflow.sensors.base.BaseSensorOperator.poke_interval` on top of the :class:`~airflow.models.BaseOperator`
attributes.

Operators packages
''''''''''''''''''
All operators are in the following packages:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/operators/index

  _api/airflow/sensors/index


.. _pythonapi:hooks:

Hooks
-----
Hooks are interfaces to external platforms and databases, implementing a common
interface when possible and acting as building blocks for operators. All hooks
are derived from :class:`~airflow.hooks.base.BaseHook`.

Hooks packages
''''''''''''''
All hooks are in the following packages:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/hooks/index

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

  _api/airflow/executors/index

Models
------
Models are built on top of the SQLAlchemy ORM Base class, and instances are
persisted in the database.

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/models/index

.. _pythonapi:exceptions:

Exceptions
----------

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/exceptions/index

Secrets Backends
----------------
Airflow relies on secrets backends to retrieve :class:`~airflow.models.connection.Connection` objects.
All secrets backends derive from :class:`~airflow.secrets.BaseSecretsBackend`.

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/secrets/index
