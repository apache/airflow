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




Smart Sensors
=============

.. warning::

  This is a **deprecated early-access** feature that will be removed in Airflow 2.4.0.
  It is superseded by :doc:`Deferrable Operators <deferring>`, which offer a more flexible way to
  achieve efficient long-running sensors, as well as allowing operators to also achieve similar
  efficiency gains. If you are considering writing a new Smart Sensor, you should instead write it
  as a Deferrable Operator.

The smart sensor is a service (run by a builtin DAG) which greatly reduces Airflow's infrastructure
cost by consolidating multiple instances of small, light-weight Sensors into a single process.

.. image:: /img/smart_sensor_architecture.png

Instead of using one process for each task, the main idea of the smart sensor service is to improve the
efficiency of these long running tasks by using centralized processes to execute those tasks in batches.

To do that, we need to run a task in two steps, the first step is to serialize the task information
into the database; and the second step is to use a few centralized processes to execute the serialized
tasks in batches.

In this way, we only need a handful of running processes.

.. image:: /img/smart_sensor_single_task_execute_flow.png

The smart sensor service is supported in a new mode called "smart sensor mode". In smart sensor mode,
instead of holding a long running process for each sensor and poking periodically, a sensor will only
store poke context at sensor_instance table and then exits with a 'sensing' state.

When the smart sensor mode is enabled, a special set of builtin smart sensor DAGs
(named smart_sensor_group_shard_xxx) is created by the system; These DAGs contain ``SmartSensorOperator``
task and manage the smart sensor jobs for the Airflow cluster. The SmartSensorOperator task can fetch
hundreds of 'sensing' instances from sensor_instance table and poke on behalf of them in batches.
Users don't need to change their existing DAGs.

Enable/Disable Smart Sensor
---------------------------

Updating from a older version might need a schema change. If there is no ``sensor_instance`` table
in the DB, please make sure to run ``airflow db upgrade``

Add the following settings in the ``airflow.cfg``:

.. code-block::

    [smart_sensor]
    use_smart_sensor = true
    shard_code_upper_limit = 10000

    # Users can change the following config based on their requirements
    shards = 5
    sensors_enabled = NamedHivePartitionSensor, MetastorePartitionSensor

*   ``use_smart_sensor``: This config indicates if the smart sensor is enabled.
*   ``shards``: This config indicates the number of concurrently running smart sensor jobs for
    the Airflow cluster.
*   ``sensors_enabled``: This config is a list of sensor class names that will use the smart sensor.
    The users use the same class names (e.g. HivePartitionSensor) in their DAGs and they don't have
    the control to use smart sensors or not, unless they exclude their tasks explicitly.

Enabling/disabling the smart sensor service is a system level configuration change.
It is transparent to the individual users. Existing DAGs don't need to be changed for
enabling/disabling the smart sensor. Rotating centralized smart sensor tasks will not
cause any user's sensor task failure.

*   Using callback arguments (``on_success_callback``, ``on_failure_callback``, and ``on_retry_callback``) on a sensor task is not compatible with the smart sensor mode. If any callback arguments are provided, the sensor task will not be executed when the smart sensor mode is enabled.

Support new operators in the smart sensor service
-------------------------------------------------

*   Define ``poke_context_fields`` as class attribute in the sensor. ``poke_context_fields``
    include all key names used for initializing a sensor object.
*   In ``airflow.cfg``, add the new operator's classname to ``[smart_sensor] sensors_enabled``.
    All supported sensors' classname should be comma separated.

Migrating to Deferrable Operators
----------------------------------

There is not a direct migration path from Smart Sensors to :doc:`Deferrable Operators <deferring>`.
You have a few paths forward, depending on your needs and situation:

*   Do nothing - your DAGs will continue to run as-is, however they will no longer get the optimization smart sensors brought
*   Deferrable Operator - move to a Deferrable Operator that alleviates the need for a sensor all-together
*   Deferrable Sensor - move to an async version of the sensor you are already using

See :ref:`Writing Deferrable Operators <deferring/writing>` for details on writing Deferrable Operators and Sensors.
