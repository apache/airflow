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



.. _howto/sensor:CdwHivePartitionSensor:


CdwHivePartitionSensor
======================

Use the :class:`~airflow.providers.cloudera.sensors.CdwHivePartitionSensor` to check the existence of a Hive partition.


Using the Sensor
----------------

Check the existence of a Hive partition.
CdwHivePartitionSensor is a subclass of HivePartitionSensor and supposed to implement the same logic by delegating the actual work to a CdwHiveMetastoreHook instance.


.. list-table::
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - table: str
     - The name of the table to wait for, supports the dot notation (my_database.my_table)
   * - partition: str
     - Name of the hive partition to check. Default ``"ds='{{ ds }}'"``
   * - cli_conn_id: str
     - The Airflow connection id for the target CDW instance, default value ``'metastore_default'``
   * - schema: str
     - The name of the DB schema, default value ``'default'``
   * - poke_interval: float
     - Time in seconds that the job should wait in between each tries. Default `'60 * 3``
