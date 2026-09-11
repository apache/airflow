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



.. _howto/operator:AssetPartitionSensor:

AssetPartitionSensor
====================

Use the :class:`~airflow.providers.standard.sensors.asset.AssetPartitionSensor` to wait for an asset
event carrying a specific partition key.

A Dag scheduled with :class:`~airflow.sdk.PartitionedAssetTimetable` is triggered *by* matching asset
partitions. This sensor covers the opposite direction: a **time-scheduled** Dag that needs to block
until one named partition of an upstream asset has been produced.

Requires Airflow 3.4 or newer — partition-key filtering of asset events is not available on earlier
versions.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_partition_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_asset_partition]
    :end-before: [END howto_sensor_asset_partition]

Bounding the lookup with ``after``
----------------------------------

By default the sensor succeeds on *any* event that carries ``partition_key``, including one produced
long before the current run. That is what you want when the key is unique per event — a timestamp
such as ``2024-01-01T05``. It is not what you want when keys repeat across runs, for example a region
code like ``us``: an event from last week would satisfy the wait immediately.

Pass ``after`` to scope the lookup to the current interval. Both ``partition_key`` and ``after`` are
templated, so they can be derived from the run:

.. code-block:: python

    AssetPartitionSensor(
        task_id="wait_for_region",
        asset=regional_sales,
        partition_key="us",
        after="{{ data_interval_start }}",
    )

Deferrable mode
---------------

Waiting for an upstream partition can take a long time, so prefer ``deferrable=True`` to release the
worker slot while waiting. The sensor pokes once up front and only defers when the partition has not
arrived yet. Deferral is also the default when ``[operators] default_deferrable`` is set.

Lineage
-------

The waited-for ``asset`` is registered as a task inlet, so the data dependency appears in the asset
and lineage graph even though the Dag remains time-scheduled. Inlets are declarative only: they make
the dependency visible without scheduling or gating the Dag run on the asset.
