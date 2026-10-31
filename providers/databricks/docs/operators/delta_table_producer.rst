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

.. _howto/operator:DatabricksDeltaTableVersionProducerOperator:


DatabricksDeltaTableVersionProducerOperator
===========================================

Use :class:`~airflow.providers.databricks.operators.databricks_delta_table.DatabricksDeltaTableVersionProducerOperator`
when a Unity Catalog Delta table is written by jobs or systems outside Airflow, and Dags should run
when it changes. Put the operator in a Dag that runs on a schedule. Each run reads the newest
Delta version and publishes an asset event for ``unity_table.to_asset()`` when that version is newer
than the last one it published. Otherwise the task is skipped and no event is published.

Downstream Dags schedule on the same asset that
:class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksCopyIntoAssetOperator` uses
for the table, so there is a single ``databricks://`` URI per table.

To wait inside an already scheduled Dag run instead, use a sensor. This operator is for producing
events continuously.

Example
-------

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_delta_producer.py
    :language: python
    :start-after: [START howto_operator_databricks_delta_table_version_producer]
    :end-before: [END howto_operator_databricks_delta_table_version_producer]

What an event means
-------------------

An event means that the producer observed a newer committed Delta version. Its ``extra`` contains:

* ``version`` and ``observed_version``: the newest Delta version at the time of the check.
* ``previous_version``: the version of the previous event, or ``baseline_version``. ``None`` for enrollment.
* ``table_identity``: the ``host``, ``catalog``, ``schema`` and ``table`` of the table.
* ``operation`` and ``timestamp``: the Delta history entry of ``version``.
* ``change``: ``enrollment``, ``new_version`` or ``recreated``.
* ``delta_table_id``: the Delta table id from ``DESCRIBE DETAIL``.
* ``dedup_id``: the asset URI, table id and version. Use it to drop duplicate events.
* ``databricks_delta_checkpoint_key``: the ``checkpoint_key`` of the producer.

Commits are coalesced. If the table moved from version 5 to version 9 between two runs, the producer
publishes one event for version 9. It does not publish events for versions 6, 7 and 8.

An event for version N does not make downstream queries read version N. The table can change again
before the consumer runs. Consumers that need a reproducible read query ``VERSION AS OF`` the
``version`` in the event, as the example does. Other tasks can publish events for the same asset,
for example ``COPY INTO`` with outlets, and those events may not have a ``version``. Consumers
that read ``version`` check ``change`` or ``databricks_delta_checkpoint_key`` first.

Checkpoints
-----------

The producer does not store state of its own. Its checkpoint is the newest event it published for the
asset with the same ``checkpoint_key``. The operator adds the asset to its ``inlets`` to read these
events. Events from other tasks for the same asset, such as ``COPY INTO`` or sensor outlet events, do
not change the checkpoint.

``checkpoint_key`` defaults to ``"<dag_id>.<task_id>"``. Renaming the Dag or task, or changing the
key, starts again with enrollment.

On the first run without a checkpoint, the producer publishes an ``enrollment`` event for the current
version, so that later runs have a checkpoint. To avoid triggering consumers on enrollment, pass
``baseline_version``. It is used as the checkpoint until the first event is published.

Delivery guarantees
-------------------

Airflow records the outlet event when the task succeeds, in the same database transaction that marks
the task successful. Publishing the event and advancing the checkpoint are therefore one write:

* If the task fails or the worker dies before the task succeeds, no event is published and the
  checkpoint does not move. The next run or retry publishes the newest version at that time.
* After the task succeeds, the event and the checkpoint exist together.

Delivery is at least once, not exactly once. The same version can be published twice when two runs of
the producer read the checkpoint at the same time, or when a successful run is cleared and runs again.
The operator sets ``max_active_tis_per_dag=1`` by default, and the producer Dag should set
``max_active_runs=1``. Consumers that must not process a version twice deduplicate on ``dedup_id``.

Maintenance commits
-------------------

``OPTIMIZE``, ``VACUUM START`` and ``VACUUM END`` commits do not change the data, so they do not
produce events by default. Pass ``ignored_operations`` to change the list, or an empty list to publish
for every commit.

If the newest commit is an ignored operation, the producer reads the history back to the checkpoint
to check that every commit after it is ignored. It publishes when it cannot check this, because the
history was removed by Delta log retention or because more than ``max_history_rows`` commits were made
since the checkpoint. The task is not skipped when it is not sure that the data did not change.

Recreated and renamed tables
----------------------------

The checkpoint stores the Delta table id. If the table is dropped and created again, or another table
is renamed to the observed name, the id changes and the version can go back. With
``on_recreation="publish"`` (the default), the producer publishes a ``recreated`` event and uses the new
table from then on. With ``on_recreation="fail"``, the task fails and the checkpoint is kept.

If the table is missing, cannot be accessed, or is not a Delta table, the task fails and publishes
nothing. Task retries apply as usual.

Cost
----

Each run starts or uses a SQL warehouse to run ``DESCRIBE DETAIL`` and ``DESCRIBE HISTORY``. Choose the
schedule of the producer Dag with this cost in mind.
