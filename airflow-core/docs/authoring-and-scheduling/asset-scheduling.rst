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

Asset-Aware Scheduling
======================

.. versionadded:: 2.4

Quickstart
----------

In addition to scheduling Dags based on time, you can also schedule Dags to run based on when a task updates an asset.

.. code-block:: python

    from airflow.sdk import DAG, Asset

    with DAG(...):
        MyOperator(
            # this task updates example.csv
            outlets=[Asset("s3://asset-bucket/example.csv")],
            ...,
        )


    with DAG(
        # this Dag should be run when example.csv is updated (by dag1)
        schedule=[Asset("s3://asset-bucket/example.csv")],
        ...,
    ):
        ...


.. image:: /img/ui-dark/asset_scheduled_dags.png

.. seealso::

    :ref:`asset_definitions` for how to declare assets.

Schedule Dags with assets
-------------------------

You can use assets to specify data dependencies in your Dags. The following example shows how after the ``producer`` task
in the ``producer`` Dag successfully completes, Airflow schedules the ``consumer`` Dag. Airflow marks an asset as ``updated``
only if the task completes successfully. If the task fails or if it is skipped, no update occurs, and Airflow doesn't
schedule the ``consumer`` Dag.

.. code-block:: python

    example_asset = Asset("s3://asset/example.csv")

    with DAG(dag_id="producer", ...):
        BashOperator(task_id="producer", outlets=[example_asset], ...)

    with DAG(dag_id="consumer", schedule=[example_asset], ...):
        ...


You can find a listing of the relationships between assets and Dags in the :ref:`Asset Views <ui-asset-views>`.

Multiple assets
-----------------

Because the ``schedule`` parameter is a list, Dags can require multiple assets. Airflow schedules a Dag after **all** assets
the Dag consumes have been updated at least once since the last time the Dag ran:

.. code-block:: python

    with DAG(
        dag_id="multiple_assets_example",
        schedule=[
            example_asset_1,
            example_asset_2,
            example_asset_3,
        ],
        ...,
    ):
        ...


If one asset is updated multiple times before all consumed assets update, the downstream Dag still only runs once, as shown in this illustration:

.. ::
    ASCII art representation of this diagram

    example_asset_1   x----x---x---x----------------------x-
    example_asset_2   -------x---x-------x------x----x------
    example_asset_3   ---------------x-----x------x---------
    Dag runs created                   *                    *

.. graphviz::

    graph asset_event_timeline {
      graph [layout=neato]
      {
        node [margin=0 fontcolor=blue width=0.1 shape=point label=""]
        e1 [pos="1,2.5!"]
        e2 [pos="2,2.5!"]
        e3 [pos="2.5,2!"]
        e4 [pos="4,2.5!"]
        e5 [pos="5,2!"]
        e6 [pos="6,2.5!"]
        e7 [pos="7,1.5!"]
        r7 [pos="7,1!" shape=star width=0.25 height=0.25 fixedsize=shape]
        e8 [pos="8,2!"]
        e9 [pos="9,1.5!"]
        e10 [pos="10,2!"]
        e11 [pos="11,1.5!"]
        e12 [pos="12,2!"]
        e13 [pos="13,2.5!"]
        r13 [pos="13,1!" shape=star width=0.25 height=0.25 fixedsize=shape]
      }
      {
        node [shape=none label="" width=0]
        end_ds1 [pos="14,2.5!"]
        end_ds2 [pos="14,2!"]
        end_ds3 [pos="14,1.5!"]
      }

      {
        node [shape=none margin=0.25  fontname="roboto,sans-serif"]
        example_asset_1 [ pos="-0.5,2.5!"]
        example_asset_2 [ pos="-0.5,2!"]
        example_asset_3 [ pos="-0.5,1.5!"]
        dag_runs [label="DagRuns created" pos="-0.5,1!"]
      }

      edge [color=lightgrey]

      example_asset_1 -- e1 -- e2       -- e4       -- e6                                        -- e13 -- end_ds1
      example_asset_2             -- e3       -- e5             -- e8       -- e10        -- e12        -- end_ds2
      example_asset_3                                     -- e7       -- e9        -- e11               -- end_ds3

    }

Gate scheduled runs on asset updates
------------------------------------

Use ``AssetAndTimeSchedule`` when you want a Dag to follow a normal time-based timetable but only start after specific assets have been updated. Airflow creates the DagRun at the scheduled time and keeps it queued until every required asset has queued an event. When the DagRun starts, those asset events are consumed so the next scheduled run waits for new updates. This does not create additional asset-triggered runs.

.. code-block:: python

    from airflow.sdk import DAG, Asset
    from airflow.timetables.assets import AssetAndTimeSchedule
    from airflow.timetables.trigger import CronTriggerTimetable

    example_asset = Asset("s3://asset/example.csv")

    with DAG(
        dag_id="gated_hourly_dag",
        schedule=AssetAndTimeSchedule(
            timetable=CronTriggerTimetable("0 * * * *", timezone="UTC"),
            assets=[example_asset],
        ),
        ...,
    ):
        ...


Fetching information from a triggering asset event
----------------------------------------------------

A triggered Dag can fetch information from the asset that triggered it using the ``triggering_asset_events`` template or parameter. See more at :ref:`templates-ref`.

The ``triggering_asset_events`` is a dictionary that looks like this:

.. code-block:: python

    {
        Asset("s3://asset-bucket/example.csv"): [
            AssetEvent(uri="s3://asset-bucket/example.csv", source_dag_run=DagRun(...), ...),
            ...,
        ],
        Asset("s3://another-bucket/another.csv"): [
            AssetEvent(uri="s3://another-bucket/another.csv", source_dag_run=DagRun(...), ...),
            ...,
        ],
    }

You can access this information in your tasks using Jinja templating or directly in Python functions.

Accessing triggering asset events with Jinja
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use Jinja templating to pass information from the triggering asset events to your operators.

**Example: Single Triggering Asset**

If your DAG is triggered by a single asset, you can access its information like this:

.. code-block:: python

    example_snowflake_asset = Asset("snowflake://my_db/my_schema/my_table")

    with DAG(dag_id="query_snowflake_data", schedule=[example_snowflake_asset], ...):
        SQLExecuteQueryOperator(
            task_id="query",
            conn_id="snowflake_default",
            sql="""
              SELECT *
              FROM my_db.my_schema.my_table
              WHERE "updated_at" >= '{{ (triggering_asset_events.values() | first | first).source_dag_run.data_interval_start }}'
              AND "updated_at" < '{{ (triggering_asset_events.values() | first | first).source_dag_run.data_interval_end }}';
            """,
        )

In this example, ``triggering_asset_events.values() | first | first`` does the following:
1. ``triggering_asset_events.values()``: Gets a list of all lists of asset events.
2. ``| first``: Gets the first list of asset events (since we only have one triggering asset).
3. ``| first``: Gets the first ``AssetEvent`` from that list.

**Example: Multiple Triggering Assets**

When your DAG is triggered by multiple assets, you can iterate through them in your Jinja template.

.. code-block:: python

    with DAG(dag_id="process_assets", schedule=[asset1, asset2], ...):
        BashOperator(
            task_id="process",
            bash_command="""
            {% for asset_uri, events in triggering_asset_events.items() %}
              echo "Processing asset: {{ asset_uri }}"
              {% for event in events %}
                echo "  Triggered by DAG: {{ event.source_dag_run.dag_id }}"
                echo "  Data interval start: {{ event.source_dag_run.data_interval_start }}"
                echo "  Data interval end: {{ event.source_dag_run.data_interval_end }}"
              {% endfor %}
            {% endfor %}
            """,
        )


Accessing triggering asset events in Python
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also access the ``triggering_asset_events`` directly in a Python function by passing it as a parameter.

.. code-block:: python

    @task
    def print_triggering_asset_events(triggering_asset_events=None):
        if triggering_asset_events:
            for asset, asset_events in triggering_asset_events.items():
                print(f"Asset: {asset.uri}")
                for event in asset_events:
                    print(f"  - Triggered by DAG run: {event.source_dag_run.dag_id}")
                    print(
                        f"    Data interval: {event.source_dag_run.data_interval_start} to {event.source_dag_run.data_interval_end}"
                    )
                    print(f"    Run ID: {event.source_dag_run.run_id}")
                    print(f"    Timestamp: {event.timestamp}")


    print_triggering_asset_events()

.. note::
    When a DAG is scheduled by multiple assets, there may be multiple asset events for each asset. The logic for handling these events can be complex. It is up to the DAG author to decide how to process them. For example, you might want to process all new data since the last run, or you might want to process each triggering event individually.

Note that this example is using `(.values() | first | first) <https://jinja.palletsprojects.com/en/3.1.x/templates/#jinja-filters.first>`_ to
fetch the first of one asset given to the Dag, and the first of one AssetEvent for that asset. An implementation can be quite complex if you
have multiple assets, potentially with multiple AssetEvents.


Event-driven scheduling
-------------------------

Asset-based scheduling triggered by other Dags (internal event-driven scheduling) does not cover all use cases.
Often you also need to trigger Dags from **external events** such as system signals, messages, or real-time data changes.
Airflow supports two main approaches for external event-driven scheduling:

Push-based scheduling (REST API)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.9

External systems can **push asset events into Airflow** using the REST API.
For example, the Dag ``waiting_for_asset_1_and_2`` will be triggered when tasks update both assets "asset-1" and "asset-2".
Once "asset-1" is updated, Airflow creates a record. This ensures that Airflow knows to trigger the Dag when "asset-2" is updated.
These records are called *queued asset events*.

.. code-block:: python

    with DAG(
        dag_id="waiting_for_asset_1_and_2",
        schedule=[Asset("asset-1"), Asset("asset-2")],
        ...,
    ):
        ...


``queuedEvent`` API endpoints are available to manipulate these records:

* Get a queued asset event for a Dag: ``/assets/queuedEvent/{uri}``
* Get queued asset events for a Dag: ``/dags/{dag_id}/assets/queuedEvent``
* Delete a queued asset event for a Dag: ``/assets/queuedEvent/{uri}``
* Delete queued asset events for a Dag: ``/dags/{dag_id}/assets/queuedEvent``
* Get queued asset events for an asset: ``/dags/{dag_id}/assets/queuedEvent/{uri}``
* Delete queued asset events for an asset: ``DELETE /dags/{dag_id}/assets/queuedEvent/{uri}``

For details on usage and parameters, see :doc:`Airflow API </stable-rest-api-ref>`.

Pull-based scheduling (Asset Watchers)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of relying on external systems to push events, Airflow can **pull** from external event sources directly.
This is done using ``AssetWatcher`` classes and triggers compatible with event-driven scheduling.

* An ``AssetWatcher`` monitors an external source such as a queue or storage system.
* When a relevant event occurs, it updates the corresponding asset and triggers Dag execution.
* Only triggers that inherit from ``BaseEventTrigger`` are compatible, to avoid infinite rescheduling scenarios.

For more details and examples, see :doc:`event-scheduling`.

Advanced asset scheduling with conditional expressions
--------------------------------------------------------

Apache Airflow includes advanced scheduling capabilities that use conditional expressions with assets.
This feature allows you to define complex dependencies for Dag executions based on asset updates, using logical
operators for more control on workflow triggers.

Logical operators for assets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Airflow supports two logical operators for combining asset conditions:

- **AND (``&``)**: Specifies that the Dag should be triggered only after all of the specified assets have been updated.
- **OR (``|``)**: Specifies that the Dag should be triggered when any of the specified assets is updated.

These operators enable you to configure your Airflow workflows to use more complex asset update conditions, making them more dynamic and flexible.

Example Use
-------------

**Scheduling based on multiple asset updates**

To schedule a Dag to run only when two specific assets have both been updated, use the AND operator (``&``):

.. code-block:: python

    dag1_asset = Asset("s3://dag1/output_1.txt")
    dag2_asset = Asset("s3://dag2/output_1.txt")

    with DAG(
        # Consume asset 1 and 2 with asset expressions
        schedule=(dag1_asset & dag2_asset),
        ...,
    ):
        ...

**Scheduling based on any asset update**

To trigger a Dag execution when either one of two assets is updated, apply the OR operator (``|``):

.. code-block:: python

    with DAG(
        # Consume asset 1 or 2 with asset expressions
        schedule=(dag1_asset | dag2_asset),
        ...,
    ):
        ...

**Complex Conditional Logic**

For scenarios requiring more intricate conditions, such as triggering a Dag when one asset is updated or when both of two other assets are updated, combine the OR and AND operators:

.. code-block:: python

    dag3_asset = Asset("s3://dag3/output_3.txt")

    with DAG(
        # Consume asset 1 or both 2 and 3 with asset expressions
        schedule=(dag1_asset | (dag2_asset & dag3_asset)),
        ...,
    ):
        ...


Scheduling based on asset aliases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Since asset events added to an alias are just simple asset events, a downstream Dag depending on the actual asset can read asset events of it normally, without considering the associated aliases. A downstream Dag can also depend on an asset alias. The authoring syntax is referencing the ``AssetAlias`` by name, and the associated asset events are picked up for scheduling. Note that a Dag can be triggered by a task with ``outlets=AssetAlias("xxx")`` if and only if the alias is resolved into ``Asset("s3://bucket/my-task")``. The Dag runs whenever a task with outlet ``AssetAlias("out")`` gets associated with at least one asset at runtime, regardless of the asset's identity. The downstream Dag is not triggered if no assets are associated to the alias for a particular given task run. This also means we can do conditional asset-triggering.

The asset alias is resolved to the assets during Dag parsing. Thus, if the "min_file_process_interval" configuration is set to a high value, there is a possibility that the asset alias may not be resolved. To resolve this issue, you can trigger Dag parsing.

.. code-block:: python

    with DAG(dag_id="asset-producer"):

        @task(outlets=[Asset("example-alias")])
        def produce_asset_events():
            pass


    with DAG(dag_id="asset-alias-producer"):

        @task(outlets=[AssetAlias("example-alias")])
        def produce_asset_events(*, outlet_events):
            outlet_events[AssetAlias("example-alias")].add(Asset("s3://bucket/my-task"))


    with DAG(dag_id="asset-consumer", schedule=Asset("s3://bucket/my-task")):
        ...

    with DAG(dag_id="asset-alias-consumer", schedule=AssetAlias("example-alias")):
        ...


In the example provided, once the Dag ``asset-alias-producer`` is executed, the asset alias ``AssetAlias("example-alias")`` will be
resolved to ``Asset("s3://bucket/my-task")``. However, the Dag ``asset-alias-consumer`` will have to wait for the next Dag re-parsing
to update its schedule. To address this, Airflow will re-parse the Dags relying on the asset alias ``AssetAlias("example-alias")``
when it's resolved into assets that these Dags did not previously depend on. As a result, both the "asset-consumer" and
"asset-alias-consumer" Dags will be triggered after the execution of Dag ``asset-alias-producer``.


Combining asset and time-based schedules
------------------------------------------

AssetTimetable Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Asset-aware timetables combine asset expressions with a time-based schedule:

* Use ``AssetOrTimeSchedule`` to create runs both on a timetable and when assets update, producing scheduled runs and asset-triggered runs independently.
* Use ``AssetAndTimeSchedule`` to keep a Dag on a timetable but only start those scheduled runs once the referenced assets have been updated.

For more detailed information on asset-aware timetables, refer to :ref:`AssetOrTimeSchedule <asset-timetable-section>`.
