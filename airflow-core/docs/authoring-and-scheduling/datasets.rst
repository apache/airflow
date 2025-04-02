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

Data-aware scheduling
=====================

.. versionadded:: 2.4

Quickstart
----------

In addition to scheduling dags based on time, you can also schedule dags to run based on when a task updates an asset.

.. code-block:: python

    from airflow.sdk import DAG, Asset

    with DAG(...):
        MyOperator(
            # this task updates example.csv
            outlets=[Asset("s3://asset-bucket/example.csv")],
            ...,
        )


    with DAG(
        # this DAG should be run when example.csv is updated (by dag1)
        schedule=[Asset("s3://asset-bucket/example.csv")],
        ...,
    ):
        ...


.. image:: /img/asset-scheduled-dags.png

.. seealso::

    :ref:`asset_definitions` for how to declare assets.

Schedule dags with assets
-------------------------

You can use assets to specify data dependencies in your dags. The following example shows how after the ``producer`` task in the ``producer`` DAG successfully completes, Airflow schedules the ``consumer`` DAG. Airflow marks an asset as ``updated`` only if the task completes successfully. If the task fails or if it is skipped, no update occurs, and Airflow doesn't schedule the ``consumer`` DAG.

.. code-block:: python

    example_asset = Asset("s3://asset/example.csv")

    with DAG(dag_id="producer", ...):
        BashOperator(task_id="producer", outlets=[example_asset], ...)

    with DAG(dag_id="consumer", schedule=[example_asset], ...):
        ...


You can find a listing of the relationships between assets and dags in the
:ref:`Assets View<ui:assets-view>`

Multiple assets
-----------------

Because the ``schedule`` parameter is a list, dags can require multiple assets. Airflow schedules a DAG after **all** assets the DAG consumes have been updated at least once since the last time the DAG ran:

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


If one asset is updated multiple times before all consumed assets update, the downstream DAG still only runs once, as shown in this illustration:

.. ::
    ASCII art representation of this diagram

    example_asset_1   x----x---x---x----------------------x-
    example_asset_2   -------x---x-------x------x----x------
    example_asset_3   ---------------x-----x------x---------
    DAG runs created                   *                    *

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

Fetching information from a triggering asset event
----------------------------------------------------

A triggered DAG can fetch information from the asset that triggered it using the ``triggering_asset_events`` template or parameter. See more at :ref:`templates-ref`.

Example:

.. code-block:: python

    example_snowflake_asset = Asset("snowflake://my_db/my_schema/my_table")

    with DAG(dag_id="load_snowflake_data", schedule="@hourly", ...):
        SQLExecuteQueryOperator(
            task_id="load", conn_id="snowflake_default", outlets=[example_snowflake_asset], ...
        )

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

        @task
        def print_triggering_asset_events(triggering_asset_events=None):
            for asset, asset_list in triggering_asset_events.items():
                print(asset, asset_list)
                print(asset_list[0].source_dag_run.dag_id)

        print_triggering_asset_events()

Note that this example is using `(.values() | first | first) <https://jinja.palletsprojects.com/en/3.1.x/templates/#jinja-filters.first>`_ to fetch the first of one asset given to the DAG, and the first of one AssetEvent for that asset. An implementation can be quite complex if you have multiple assets, potentially with multiple AssetEvents.


Manipulating queued asset events through REST API
---------------------------------------------------

.. versionadded:: 2.9

In this example, the DAG ``waiting_for_asset_1_and_2`` will be triggered when tasks update both assets "asset-1" and "asset-2". Once "asset-1" is updated, Airflow creates a record. This ensures that Airflow knows to trigger the DAG when "asset-2" is updated. We call such records queued asset events.

.. code-block:: python

    with DAG(
        dag_id="waiting_for_asset_1_and_2",
        schedule=[Asset("asset-1"), Asset("asset-2")],
        ...,
    ):
        ...


``queuedEvent`` API endpoints are introduced to manipulate such records.

* Get a queued asset event for a DAG: ``/assets/queuedEvent/{uri}``
* Get queued asset events for a DAG: ``/dags/{dag_id}/assets/queuedEvent``
* Delete a queued asset event for a DAG: ``/assets/queuedEvent/{uri}``
* Delete queued asset events for a DAG: ``/dags/{dag_id}/assets/queuedEvent``
* Get queued asset events for an asset: ``/dags/{dag_id}/assets/queuedEvent/{uri}``
* Delete queued asset events for an asset: ``DELETE /dags/{dag_id}/assets/queuedEvent/{uri}``

 For how to use REST API and the parameters needed for these endpoints, please refer to :doc:`Airflow API </stable-rest-api-ref>`.

Advanced asset scheduling with conditional expressions
--------------------------------------------------------

Apache Airflow includes advanced scheduling capabilities that use conditional expressions with assets. This feature allows you to define complex dependencies for DAG executions based on asset updates, using logical operators for more control on workflow triggers.

Logical operators for assets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Airflow supports two logical operators for combining asset conditions:

- **AND (``&``)**: Specifies that the DAG should be triggered only after all of the specified assets have been updated.
- **OR (``|``)**: Specifies that the DAG should be triggered when any of the specified assets is updated.

These operators enable you to configure your Airflow workflows to use more complex asset update conditions, making them more dynamic and flexible.

Example Use
-------------

**Scheduling based on multiple asset updates**

To schedule a DAG to run only when two specific assets have both been updated, use the AND operator (``&``):

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

To trigger a DAG execution when either one of two assets is updated, apply the OR operator (``|``):

.. code-block:: python

    with DAG(
        # Consume asset 1 or 2 with asset expressions
        schedule=(dag1_asset | dag2_asset),
        ...,
    ):
        ...

**Complex Conditional Logic**

For scenarios requiring more intricate conditions, such as triggering a DAG when one asset is updated or when both of two other assets are updated, combine the OR and AND operators:

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
Since asset events added to an alias are just simple asset events, a downstream DAG depending on the actual asset can read asset events of it normally, without considering the associated aliases. A downstream DAG can also depend on an asset alias. The authoring syntax is referencing the ``AssetAlias`` by name, and the associated asset events are picked up for scheduling. Note that a DAG can be triggered by a task with ``outlets=AssetAlias("xxx")`` if and only if the alias is resolved into ``Asset("s3://bucket/my-task")``. The DAG runs whenever a task with outlet ``AssetAlias("out")`` gets associated with at least one asset at runtime, regardless of the asset's identity. The downstream DAG is not triggered if no assets are associated to the alias for a particular given task run. This also means we can do conditional asset-triggering.

The asset alias is resolved to the assets during DAG parsing. Thus, if the "min_file_process_interval" configuration is set to a high value, there is a possibility that the asset alias may not be resolved. To resolve this issue, you can trigger DAG parsing.

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


In the example provided, once the DAG ``asset-alias-producer`` is executed, the asset alias ``AssetAlias("example-alias")`` will be resolved to ``Asset("s3://bucket/my-task")``. However, the DAG ``asset-alias-consumer`` will have to wait for the next DAG re-parsing to update its schedule. To address this, Airflow will re-parse the dags relying on the asset alias ``AssetAlias("example-alias")`` when it's resolved into assets that these dags did not previously depend on. As a result, both the "asset-consumer" and "asset-alias-consumer" dags will be triggered after the execution of DAG ``asset-alias-producer``.


Combining asset and time-based schedules
------------------------------------------

AssetTimetable Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can schedule dags based on both asset events and time-based schedules using ``AssetOrTimeSchedule``. This allows you to create workflows when a DAG needs both to be triggered by data updates and run periodically according to a fixed timetable.

For more detailed information on ``AssetOrTimeSchedule``, refer to the corresponding section in :ref:`AssetOrTimeSchedule <asset-timetable-section>`.
