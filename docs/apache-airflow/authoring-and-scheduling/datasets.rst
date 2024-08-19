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

In addition to scheduling DAGs based on time, you can also schedule DAGs to run based on when a task updates a dataset.

.. code-block:: python

    from airflow.datasets import Dataset

    with DAG(...):
        MyOperator(
            # this task updates example.csv
            outlets=[Dataset("s3://dataset-bucket/example.csv")],
            ...,
        )


    with DAG(
        # this DAG should be run when example.csv is updated (by dag1)
        schedule=[Dataset("s3://dataset-bucket/example.csv")],
        ...,
    ):
        ...


.. image:: /img/dataset-scheduled-dags.png


What is a "dataset"?
--------------------

An Airflow dataset is a logical grouping of data. Upstream producer tasks can update datasets, and dataset updates contribute to scheduling downstream consumer DAGs.

`Uniform Resource Identifier (URI) <https://en.wikipedia.org/wiki/Uniform_Resource_Identifier>`_ define datasets:

.. code-block:: python

    from airflow.datasets import Dataset

    example_dataset = Dataset("s3://dataset-bucket/example.csv")

Airflow makes no assumptions about the content or location of the data represented by the URI, and treats the URI like a string. This means that Airflow treats any regular expressions, like ``input_\d+.csv``, or file glob patterns, such as ``input_2022*.csv``, as an attempt to create multiple datasets from one declaration, and they will not work.

You must create datasets with a valid URI. Airflow core and providers define various URI schemes that you can use, such as ``file`` (core), ``postgres`` (by the Postgres provider), and ``s3`` (by the Amazon provider). Third-party providers and plugins might also provide their own schemes. These pre-defined schemes have individual semantics that are expected to be followed.

What is valid URI?
------------------

Technically, the URI must conform to the valid character set in RFC 3986, which is basically ASCII alphanumeric characters, plus ``%``,  ``-``, ``_``, ``.``, and ``~``. To identify a resource that cannot be represented by URI-safe characters, encode the resource name with `percent-encoding <https://en.wikipedia.org/wiki/Percent-encoding>`_.

The URI is also case sensitive, so ``s3://example/dataset`` and ``s3://Example/Dataset`` are considered different. Note that the *host* part of the URI is also case sensitive, which differs from RFC 3986.

Do not use the ``airflow`` scheme, which is is reserved for Airflow's internals.

Airflow always prefers using lower cases in schemes, and case sensitivity is needed in the host part of the URI to correctly distinguish between resources.

.. code-block:: python

    # invalid datasets:
    reserved = Dataset("airflow://example_dataset")
    not_ascii = Dataset("èxample_datašet")

If you want to define datasets with a scheme that doesn't include additional semantic constraints, use a scheme with the prefix ``x-``. Airflow skips any semantic validation on URIs with these schemes.

.. code-block:: python

    # valid dataset, treated as a plain string
    my_ds = Dataset("x-my-thing://foobarbaz")

The identifier does not have to be absolute; it can be a scheme-less, relative URI, or even just a simple path or string:

.. code-block:: python

    # valid datasets:
    schemeless = Dataset("//example/dataset")
    csv_file = Dataset("example_dataset")

Non-absolute identifiers are considered plain strings that do not carry any semantic meanings to Airflow.

Extra information on dataset
----------------------------

If needed, you can include an extra dictionary in a dataset:

.. code-block:: python

    example_dataset = Dataset(
        "s3://dataset/example.csv",
        extra={"team": "trainees"},
    )

This can be used to supply custom description to the dataset, such as who has ownership to the target file, or what the file is for. The extra information does not affect a dataset's identity. This means a DAG will be triggered by a dataset with an identical URI, even if the extra dict is different:

.. code-block:: python

    with DAG(
        dag_id="consumer",
        schedule=[Dataset("s3://dataset/example.csv", extra={"different": "extras"})],
    ):
        ...

    with DAG(dag_id="producer", ...):
        MyOperator(
            # triggers "consumer" with the given extra!
            outlets=[Dataset("s3://dataset/example.csv", extra={"team": "trainees"})],
            ...,
        )

.. note:: **Security Note:** Dataset URI and extra fields are not encrypted, they are stored in cleartext in Airflow's metadata database. Do NOT store any sensitive values, especially credentials, in either dataset URIs or extra key values!

How to use datasets in your DAGs
--------------------------------

You can use datasets to specify data dependencies in your DAGs. The following example shows how after the ``producer`` task in the ``producer`` DAG successfully completes, Airflow schedules the ``consumer`` DAG. Airflow marks a dataset as ``updated`` only if the task completes successfully. If the task fails or if it is skipped, no update occurs, and Airflow doesn't schedule the ``consumer`` DAG.

.. code-block:: python

    example_dataset = Dataset("s3://dataset/example.csv")

    with DAG(dag_id="producer", ...):
        BashOperator(task_id="producer", outlets=[example_dataset], ...)

    with DAG(dag_id="consumer", schedule=[example_dataset], ...):
        ...


You can find a listing of the relationships between datasets and DAGs in the
:ref:`Datasets View<ui:datasets-view>`

Multiple Datasets
-----------------

Because the ``schedule`` parameter is a list, DAGs can require multiple datasets. Airflow schedules a DAG after **all** datasets the DAG consumes have been updated at least once since the last time the DAG ran:

.. code-block:: python

    with DAG(
        dag_id="multiple_datasets_example",
        schedule=[
            example_dataset_1,
            example_dataset_2,
            example_dataset_3,
        ],
        ...,
    ):
        ...


If one dataset is updated multiple times before all consumed datasets update, the downstream DAG still only runs once, as shown in this illustration:

.. ::
    ASCII art representation of this diagram

    example_dataset_1   x----x---x---x----------------------x-
    example_dataset_2   -------x---x-------x------x----x------
    example_dataset_3   ---------------x-----x------x---------
    DAG runs created                   *                    *

.. graphviz::

    graph dataset_event_timeline {
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
        example_dataset_1 [ pos="-0.5,2.5!"]
        example_dataset_2 [ pos="-0.5,2!"]
        example_dataset_3 [ pos="-0.5,1.5!"]
        dag_runs [label="DagRuns created" pos="-0.5,1!"]
      }

      edge [color=lightgrey]

      example_dataset_1 -- e1 -- e2       -- e4       -- e6                                        -- e13 -- end_ds1
      example_dataset_2             -- e3       -- e5             -- e8       -- e10        -- e12        -- end_ds2
      example_dataset_3                                     -- e7       -- e9        -- e11               -- end_ds3

    }

Attaching extra information to an emitting dataset event
--------------------------------------------------------

.. versionadded:: 2.10.0

A task with a dataset outlet can optionally attach extra information before it emits a dataset event. This is different
from `Extra information on dataset`_. Extra information on a dataset statically describes the entity pointed to by the dataset URI; extra information on the *dataset event* instead should be used to annotate the triggering data change, such as how many rows in the database are changed by the update, or the date range covered by it.

The easiest way to attach extra information to the dataset event is by ``yield``-ing a ``Metadata`` object from a task:

.. code-block:: python

    from airflow.datasets import Dataset
    from airflow.datasets.metadata import Metadata

    example_s3_dataset = Dataset("s3://dataset/example.csv")


    @task(outlets=[example_s3_dataset])
    def write_to_s3():
        df = ...  # Get a Pandas DataFrame to write.
        # Write df to dataset...
        yield Metadata(example_s3_dataset, {"row_count": len(df)})

Airflow automatically collects all yielded metadata, and populates dataset events with extra information for corresponding metadata objects.

This can also be done in classic operators. The best way is to subclass the operator and override ``execute``. Alternatively, extras can also be added in a task's ``pre_execute`` or ``post_execute`` hook. If you choose to use hooks, however, remember that they are not rerun when a task is retried, and may cause the extra information to not match actual data in certain scenarios.

Another way to achieve the same is by accessing ``outlet_events`` in a task's execution context directly:

.. code-block:: python

    @task(outlets=[example_s3_dataset])
    def write_to_s3(*, outlet_events):
        outlet_events[example_s3_dataset].extra = {"row_count": len(df)}

There's minimal magic here---Airflow simply writes the yielded values to the exact same accessor. This also works in classic operators, including ``execute``, ``pre_execute``, and ``post_execute``.

.. _fetching_information_from_previously_emitted_dataset_events:

Fetching information from previously emitted dataset events
-----------------------------------------------------------

.. versionadded:: 2.10.0

Events of a dataset defined in a task's ``outlets``, as described in the previous section, can be read by a task that declares the same dataset in its ``inlets``. A dataset event entry contains ``extra`` (see previous section for details), ``timestamp`` indicating when the event was emitted from a task, and ``source_task_instance`` linking the event back to its source.

Inlet dataset events can be read with the ``inlet_events`` accessor in the execution context. Continuing from the ``write_to_s3`` task in the previous section:

.. code-block:: python

    @task(inlets=[example_s3_dataset])
    def post_process_s3_file(*, inlet_events):
        events = inlet_events[example_s3_dataset]
        last_row_count = events[-1].extra["row_count"]

Each value in the ``inlet_events`` mapping is a sequence-like object that orders past events of a given dataset by ``timestamp``, earliest to latest. It supports most of Python's list interface, so you can use ``[-1]`` to access the last event, ``[-2:]`` for the last two, etc. The accessor is lazy and only hits the database when you access items inside it.


Fetching information from a triggering dataset event
----------------------------------------------------

A triggered DAG can fetch information from the dataset that triggered it using the ``triggering_dataset_events`` template or parameter. See more at :ref:`templates-ref`.

Example:

.. code-block:: python

    example_snowflake_dataset = Dataset("snowflake://my_db/my_schema/my_table")

    with DAG(dag_id="load_snowflake_data", schedule="@hourly", ...):
        SQLExecuteQueryOperator(
            task_id="load", conn_id="snowflake_default", outlets=[example_snowflake_dataset], ...
        )

    with DAG(dag_id="query_snowflake_data", schedule=[example_snowflake_dataset], ...):
        SQLExecuteQueryOperator(
            task_id="query",
            conn_id="snowflake_default",
            sql="""
              SELECT *
              FROM my_db.my_schema.my_table
              WHERE "updated_at" >= '{{ (triggering_dataset_events.values() | first | first).source_dag_run.data_interval_start }}'
              AND "updated_at" < '{{ (triggering_dataset_events.values() | first | first).source_dag_run.data_interval_end }}';
            """,
        )

        @task
        def print_triggering_dataset_events(triggering_dataset_events=None):
            for dataset, dataset_list in triggering_dataset_events.items():
                print(dataset, dataset_list)
                print(dataset_list[0].source_dag_run.dag_id)

        print_triggering_dataset_events()

Note that this example is using `(.values() | first | first) <https://jinja.palletsprojects.com/en/3.1.x/templates/#jinja-filters.first>`_ to fetch the first of one dataset given to the DAG, and the first of one DatasetEvent for that dataset. An implementation can be quite complex if you have multiple datasets, potentially with multiple DatasetEvents.


Manipulating queued dataset events through REST API
---------------------------------------------------

.. versionadded:: 2.9

In this example, the DAG ``waiting_for_dataset_1_and_2`` will be triggered when tasks update both datasets "dataset-1" and "dataset-2". Once "dataset-1" is updated, Airflow creates a record. This ensures that Airflow knows to trigger the DAG when "dataset-2" is updated. We call such records queued dataset events.

.. code-block:: python

    with DAG(
        dag_id="waiting_for_dataset_1_and_2",
        schedule=[Dataset("dataset-1"), Dataset("dataset-2")],
        ...,
    ):
        ...


``queuedEvent`` API endpoints are introduced to manipulate such records.

* Get a queued Dataset event for a DAG: ``/datasets/queuedEvent/{uri}``
* Get queued Dataset events for a DAG: ``/dags/{dag_id}/datasets/queuedEvent``
* Delete a queued Dataset event for a DAG: ``/datasets/queuedEvent/{uri}``
* Delete queued Dataset events for a DAG: ``/dags/{dag_id}/datasets/queuedEvent``
* Get queued Dataset events for a Dataset: ``/dags/{dag_id}/datasets/queuedEvent/{uri}``
* Delete queued Dataset events for a Dataset: ``DELETE /dags/{dag_id}/datasets/queuedEvent/{uri}``

 For how to use REST API and the parameters needed for these endpoints, please refer to :doc:`Airflow API </stable-rest-api-ref>`.

Advanced dataset scheduling with conditional expressions
--------------------------------------------------------

Apache Airflow includes advanced scheduling capabilities that use conditional expressions with datasets. This feature allows you to define complex dependencies for DAG executions based on dataset updates, using logical operators for more control on workflow triggers.

Logical operators for datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Airflow supports two logical operators for combining dataset conditions:

- **AND (``&``)**: Specifies that the DAG should be triggered only after all of the specified datasets have been updated.
- **OR (``|``)**: Specifies that the DAG should be triggered when any of the specified datasets is updated.

These operators enable you to configure your Airflow workflows to use more complex dataset update conditions, making them more dynamic and flexible.

Example Use
-------------

**Scheduling based on multiple dataset updates**

To schedule a DAG to run only when two specific datasets have both been updated, use the AND operator (``&``):

.. code-block:: python

    dag1_dataset = Dataset("s3://dag1/output_1.txt")
    dag2_dataset = Dataset("s3://dag2/output_1.txt")

    with DAG(
        # Consume dataset 1 and 2 with dataset expressions
        schedule=(dag1_dataset & dag2_dataset),
        ...,
    ):
        ...

**Scheduling based on any dataset update**

To trigger a DAG execution when either one of two datasets is updated, apply the OR operator (``|``):

.. code-block:: python

    with DAG(
        # Consume dataset 1 or 2 with dataset expressions
        schedule=(dag1_dataset | dag2_dataset),
        ...,
    ):
        ...

**Complex Conditional Logic**

For scenarios requiring more intricate conditions, such as triggering a DAG when one dataset is updated or when both of two other datasets are updated, combine the OR and AND operators:

.. code-block:: python

    dag3_dataset = Dataset("s3://dag3/output_3.txt")

    with DAG(
        # Consume dataset 1 or both 2 and 3 with dataset expressions
        schedule=(dag1_dataset | (dag2_dataset & dag3_dataset)),
        ...,
    ):
        ...


Dynamic data events emitting and dataset creation through AssetAlias
-----------------------------------------------------------------------
An asset alias can be used to emit dataset events of datasets with association to the aliases. Downstreams can depend on resolved dataset. This feature allows you to define complex dependencies for DAG executions based on dataset updates.

How to use AssetAlias
~~~~~~~~~~~~~~~~~~~~~~~

``AssetAlias`` has one single argument ``name`` that uniquely identifies the dataset. The task must first declare the alias as an outlet, and use ``outlet_events`` or yield ``Metadata`` to add events to it.

The following example creates a dataset event against the S3 URI ``f"s3://bucket/my-task"``  with optional extra information ``extra``. If the dataset does not exist, Airflow will dynamically create it and log a warning message.

**Emit a dataset event during task execution through outlet_events**

.. code-block:: python

    from airflow.datasets import AssetAlias


    @task(outlets=[AssetAlias("my-task-outputs")])
    def my_task_with_outlet_events(*, outlet_events):
        outlet_events["my-task-outputs"].add(Dataset("s3://bucket/my-task"), extra={"k": "v"})


**Emit a dataset event during task execution through yielding Metadata**

.. code-block:: python

    from airflow.datasets.metadata import Metadata


    @task(outlets=[AssetAlias("my-task-outputs")])
    def my_task_with_metadata():
        s3_dataset = Dataset("s3://bucket/my-task")
        yield Metadata(s3_dataset, extra={"k": "v"}, alias="my-task-outputs")

Only one dataset event is emitted for an added dataset, even if it is added to the alias multiple times, or added to multiple aliases. However, if different ``extra`` values are passed, it can emit multiple dataset events. In the following example, two dataset events will be emitted.

.. code-block:: python

    from airflow.datasets import AssetAlias


    @task(
        outlets=[
            AssetAlias("my-task-outputs-1"),
            AssetAlias("my-task-outputs-2"),
            AssetAlias("my-task-outputs-3"),
        ]
    )
    def my_task_with_outlet_events(*, outlet_events):
        outlet_events["my-task-outputs-1"].add(Dataset("s3://bucket/my-task"), extra={"k": "v"})
        # This line won't emit an additional dataset event as the dataset and extra are the same as the previous line.
        outlet_events["my-task-outputs-2"].add(Dataset("s3://bucket/my-task"), extra={"k": "v"})
        # This line will emit an additional dataset event as the extra is different.
        outlet_events["my-task-outputs-3"].add(Dataset("s3://bucket/my-task"), extra={"k2": "v2"})

Scheduling based on asset aliases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Since dataset events added to an alias are just simple dataset events, a downstream DAG depending on the actual dataset can read dataset events of it normally, without considering the associated aliases. A downstream DAG can also depend on an asset alias. The authoring syntax is referencing the ``AssetAlias`` by name, and the associated dataset events are picked up for scheduling. Note that a DAG can be triggered by a task with ``outlets=AssetAlias("xxx")`` if and only if the alias is resolved into ``Dataset("s3://bucket/my-task")``. The DAG runs whenever a task with outlet ``AssetAlias("out")`` gets associated with at least one dataset at runtime, regardless of the dataset's identity. The downstream DAG is not triggered if no datasets are associated to the alias for a particular given task run. This also means we can do conditional dataset-triggering.

The asset alias is resolved to the datasets during DAG parsing. Thus, if the "min_file_process_interval" configuration is set to a high value, there is a possibility that the asset alias may not be resolved. To resolve this issue, you can trigger DAG parsing.

.. code-block:: python

    with DAG(dag_id="dataset-producer"):

        @task(outlets=[Dataset("example-alias")])
        def produce_dataset_events():
            pass


    with DAG(dag_id="asset-alias-producer"):

        @task(outlets=[AssetAlias("example-alias")])
        def produce_dataset_events(*, outlet_events):
            outlet_events["example-alias"].add(Dataset("s3://bucket/my-task"))


    with DAG(dag_id="dataset-consumer", schedule=Dataset("s3://bucket/my-task")):
        ...

    with DAG(dag_id="asset-alias-consumer", schedule=AssetAlias("example-alias")):
        ...


In the example provided, once the DAG ``asset-alias-producer`` is executed, the asset alias ``AssetAlias("example-alias")`` will be resolved to ``Dataset("s3://bucket/my-task")``. However, the DAG ``asset-alias-consumer`` will have to wait for the next DAG re-parsing to update its schedule. To address this, Airflow will re-parse the DAGs relying on the asset alias ``AssetAlias("example-alias")`` when it's resolved into datasets that these DAGs did not previously depend on. As a result, both the "dataset-consumer" and "asset-alias-consumer" DAGs will be triggered after the execution of DAG ``asset-alias-producer``.


Fetching information from previously emitted dataset events through resolved asset aliases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As mentioned in :ref:`Fetching information from previously emitted dataset events<fetching_information_from_previously_emitted_dataset_events>`, inlet dataset events can be read with the ``inlet_events`` accessor in the execution context, and you can also use asset aliases to access the dataset events triggered by them.

.. code-block:: python

    with DAG(dag_id="asset-alias-producer"):

        @task(outlets=[AssetAlias("example-alias")])
        def produce_dataset_events(*, outlet_events):
            outlet_events["example-alias"].add(Dataset("s3://bucket/my-task"), extra={"row_count": 1})


    with DAG(dag_id="asset-alias-consumer", schedule=None):

        @task(inlets=[AssetAlias("example-alias")])
        def consume_asset_alias_events(*, inlet_events):
            events = inlet_events[AssetAlias("example-alias")]
            last_row_count = events[-1].extra["row_count"]


Combining dataset and time-based schedules
------------------------------------------

DatasetTimetable Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can schedule DAGs based on both dataset events and time-based schedules using ``DatasetOrTimeSchedule``. This allows you to create workflows when a DAG needs both to be triggered by data updates and run periodically according to a fixed timetable.

For more detailed information on ``DatasetOrTimeSchedule``, refer to the corresponding section in :ref:`DatasetOrTimeSchedule <dataset-timetable-section>`.
