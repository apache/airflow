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

This extra information does not affect a dataset's identity. This means a DAG will be triggered by a dataset with an identical URI, even if the extra dict is different:

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

Fetching information from a triggering dataset event
----------------------------------------------------

A triggered DAG can fetch information from the dataset that triggered it using the ``triggering_dataset_events`` template or parameter.
See more at :ref:`templates-ref`.

Example:

.. code-block:: python

    example_snowflake_dataset = Dataset("snowflake://my_db.my_schema.my_table")

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

Combining dataset and time-based schedules
------------------------------------------

DatasetTimetable Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can schedule DAGs based on both dataset events and time-based schedules using ``DatasetOrTimeSchedule``. This allows you to create workflows when a DAG needs both to be triggered by data updates and run periodically according to a fixed timetable.

For more detailed information on ``DatasetOrTimeSchedule``, refer to the corresponding section in :ref:`DatasetOrTimeSchedule <dataset-timetable-section>`.
