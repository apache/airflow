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

In addition to scheduling DAGs based upon time, they can also be scheduled based upon a task updating a dataset.

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

An Airflow dataset is a stand-in for a logical grouping of data. Datasets may be updated by upstream "producer" tasks, and dataset updates contribute to scheduling downstream "consumer" DAGs.

A dataset is defined by a Uniform Resource Identifier (URI):

.. code-block:: python

    from airflow.datasets import Dataset

    example_dataset = Dataset("s3://dataset-bucket/example.csv")

Airflow makes no assumptions about the content or location of the data represented by the URI. It is treated as a string, so any use of regular expressions (eg ``input_\d+.csv``) or file glob patterns (eg ``input_2022*.csv``) as an attempt to create multiple datasets from one declaration will not work.

A dataset should be created with a valid URI. Airflow core and providers define various URI schemes that you can use, such as ``file`` (core), ``postgres`` (by the Postgres provider), and ``s3`` (by the Amazon provider). Third-party providers and plugins may also provide their own schemes. These pre-defined schemes have individual semantics that are expected to be followed.

What is valid URI?
------------------

Technically, the URI must conform to the valid character set in RFC 3986. If you don't know what this means, that's basically ASCII alphanumeric characters, plus ``%``,  ``-``, ``_``, ``.``, and ``~``. To identify a resource that cannot be represented by URI-safe characters, encode the resource name with `percent-encoding <https://en.wikipedia.org/wiki/Percent-encoding>`_.

The URI is also case sensitive, so ``s3://example/dataset`` and ``s3://Example/Dataset`` are considered different. Note that the *host* part of the URI is also case sensitive, which differs from RFC 3986.

Do not use the ``airflow`` scheme, which is is reserved for Airflow's internals.

Airflow always prefers using lower cases in schemes, and case sensitivity is needed in the host part to correctly distinguish between resources.

.. code-block:: python

    # invalid datasets:
    reserved = Dataset("airflow://example_dataset")
    not_ascii = Dataset("èxample_datašet")

If you wish to define datasets with a scheme without additional semantic constraints, use a scheme with the prefix ``x-``. Airflow will skip any semantic validation on URIs with such schemes.

.. code-block:: python

    # valid dataset, treated as a plain string
    my_ds = Dataset("x-my-thing://foobarbaz")

The identifier does not have to be absolute; it can be a scheme-less, relative URI, or even just a simple path or string:

.. code-block:: python

    # valid datasets:
    schemeless = Dataset("//example/dataset")
    csv_file = Dataset("example_dataset")

Non-absolute identifiers are considered plain strings that do not carry any semantic meanings to Airflow.

Extra information on Dataset
----------------------------

If needed, an extra dictionary can be included in a Dataset:

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

.. note:: **Security Note:** Dataset URI and extra fields are not encrypted, they are stored in cleartext, in Airflow's metadata database. Do NOT store any sensitive values, especially credentials, in dataset URIs or extra key values!

How to use datasets in your DAGs
--------------------------------

You can use datasets to specify data dependencies in your DAGs. Take the following example:

.. code-block:: python

    example_dataset = Dataset("s3://dataset/example.csv")

    with DAG(dag_id="producer", ...):
        BashOperator(task_id="producer", outlets=[example_dataset], ...)

    with DAG(dag_id="consumer", schedule=[example_dataset], ...):
        ...

Once the ``producer`` task in the ``producer`` DAG has completed successfully, Airflow schedules the ``consumer`` DAG. A dataset will be marked as updated only if the task completes successfully — if the task fails or if it is skipped, no update occurs, and the ``consumer`` DAG will not be scheduled.

A listing of the relationships between datasets and DAGs can be found in the
:ref:`Datasets View<ui:datasets-view>`

Multiple Datasets
-----------------

As the ``schedule`` parameter is a list, DAGs can require multiple datasets, and the DAG will be scheduled once **all** datasets it consumes have been updated at least once since the last time it was run:

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


If one dataset is updated multiple times before all consumed datasets have been updated, the downstream DAG will still only be run once, as shown in this illustration:

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

Attaching extra information to an emitting Dataset Event
--------------------------------------------------------

.. versionadded:: 2.10.0

A task with a dataset outlet can optionally attach extra information before it emits a dataset event. This is different
from `Extra information on Dataset`_. Extra information on a dataset statically describes the entity pointed to by the dataset URI; extra information on the *dataset event* instead should be used to annotate the triggering data change, such as how many rows in the database are changed by the update, or the date range covered by it.

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

Another way to achieve the same is by accessing ``dataset_events`` in a task's execution context directly:

.. code-block:: python

    @task(outlets=[example_s3_dataset])
    def write_to_s3(*, dataset_events):
        dataset_events[example_s3_dataset].extras = {"row_count": len(df)}

There's minimal magic here---Airflow simply writes the yielded values to the exact same accessor. This also works in classic operators, including ``execute``, ``pre_execute``, and ``post_execute``.


Fetching information from a Triggering Dataset Event
----------------------------------------------------

A triggered DAG can fetch information from the Dataset that triggered it using the ``triggering_dataset_events`` template or parameter.
See more at :ref:`templates-ref`.

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

Note that this example is using `(.values() | first | first) <https://jinja.palletsprojects.com/en/3.1.x/templates/#jinja-filters.first>`_ to fetch the first of one Dataset given to the DAG, and the first of one DatasetEvent for that Dataset. An implementation may be quite complex if you have multiple Datasets, potentially with multiple DatasetEvents.

Advanced Dataset Scheduling with Conditional Expressions
--------------------------------------------------------

Apache Airflow introduces advanced scheduling capabilities that leverage conditional expressions with datasets. This feature allows Airflow users to define complex dependencies for DAG executions based on dataset updates, using logical operators for more granular control over workflow triggers.

Logical Operators for Datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Airflow supports two logical operators for combining dataset conditions:

- **AND (``&``)**: Specifies that the DAG should be triggered only after all of the specified datasets have been updated.
- **OR (``|``)**: Specifies that the DAG should be triggered when any one of the specified datasets is updated.

These operators enable the expression of complex dataset update conditions, enhancing the dynamism and flexibility of Airflow workflows.

Example Usage
-------------

**Scheduling Based on Multiple Dataset Updates**

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

**Scheduling Based on Any Dataset Update**

To trigger a DAG execution when either of two datasets is updated, apply the OR operator (``|``):

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

Combining Dataset and Time-Based Schedules
------------------------------------------

DatasetTimetable Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
With the introduction of ``DatasetOrTimeSchedule``, it is now possible to schedule DAGs based on both dataset events and time-based schedules. This feature offers flexibility for scenarios where a DAG needs to be triggered by data updates as well as run periodically according to a fixed timetable.

For more detailed information on ``DatasetOrTimeSchedule`` and its usage, refer to the corresponding section in :ref:`DatasetOrTimeSchedule <dataset-timetable-section>`.
