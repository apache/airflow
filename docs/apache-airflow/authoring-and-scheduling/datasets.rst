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

There are two restrictions on the dataset URI:

1. It must be a valid URI, which means it must be composed of only ASCII characters.
2. The URI scheme cannot be ``airflow`` (this is reserved for future use).

If you try to use either of the examples below, your code will cause a ValueError to be raised, and Airflow will not import it.

.. code-block:: python

    # invalid datasets:
    reserved = Dataset("airflow://example_dataset")
    not_ascii = Dataset("èxample_datašet")

The identifier does not have to be an absolute URI, it can be a scheme-less, relative URI, or even just a simple path or string:

.. code-block:: python

    # valid datasets:
    schemeless = Dataset("//example/dataset")
    csv_file = Dataset("example_dataset")

If required, an extra dictionary can be included in a Dataset:

.. code-block:: python

    example_dataset = Dataset(
        "s3://dataset/example.csv",
        extra={"team": "trainees"},
    )

.. note:: **Security Note:** Dataset URI and extra fields are not encrypted, they are stored in cleartext, in Airflow's metadata database. Do NOT store any sensitive values, especially credentials, in dataset URIs or extra key values!

The URI is also case sensitive throughout, so ``s3://example_dataset`` and ``s3://Example_Dataset`` are considered different, as is ``s3://example_dataset`` and ``S3://example_dataset``.

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

Notes on schedules
------------------

The ``schedule`` parameter to your DAG can take either a list of datasets to consume or a timetable-based option. The two cannot currently be mixed.

When using datasets, in this first release (v2.4) waiting for all datasets in the list to be updated is the only option when multiple datasets are consumed by a DAG. A later release may introduce more fine-grained options allowing for greater flexibility.
