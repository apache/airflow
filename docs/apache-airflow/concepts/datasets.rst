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

In addition to scheduling DAGs based upon time, they can also be scheduled based upon another DAG updating a dataset.

.. code-block:: python

    from airflow import Dataset

    with DAG(...):
        task1 = MyOperator(
            # this task updates example.csv
            outlets=[Dataset("s3://dataset-bucket/example.csv")],
            ...,
        )


    with DAG(
        # this DAG should be run when example.csv is updated (by dag1)
        schedule=[Dataset("s3://dataset-bucket/example.csv")],
        ...,
    ) as dag2:
        Task2 = OtherOperator(...)
        ...

What is a "dataset"?
--------------------

An Airflow dataset is a stand-in for a logical grouping of data that flows through multiple DAGs, possibly being changed or updated by each one. Datasets are updated by upstream "producer" tasks, and dataset updates contribute to scheduling downstream "consumer" DAGs.

A dataset is a construct around a Uniform Resource Identifier (URI) that you create in your code:

.. code-block:: python

    from airflow import Dataset

    example_dataset = Dataset('s3://dataset-bucket/example.csv')

Airflow treats the dataset URI identifiers as opaque values intended to be human-readable, and makes no assumptions about the content or location of the data represented by the identifier. They are treated as strings, so any use of regular expressions (eg ``input_\d+.csv``) or file glob patterns (eg ``input_2022*.csv``) as an attempt to create multiple datasets from one declaration will not work.

There are three restrictions on the dataset identifier:

1. It must be a valid URI, which means it must only be composed of only ASCII characters.
2. The URI scheme cannot be ``airflow`` (this is reserved for future use).
3. It must be unique (although it is case is sensitive throughout, so "s3://example_dataset" and "s3://Example_Dataset" are considered different, and "s3://example_dataset" and "S3://example_dataset" are considered different).

If you try to use either of the examples below, your code will cause a ValueError to be raised, and Airflow will not import it.

.. code-block:: python

    # invalid datasets:
    reserved = Dataset(uri="airflow://example_dataset")
    not_ascii = Dataset(uri="èxample_datašet")

The identifier does not have to be an absolute URI, it can be a scheme-less, relative URI, or even just a simple path or string:

.. code-block:: python

    # valid datasets:
    schemeless = Dataset(uri="//example/dataset")
    csv_file = Dataset(uri="example.csv")

If required, an extra dictionary can be included in a Dataset:

.. code-block:: python

    example_dataset = Dataset(
        "s3://dataset/example.csv",
        extra={'team': 'trainees'},
    )

..note::

    Security Note: Dataset URI and extra fields are not encrypted, they are stored in cleartext, in Airflow's metadata database. Do NOT store any sensitive values, especially URL server credentials, in dataset URIs or extra key values!

How to use datasets in your DAGs
--------------------------------

You can use datasets to specify data dependencies in your DAGs. Take the following example:

.. code-block:: python

    example_dataset = Dataset("s3://dataset/example.csv")

    with DAG(dag_id='update_example_dataset', ...) as update_example_dataset:
        BashOperator(task_id='example_producer', outlets=[example_dataset], ...)

    with DAG(dag_id='example_consumer', schedule=[example_dataset], ...):
        BashOperator(...)

Once the ``example_producer`` task of the first ``update_example_dataset`` DAG has completed successfully, Airflow schedules ``requires_example_dataset``. Only a task's success triggers dataset updates — if the task fails or if it raises an :class:`~airflow.exceptions.AirflowSkipException`, no update occurs, and the ``requires_example_dataset`` DAG will not be scheduled.

Multiple Datasets
-----------------

As the ``schedule`` parameter is a list, DAGs can require multiple datasets, and the DAG will be scheduled once **all** datasets it consumes have been updated at least once since the last time it was run:

.. code-block:: python

    with DAG(
        dag_id='multiple_datasets_example',
        schedule=[
            example_dataset_1,
            example_dataset_2,
            example_dataset_3,
        ],
        ...,
    ):
        ...


If one dataset is updated multiple times before all consumed datasets have been updated, the downstream DAG will still only be run once, as shown in this illustration::

    example_dataset_1   x----x---x---x----------------------x-
    example_dataset_2   -------x---x-------x------x----x------
    example_dataset_3   ---------------x-----x------x---------
    DAG runs created                   *                    *

Notes on schedules
------------------

The ``schedule`` parameter to your DAG can take either a list of datasets to consume or a timetable-based option. The two cannot currently be mixed.

When using datasets, in this first release (v2.4) waiting for all datasets in the list to be updated is the only option when multiple datasets are consumed by a DAG. A later release should introduce more fine-grained options allowing for greater flexibility.

.. TODO:

    Add screengrabs of the new parts of the DAGs view
    Add screengrabs and prose to explain the new Dataset views
