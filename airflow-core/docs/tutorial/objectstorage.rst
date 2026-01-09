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




Cloud-Native Workflows with Object Storage
==========================================

.. versionadded:: 2.8

Welcome to the final tutorial in our Airflow series! By now, you've built Dags with Python and the TaskFlow API, passed
data with XComs, and chained tasks together into clear, reusable workflows.

In this tutorial we'll take it a step further by introducing the **Object Storage API**. This API makes it easier to
read from and write to cloud storage -- like Amazon S3, Google Cloud Storage (GCS), or Azure Blob Storage -- without
having to worry about provider-specific SDKs or low-level credentials management.

We'll walk you through a real-world use case:

1. Pulling data from a public API
2. Saving that data to object storage in Parquet format
3. Analyzing it using SQL with DuckDB

Along the way, we'll highlight the new ``ObjectStoragePath`` abstraction, explain how Airflow handles cloud credentials via
connections, and show how this enables portable, cloud-agnostic pipelines.

Why This Matters
----------------

Many data workflows depend on files -- whether it's raw CSVs, intermediate Parquet files, or model artifacts.
Traditionally, you'd need to write S3-specific or GCS-specific code for this. Now, with ``ObjectStoragePath``, you can
write generic code that works across providers, as long as you've configured the right Airflow connection.

Let's get started!

Prerequisites
-------------

Before diving in, make sure you have the following:

- **DuckDB**, an in-process SQL database: Install with ``pip install duckdb``
- **Amazon S3 access** and **Amazon Provider with s3fs**: ``pip install apache-airflow-providers-amazon[s3fs]``
  (You can substitute your preferred provider by changing the storage URL protocol and installing the relevant provider.)
- **Pandas** for working with tabular data: ``pip install pandas``

Creating an ObjectStoragePath
-----------------------------

At the heart of this tutorial is ``ObjectStoragePath``, a new abstraction for handling paths on cloud object stores.
Think of it like ``pathlib.Path``, but for buckets instead of filesystems.

.. exampleinclude:: /../src/airflow/example_dags/tutorial_objectstorage.py
    :language: python
    :start-after: [START create_object_storage_path]
    :end-before: [END create_object_storage_path]

|

The URL syntax is simple: ``protocol://bucket/path/to/file``

- The ``protocol`` (like ``s3``, ``gs`` or ``azure``) determines the backend
- The "username" part of the URL can be a ``conn_id``, telling Airflow how to authenticate
- If the ``conn_id`` is omitted, Airflow will fall back to the default connection for that backend

You can also provide the ``conn_id`` as keyword argument for clarity:

.. code-block:: python

    ObjectStoragePath("s3://airflow-tutorial-data/", conn_id="aws_default")

This is especially handy when reusing a path defined elsewhere (like in an Asset), or when the connection isn't baked
into the URL. The keyword argument always takes precedence.

.. tip:: You can safely create an ``ObjectStoragePath`` in your global Dag scope. Connections are resolved only when the
  path is used, not when it's created.

Saving Data to Object Storage
-----------------------------

Let's fetch some data and save it to the cloud.

.. exampleinclude:: /../src/airflow/example_dags/tutorial_objectstorage.py
    :language: python
    :start-after: [START get_air_quality_data]
    :end-before: [END get_air_quality_data]

|

Here's what's happening:

- We call a public API from the Finnish Meteorological Institute for Helsinki air quality data
- The JSON response is parsed into a pandas DataFrame
- We generate a filename based on the task's logical date
- Using ``ObjectStoragePath``, we write the data directly to cloud storage as Parquet

This is a classic TaskFlow pattern. The object key changes each day, allowing us to run this daily and build a dataset
over time. We return the final object path to be used in the next task.

Why this is cool: No boto3, no GCS client setup, no credentials juggling. Just simple file semantics that work across
storage backends.

Analyzing the Data with DuckDB
------------------------------

Now let's analyze that data using SQL with DuckDB.

.. exampleinclude:: /../src/airflow/example_dags/tutorial_objectstorage.py
    :language: python
    :start-after: [START analyze]
    :end-before: [END analyze]

|

A few key things to note:

- DuckDB supports reading Parquet natively
- DuckDB and ObjectStoragePath both rely on ``fsspec``, which makes it easy to register the object storage backend
- We use ``path.fs`` to grab the right filesystem object and register it with DuckDB
- Finally, we query the Parquet file using SQL and return a pandas DataFrame

Notice that the function doesn't recreate the path manually -- it gets the full path from the previous task using Xcom.
This makes the task portable and decoupled from earlier logic.

Bringing It All Together
------------------------

Here's the full Dag that ties everything together:

.. exampleinclude:: /../src/airflow/example_dags/tutorial_objectstorage.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

|

You can trigger this Dag and view it in the Graph View in the Airflow UI. Each task logs its inputs and outputs clearly,
and you can inspect returned paths in the Xcom tab.

What to Explore Next
--------------------

Here are some ways to take this further:

- Use object sensors (like ``S3KeySensor``) to wait for files uploaded by external systems
- Orchestrate S3-to-GCS transfers or cross-region data syncs
- Add branching logic to handle missing or malformed files
- Experiment with different formats like CSV or JSON

**See Also**

- Learn how to securely access cloud services by configuring Airflow connections in the :doc:`Managing Connections guide <../authoring-and-scheduling/connections>`
- Build event-driven pipelines that respond to file uploads or external triggers using the :doc:`Event-Driven Scheduling framework <../authoring-and-scheduling/event-scheduling>`
- Reinforce your understanding of decorators, return values, and task chaining with the :doc:`TaskFlow API guide <../core-concepts/taskflow>`
