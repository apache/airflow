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




Object Storage
==============

This tutorial shows how to use the Object Storage API to manage objects that
reside on object storage, like S3, gcs and azure blob storage. The API is introduced
as part of Airflow 2.8.

The tutorial covers a simple pattern that is often used in data engineering and
data science workflows: accessing a web api, saving and analyzing the result. For the
tutorial to work you will need to have Duck DB installed, which is a in-process
analytical database. You can do this by running ``pip install duckdb``. The tutorial
makes use of S3 Object Storage. This requires that the amazon provider is installed
including ``s3fs`` by running ``pip install apache-airflow-providers-amazon[s3fs]``.
If you would like to use a different storage provider, you can do so by changing the
url in the ``create_object_storage_path`` function to the appropriate url for your
provider, for example by replacing ``s3://`` with ``gs://`` for Google Cloud Storage.
You will also need the right provider to be installed then. Finally, you will need
``pandas``, which can be installed by running ``pip install pandas``.


Creating an ObjectStoragePath
-----------------------------

The ObjectStoragePath is a path-like object that represents a path on object storage.
It is the fundamental building block of the Object Storage API.

.. exampleinclude:: /../../airflow/example_dags/tutorial_objectstorage.py
    :language: python
    :start-after: [START create_object_storage_path]
    :end-before: [END create_object_storage_path]

The ObjectStoragePath constructor can take an optional connection id. If supplied
it will use the connection to obtain the right credentials to access the backend.
Otherwise it will revert to the default for that backend.

It is safe to instantiate an ObjectStoragePath at the root of your DAG. Connections
will not be created until the path is used. This means that you can create the
path in the global scope of your DAG and use it in multiple tasks.

Saving data to Object Storage
-----------------------------

An ObjectStoragePath behaves mostly like a pathlib.Path object. You can
use it to save and load data directly to and from object storage. So, a typical
flow could look like this:

.. exampleinclude:: /../../airflow/example_dags/tutorial_objectstorage.py
    :language: python
    :start-after: [START get_air_quality_data]
    :end-before: [END get_air_quality_data]

The ``get_air_quality_data`` calls the API of the Finnish Meteorological Institute
to obtain the air quality data for the region of Helsinki. It creates a
Pandas DataFrame from the resulting json. It then saves the data to object storage
and converts it on the fly to parquet.

The key of the object is automatically generated from the logical date of the task,
so we could run this everyday and it would create a new object for each day. We
concatenate this key with the base path to create the full path to the object. Finally,
after writing the object to storage, we return the path to the object. This allows
us to use the path in the next task.

Analyzing the data
------------------

In understanding the data, you typically want to analyze it. Duck DB is a great
tool for this. It is an in-process analytical database that allows you to run
SQL queries on data in memory.

Because the data is already in parquet format, we can use the ``read_parquet`` and
because both Duck DB and the ObjectStoragePath use ``fsspec`` we can register the
backend of the ObjectStoragePath with Duck DB. ObjectStoragePath exposes the ``fs``
property for this. We can then use the ``register_filesystem`` function from Duck DB
to register the backend with Duck DB.

In Duck DB we can then create a table from the data and run a query on it. The
query is returned as a dataframe, which could be used for further analysis or
saved to object storage.

.. exampleinclude:: /../../airflow/example_dags/tutorial_objectstorage.py
    :language: python
    :start-after: [START analyze]
    :end-before: [END analyze]

You might note that the ``analyze`` function does not know the original
path to the object, but that it is passed in as a parameter and obtained
through XCom. You do not need to re-instantiate the Path object. Also
the connection details are handled transparently.

Putting it all together
-----------------------

The final DAG looks like this, which wraps things so that we can run it:

.. exampleinclude:: /../../airflow/example_dags/tutorial_objectstorage.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]
