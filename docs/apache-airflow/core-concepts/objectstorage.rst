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


.. _concepts:objectstorage:

Object Storage
==============

.. versionadded:: 2.8.0

All major cloud providers offer persistent data storage in object stores. These are not classic
"POSIX" file systems. In order to store hundreds of petabytes of data without any single points
of failure, object stores replace the classic file system directory tree with a simpler model
of object-name => data. To enable remote access, operations on objects are usually offered as
(slow) HTTP REST operations.

Airflow provides a generic abstraction on top of object stores, like s3, gcs, and azure blob storage.
This abstraction allows you to use a variety of object storage systems in your DAGs without having to
change you code to deal with every different object storage system. In addition, it allows you to use
most of the standard Python modules, like ``shutil``, that can work with file-like objects.

Support for a particular object storage system depends on the providers you have installed. For
example, if you have installed the ``apache-airflow-providers-google`` provider, you will be able to
use the ``gcs`` scheme for object storage. Out of the box, Airflow provides support for the ``file``
scheme.

.. note::
    Support for s3 requires you to install ``apache-airflow-providers-amazon[s3fs]``. This is because
    it depends on ``aiobotocore``, which is not installed by default as it can create dependency
    challenges with ``botocore``.

Cloud Object Stores are not real file systems
---------------------------------------------
Object stores are not real file systems although they can appear so. They do not support all the
operations that a real file system does. Key differences are:

* No guaranteed atomic rename operation. This means that if you move a file from one location to another, it
  will be copied and then deleted. If the copy fails, you will lose the file.
* Directories are emulated and might make working with them slow. For example, listing a directory might
  require listing all the objects in the bucket and filtering them by prefix.
* Seeking within a file may require significant call overhead hurting performance or might not be supported at all.

Airflow relies on `fsspec <https://filesystem-spec.readthedocs.io/en/latest/>`_ to provide a consistent
experience across different object storage systems. It  implements local file caching to speed up access.
However, you should be aware of the limitations of object storage when designing your DAGs.


.. _concepts:basic-use:

Basic Use
---------

To use object storage, you need to instantiate a Path-like (see below) object with the URI of the
object you want to interact with. For example, to point to a bucket in s3, you would do the following:

.. code-block:: python

    from airflow.io.store.path import ObjectStoragePath

    base = ObjectStoragePath("s3://my-bucket/", conn_id="aws_default")  # conn_id is optional


Listing file-objects:

.. code-block:: python

    @task
    def list_files() -> list(ObjectStoragePath):
        files = []
        for f in base.iterdir():
            if f.is_file():
                files.append(f)

        return files


Navigating inside a directory tree:

.. code-block:: python

   base = ObjectStoragePath("s3://my-bucket/")
   subdir = base / "subdir"

   # prints ObjectStoragePath("s3://my-bucket/subdir")
   print(subdir)


Opening a file:

.. code-block:: python

    @task
    def read_file(path: ObjectStoragePath) -> str:
        with path.open() as f:
            return f.read()


Leveraging XCOM, you can pass paths between tasks:

.. code-block:: python

      @task
      def create(path: ObjectStoragePath) -> ObjectStoragePath:
          return path / "new_file.txt"


      @task
      def write_file(path: ObjectStoragePath, content: str):
          with path.open("wb") as f:
              f.write(content)


      new_file = create(base)
      write = write_file(new_file, b"data")

      read >> write


Configuration
-------------

In its basic use, the object storage abstraction does not require much configuration and relies upon the
standard Airflow connection mechanism. This means that you can use the ``conn_id`` argument to specify
the connection to use. Any settings by the connection are pushed down to the underlying implementation.
For example, if you are using s3, you can specify the ``aws_access_key_id`` and ``aws_secret_access_key``
but also add extra arguments like ``endpoint_url`` to specify a custom endpoint.

Alternative backends
^^^^^^^^^^^^^^^^^^^^

It is possible to configure an alternative backend for a scheme or protocol. This is done by attaching
a ``backend`` to the scheme. For example, to enable the databricks backend for the ``dbfs`` scheme, you
would do the following:

.. code-block:: python

    from airflow.io.store.path import ObjectStoragePath
    from airflow.io.store import attach

    from fsspec.implementations.dbfs import DBFSFileSystem

    attach(protocol="dbfs", fs=DBFSFileSystem(instance="myinstance", token="mytoken"))
    base = ObjectStoragePath("dbfs://my-location/")


.. note::
    To reuse the registration across tasks make sure to attach the backend at the top-level of your DAG.
    Otherwise, the backend will not be available across multiple tasks.


.. _concepts:api:

Path-like API
-------------

The object storage abstraction is implemented as a `Path-like API <https://docs.python.org/3/library/pathlib.html>`_.
This means that you can mostly use the same API to interact with object storage as you would with a local filesystem.
In this section we only list the differences between the two APIs. Extended operations beyond the standard Path API
, like copying and moving, are listed in the next section. For details about each operation, like what arguments
they take, see the documentation of the :class:`~airflow.io.store.path.ObjectStoragePath` class.


stat
^^^^

Returns a ``stat_result`` like object that supports the following attributes: ``st_size``, ``st_mtime``, ``st_mode``,
but also acts like a dictionary that can provide additional metadata about the object. For example, for s3 it will,
return the additional keys like: ``['ETag', 'ContentType']``. If your code needs to be portable across different object
store do not rely on the extended metadata.

.. note::
    While ``stat`` does accept the ``follow_symlinks`` argument, it is not passed on to the object storage backend as
    not all object storage does not support symlinks.


mkdir
^^^^^

Create a directory entry at the specified path or within a bucket/container. For systems that don't have true
directories, it may create a directory entry for this instance only and not affect the real filesystem.

If ``create_parents`` is ``True`` (the default), any missing parents of this path are created as needed.


touch
^^^^^

Create an empty file, or update the timestamp. If ``truncate`` is ``True``, the file is truncated, which is the
default.


.. _concepts:extended-operations:

Extended Operations
-------------------

The following operations are not part of the standard Path API, but are supported by the object storage abstraction.

ukey
^^^^

Hash of file properties, to tell if it has changed.


checksum
^^^^^^^^

Return the checksum of the file.


read_block
^^^^^^^^^^

Read a block of bytes from the file. This is useful for reading large files in chunks.


du
^^

Space used by files and optionally directories within a path.


find
^^^^

Find files and optionally directories within a path.


ls
^^

List files within a path.


sign
^^^^

Create a signed URL representing the given path. Some implementations allow temporary URLs to be generated, as a
way of delegating credentials.


copy
^^^^

Copy a file from one path to another. If the destination is a directory, the file will be copied into it. If the
destination is a file, it will be overwritten.

move
^^^^

Move a file from one path to another. If the destination is a directory, the file will be moved into it. If the
destination is a file, it will be overwritten.


.. _concepts:copying-and-moving:

Copying and Moving
------------------

This documents the expected behavior of the ``copy`` and ``move`` operations, particularly for cross object store (e.g.
file -> s3) behavior. Each method copies or moves files or directories from a ``source`` to a ``target`` location.
The intended behavior is the same as specified by
``fsspec``. For cross object store directory copying,
Airflow needs to walk the directory tree and copy each file individually. This is done by streaming each file from the
source to the target.


External Integrations
---------------------

Many other projects, like DuckDB, Apache Iceberg etc, can make use of the object storage abstraction. Often this is
done by passing the underlying ``fsspec`` implementation. For this this purpose ``ObjectStoragePath`` exposes
the ``fs`` property. For example, the following works with ``duckdb`` so that the connection details from Airflow
are used to connect to s3 and a parquet file, indicated by a ``ObjectStoragePath``, is read:

.. code-block:: python

    import duckdb
    from airflow.io.store.path import ObjectStoragePath

    path = ObjectStoragePath("s3://my-bucket/my-table.parquet", conn_id="aws_default")
    conn = duckdb.connect(database=":memory:")
    conn.register_filesystem(path.fs)
    conn.execute(f"CREATE OR REPLACE TABLE my_table AS SELECT * FROM read_parquet('{path}")
