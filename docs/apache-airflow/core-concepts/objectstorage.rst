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

Airflow provides a generic abstraction on top of object stores, like s3, gcs, and azure blob storage.
This abstraction allows you to use a variety of object storage systems in your DAGs without having to
change you code to deal with every different object storage system. In addition, it allows you to use
most of the standard Python modules, like ``shutil``, that can work with file-like objects.

Support for a particular object storage system is dependent on the providers you have installed. For
example, if you have installed the ``apache-airflow-providers-google`` provider, you will be able to
use the ``gcs`` scheme for object storage. Out of the box, Airflow provides support for the ``file``
scheme.

.. note::
    Support for s3 requires you to install ``apache-airflow-providers-amazon[s3fs]``. This is because
    it depends on ``aiobotocore``, which is not installed by default as it can create dependency
    challenges with ``botocore``.


.. _concepts:basic-use:

Basic Use
---------

To use object storage you instantiate a Path-like (see below) object with the URI of the object you
want to interact with. For example, to point to a bucket in s3 you would do the following:

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
`fsspec <https://filesystem-spec.readthedocs.io/en/latest/copying.html>`_. For cross object store directory copying,
Airflow needs to walk the directory tree and copy each file individually. This is done by streaming each file from the
source to the target.
