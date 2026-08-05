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

Object Storage State Store Backend
===================================

The default state store backend is :class:`~airflow.state.metastore.MetastoreStateBackend`, which persists
task and asset state in the Airflow metadata database via the API Server's Execution API. For larger values,
you may want to store state on object storage directly from the task instead.

To enable object storage for task and asset state store, set ``state_store_backend`` in the ``[workers]``
section to ``airflow.providers.common.io.state_store.backend.StateStoreObjectStorageBackend``, and set
``state_store_objectstorage_path`` to the desired base location. The connection id is obtained from the
user part of the URL, e.g. ``state_store_objectstorage_path = s3://conn_id@mybucket/task-state/``.

Task state is stored under ``<dag_id>/<run_id>/<task_id>/<map_index>/<key>`` and asset state under
``assets/<asset_identifier>/<key>`` beneath the configured base path.

By default (``state_store_objectstorage_threshold = 0``) all serialized values are offloaded to object storage.
Set ``state_store_objectstorage_threshold`` to a positive number of bytes to only offload values whose
serialized size meets or exceeds the threshold, anything smaller are stored in the Airflow metadata database.

Optionally set ``state_store_objectstorage_compression`` to an fsspec-supported compression algorithm such as
``gzip`` or ``snappy`` to compress values before writing.

The following example stores all task and asset state in S3, compressed with gzip::

      [workers]
      state_store_backend = airflow.providers.common.io.state_store.backend.StateStoreObjectStorageBackend

      [common.io]
      state_store_objectstorage_path = s3://conn_id@mybucket/task-state/
      state_store_objectstorage_compression = gzip

To only offload values larger than 1 MB::

      [workers]
      state_store_backend = airflow.providers.common.io.state_store.backend.StateStoreObjectStorageBackend

      [common.io]
      state_store_objectstorage_path = s3://conn_id@mybucket/task-state/
      state_store_objectstorage_threshold = 1048576

Using the local filesystem (useful for development)::

      [workers]
      state_store_backend = airflow.providers.common.io.state_store.backend.StateStoreObjectStorageBackend

      [common.io]
      state_store_objectstorage_path = file:///var/airflow/task-state/

.. note::

  Compression requires the relevant library to be installed in your Python environment.
  For example, ``snappy`` requires ``python-snappy``. Gzip and bz2 work out of the box.

.. note::

  ``expires_at`` is not enforced by this backend. Values written to object storage persist
  indefinitely until explicitly deleted. Use your object storage provider's lifecycle policies
  (e.g. S3 lifecycle rules, GCS object lifecycle) to automatically expire old state.

.. note::

  Task state paths are keyed on ``(dag_id, run_id, task_id, map_index)`` and are stable across
  task retries. This makes this backend suitable for operators that use
  :class:`~airflow.sdk.ResumableJobMixin` to reconnect to external jobs after a retry.
