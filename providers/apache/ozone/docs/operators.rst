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

Apache Ozone Operators
======================

The Ozone provider is CLI-first: operators delegate most runtime work to
``OzoneFsHook`` or ``OzoneAdminHook`` and execute native ``ozone`` commands.

Prerequisites
-------------

To use these operators, configure an :doc:`Ozone Connection <connections>`
and make sure the worker runtime has:

* Java runtime available to the worker.
* ``ozone`` CLI available in ``PATH``.
* ``ozone-site.xml`` and ``core-site.xml`` reachable through ``ozone_conf_dir``
  or otherwise available in the worker runtime.

Common operator parameters
--------------------------

Most operators accept:

* ``ozone_conn_id``: Airflow connection ID. Defaults to ``ozone_default`` for
  filesystem operations and ``ozone_admin_default`` for administrative
  operations.
* ``retry_attempts``: number of CLI retries for transient failures.
* ``timeout``: timeout in seconds for the underlying CLI command.
* ``if_exists`` on create/upload/copy/move operators: explicit
  ``ExistingTargetPolicy`` for an already existing destination. DAG code may pass
  the enum values or their string forms. Supported values are ``error`` and
  ``ignore``; upload operators also support ``overwrite``.

Administrative operators
------------------------

These operators use ``OzoneAdminHook`` and work with Ozone volumes, buckets,
and quotas.

* ``OzoneCreateVolumeOperator``: create a volume if it does not exist.
* ``OzoneCreateBucketOperator``: create a bucket inside a target volume.
* ``OzoneSetQuotaOperator``: apply a quota to a volume or bucket.
* ``OzoneDeleteVolumeOperator``: delete a volume, optionally recursively.
* ``OzoneDeleteBucketOperator``: delete a bucket, optionally recursively.

Example:

.. code-block:: python

    from airflow.providers.apache.ozone.operators.ozone import (
        OzoneCreateBucketOperator,
        OzoneCreateVolumeOperator,
        OzoneSetQuotaOperator,
    )

    create_volume = OzoneCreateVolumeOperator(
        task_id="create_volume",
        volume_name="analytics",
        ozone_conn_id="ozone_admin_default",
    )

    create_bucket = OzoneCreateBucketOperator(
        task_id="create_bucket",
        volume_name="analytics",
        bucket_name="landing",
        ozone_conn_id="ozone_admin_default",
    )

    set_quota = OzoneSetQuotaOperator(
        task_id="set_quota",
        volume="analytics",
        bucket="landing",
        quota="20GB",
        ozone_conn_id="ozone_admin_default",
    )

Filesystem operators
--------------------

These operators use ``OzoneFsHook`` and work with ``ofs://`` or ``o3fs://``
paths.

Path and object creation:

* ``OzoneCreatePathOperator``: create a directory path in Ozone FS.
* ``OzoneUploadContentOperator``: write inline text content to a temporary file
  and upload it to Ozone.
* ``OzoneUploadFileOperator``: upload a local file to Ozone.

Existing-target behavior is intentionally explicit:

* ``if_exists="error"`` fails before running a potentially expensive CLI write
  when the destination already exists.
* ``if_exists="ignore"`` treats an existing destination as success, which is
  useful for idempotent marker files and setup paths.
* ``if_exists="overwrite"`` is available for uploads only. The provider deletes
  the existing key first and then writes through ``ozone sh key put``. Directory
  creation, copy, and move operations do not support overwrite to avoid
  accidental data loss.

For hook-level path creation, new code should use
``OzoneFsHook.make_path(..., if_exists=...)``. The older
``OzoneFsHook.create_path(..., fail_if_exists=...)`` API is kept for backwards
compatibility only and logs a deprecation warning before delegating to
``make_path``.

Path and object removal:

* ``OzoneDeleteKeyOperator``: delete one key or a wildcard-selected key set.
* ``OzoneDeletePathOperator``: delete a path, optionally recursively.

Path inspection:

* ``OzonePathExistsOperator``: return ``True`` or ``False`` via XCom depending
  on path existence.
* ``OzoneListOperator``: list keys/paths and return the result via XCom.

In-cluster file movement:

* ``OzoneMoveOperator``: move or rename a key/path inside Ozone.
* ``OzoneCopyOperator``: copy a key/path inside Ozone.
* ``OzoneDownloadFileOperator``: download a remote key to local filesystem.

Example:

.. code-block:: python

    from airflow.providers.apache.ozone.operators.ozone import (
        OzoneCreatePathOperator,
        OzoneListOperator,
        OzoneUploadContentOperator,
    )

    create_path = OzoneCreatePathOperator(
        task_id="create_path",
        path="ofs://om-service/analytics/landing/incoming",
        ozone_conn_id="ozone_default",
        if_exists="ignore",
    )

    upload_marker = OzoneUploadContentOperator(
        task_id="upload_marker",
        content="ready",
        remote_path="ofs://om-service/analytics/landing/incoming/_SUCCESS",
        ozone_conn_id="ozone_default",
        if_exists="error",
    )

    list_files = OzoneListOperator(
        task_id="list_files",
        path="ofs://om-service/analytics/landing/incoming/*",
        ozone_conn_id="ozone_default",
    )

Size-aware upload and download
------------------------------

``OzoneUploadContentOperator``, ``OzoneUploadFileOperator``, and
``OzoneDownloadFileOperator`` support ``max_content_size_bytes``.

If this parameter is not passed explicitly, the operator uses the
connection-level limit from ``OzoneConnSnapshot.max_content_size_bytes``.
This helps protect workers from unexpectedly large payloads.

Design notes
------------

The operators intentionally stay thin:

* validation and CLI execution live in hooks and utils;
* SSL and Kerberos wiring come from the connection contract;
* operator code focuses on task arguments and Airflow integration.
