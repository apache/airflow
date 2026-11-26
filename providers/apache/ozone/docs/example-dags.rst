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

Apache Ozone Example DAGs
=========================

This provider ships example DAGs that demonstrate native Ozone workflows.
Each example DAG now supports two layers of configuration:

* environment variables (used only as defaults during DAG parsing);
* DAG ``params`` / Trigger UI / ``dag_run.conf`` overrides at run time.

In practice this means:

* local dev scripts and CI can continue using ``OZONE_EXAMPLE_*`` environment variables;
* testers can trigger the DAG manually from Airflow UI and override values without editing source code
  or changing worker environment.

Where example DAGs are located
------------------------------

``providers/apache/ozone/src/airflow/providers/apache/ozone/example_dags``

Main examples:

* ``example_ozone_usage.py``
* ``example_ozone_copy_from_hdfs.py``
* ``example_ozone_data_lifecycle.py``
* ``example_ozone_multi_tenant_management.py``

What each example DAG does
--------------------------

``example_ozone_usage.py`` (plain / SSL / Kerberos / SSL+Kerberos):

* Creates a volume and bucket with admin operators.
* Creates an ``ofs://`` directory path in Ozone FS.
* Uploads inline text content to a file in that directory.
* Waits for the file with ``OzoneKeySensor``.
* Overwrites the demo upload target, so the same DAG can be triggered
  repeatedly without manually deleting the previous file.
* Works in all Ozone security modes; pick mode via
  ``admin_conn_id`` / ``OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID`` and corresponding
  ``Connection Extra``.
* Best for first smoke-check of provider installation and CLI connectivity.

``example_ozone_copy_from_hdfs.py`` (HDFS -> Ozone copy):

* Waits for a trigger marker file in Ozone landing zone.
* Creates the trigger marker file after a short delay for standalone demos.
* Applies storage quota to the target bucket.
* Migrates data from HDFS to Ozone with ``HdfsToOzoneOperator`` (DistCp).
* Useful as a template for ingestion pipelines where Ozone is destination storage.

``example_ozone_data_lifecycle.py`` (archive + backup + cleanup):

* Ensures landing/archive volumes and buckets exist.
* Lists files in landing zone with ``OzoneListOperator``.
* Simulates processing stage (``TaskGroup`` with a processing task).
* Moves processed files into date-partitioned archive path.
* Creates a snapshot backup and then cleans original landing files.
* Demonstrates full lifecycle automation: discover -> process -> archive -> backup -> cleanup.

``example_ozone_multi_tenant_management.py`` (provisioning):

* Creates a dedicated project volume.
* Sets volume quota.
* Creates standard landing/processed buckets with bucket quotas.
* Creates standard ``data`` directories in each bucket.
* Useful for platform teams that provision isolated tenant/project storage.

How configuration is applied
----------------------------

Example DAG configuration works in this order:

1. built-in fallback in DAG code;
2. ``OZONE_EXAMPLE_*`` environment variable default (if present);
3. manual override from Trigger UI / ``dag_run.conf`` for a specific DAG run.

The environment-variable layer exists mainly for local bootstrap scripts and CI.
The recommended interactive workflow for testers is:

1. open the DAG in Airflow UI;
2. click ``Trigger DAG``;
3. change values in the generated params form;
4. or paste JSON into ``Trigger DAG with config``.

The underlying implementation uses DAG-level ``params`` and templated operator
fields, so values from ``dag_run.conf`` are rendered into operator arguments
before task execution.

Common variable
---------------

Used by all examples:

* ``OZONE_EXAMPLE_OM_HOST``

Common runtime param:

* ``om_host``

Per-example variables
---------------------

Basic usage (``example_ozone_usage.py``):

Environment defaults:

* ``OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID``
* ``OZONE_EXAMPLE_USAGE_VOLUME``
* ``OZONE_EXAMPLE_USAGE_BUCKET``
* ``OZONE_EXAMPLE_USAGE_DIR``
* ``OZONE_EXAMPLE_USAGE_FILE``
* ``OZONE_EXAMPLE_USAGE_VOLUME_QUOTA``
* ``OZONE_EXAMPLE_USAGE_BUCKET_QUOTA``

Trigger/UI params:

* ``admin_conn_id``
* ``volume``
* ``bucket``
* ``directory``
* ``file_name``
* ``volume_quota``
* ``bucket_quota``

To switch mode for this single usage DAG, point
``OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID`` to a connection configured for:

* plain Ozone;
* Ozone + SSL;
* Ozone + Kerberos;
* Ozone + SSL + Kerberos.

For Kerberos mode, required connection-extra keys include
``hadoop_security_authentication=kerberos``, ``kerberos_principal``,
either ``kerberos_keytab`` or ``kerberos_password``, optional ``krb5_conf``,
and ``ozone_conf_dir``.

Example trigger config:

.. code-block:: json

  {
    "om_host": "adho",
    "admin_conn_id": "ozone_admin_default",
    "volume": "vol1",
    "bucket": "bucket-native",
    "directory": "data_dir",
    "file_name": "file.txt",
    "volume_quota": "10GB",
    "bucket_quota": "10GB"
  }

Ozone copy from HDFS (``example_ozone_copy_from_hdfs.py``):

This DAG models an HDFS -> Ozone copy flow. It waits for ``trigger_file`` with
``OzoneKeySensor`` and, for standalone demos, starts a parallel helper branch
that waits 10 seconds and creates the same trigger file with
``OzoneUploadContentOperator`` using ``if_exists="ignore"``. In a real pipeline
this helper branch can be removed and the trigger file can be produced by an
upstream ingestion job.

Environment defaults:

* ``OZONE_EXAMPLE_COPY_FROM_HDFS_CONN_ID``
* ``OZONE_EXAMPLE_COPY_FROM_HDFS_HDFS_CONN_ID``
* ``OZONE_EXAMPLE_COPY_FROM_HDFS_VOLUME``
* ``OZONE_EXAMPLE_COPY_FROM_HDFS_BUCKET``
* ``OZONE_EXAMPLE_COPY_FROM_HDFS_TRIGGER_FILE``
* ``OZONE_EXAMPLE_COPY_FROM_HDFS_QUOTA``
* ``OZONE_EXAMPLE_COPY_FROM_HDFS_SOURCE_PATH``
* ``OZONE_EXAMPLE_COPY_FROM_HDFS_DEST_SUBPATH``

Trigger/UI params:

* ``pipeline_conn_id``
* ``hdfs_conn_id``
* ``volume``
* ``bucket``
* ``trigger_file``
* ``quota``
* ``source_path``
* ``dest_subpath``

Example trigger config:

.. code-block:: json

  {
    "om_host": "adho",
    "pipeline_conn_id": "ozone_admin_default",
    "hdfs_conn_id": "hdfs_admin_default",
    "volume": "vol1",
    "bucket": "bucket1",
    "trigger_file": "trigger.lck",
    "quota": "5GB",
    "source_path": "hdfs:///user/data/legacy/",
    "dest_subpath": "migrated/"
  }

Data lifecycle (``example_ozone_data_lifecycle.py``):

Environment defaults:

* ``OZONE_EXAMPLE_LIFECYCLE_CONN_ID``
* ``OZONE_EXAMPLE_LIFECYCLE_LANDING_VOLUME``
* ``OZONE_EXAMPLE_LIFECYCLE_LANDING_BUCKET``
* ``OZONE_EXAMPLE_LIFECYCLE_ARCHIVE_VOLUME``
* ``OZONE_EXAMPLE_LIFECYCLE_ARCHIVE_BUCKET``
* ``OZONE_EXAMPLE_LIFECYCLE_SNAPSHOT_PREFIX``

Trigger/UI params:

* ``lifecycle_conn_id``
* ``landing_volume``
* ``landing_bucket``
* ``archive_volume``
* ``archive_bucket``
* ``snapshot_prefix``

Example trigger config:

.. code-block:: json

  {
    "om_host": "adho",
    "lifecycle_conn_id": "ozone_admin_default",
    "landing_volume": "landing",
    "landing_bucket": "raw",
    "archive_volume": "archive",
    "archive_bucket": "processed",
    "snapshot_prefix": "snap"
  }

Multi-tenant management (``example_ozone_multi_tenant_management.py``):

Environment defaults:

* ``OZONE_EXAMPLE_MULTI_TENANT_CONN_ID``
* ``OZONE_EXAMPLE_MULTI_TENANT_PROJECT_VOLUME``
* ``OZONE_EXAMPLE_MULTI_TENANT_PROJECT_QUOTA``
* ``OZONE_EXAMPLE_MULTI_TENANT_LANDING_BUCKET``
* ``OZONE_EXAMPLE_MULTI_TENANT_PROCESSED_BUCKET``
* ``OZONE_EXAMPLE_MULTI_TENANT_BUCKET_QUOTA``

Trigger/UI params:

* ``multi_tenant_conn_id``
* ``project_volume``
* ``project_quota``
* ``landing_bucket``
* ``processed_bucket``
* ``bucket_quota``

Example trigger config:

.. code-block:: json

  {
    "om_host": "adho",
    "multi_tenant_conn_id": "ozone_admin_default",
    "project_volume": "project-alpha",
    "project_quota": "10GB",
    "landing_bucket": "landing",
    "processed_bucket": "processed",
    "bucket_quota": "1GB"
  }

DAG developer notes
-------------------

* Example DAG params and ``OZONE_EXAMPLE_*`` variables are for demonstration only
  and are not part of provider runtime API.
* Production DAGs should keep operational parameters in task args and Airflow Connections.
* Connection parsing for runtime behavior is defined by
  ``airflow.providers.apache.ozone.utils.connection_schema``.
* Custom DAG extensions can read non-provider extra keys through
  ``hook.connection_snapshot.raw_extra``.

Provider runtime tuning (for example runs)
------------------------------------------

Example DAGs use the same provider runtime policy as regular tasks.
Retry/timeout behavior is configured in provider code and per-task arguments:

* Hook defaults: ``RETRY_ATTEMPTS``, ``FAST_TIMEOUT_SECONDS``, ``SLOW_TIMEOUT_SECONDS``
  (see ``airflow.providers.apache.ozone.hooks.ozone``).
* Operators/transfers can override ``retry_attempts`` and ``timeout`` per task
  where needed.
* ``OzoneKeySensor.timeout`` configures one Ozone CLI existence check and is
  stored internally as ``cli_timeout``. It is not forwarded to
  ``BaseSensorOperator.timeout``.
