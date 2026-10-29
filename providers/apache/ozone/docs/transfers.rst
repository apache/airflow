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

Apache Ozone Transfers
======================

The provider currently exposes two transfer-oriented building blocks:

* ``HdfsToOzoneOperator`` for bulk migration from HDFS to Ozone with
  ``hadoop distcp``;
* ``OzoneBackupOperator`` for bucket snapshot creation as a backup or
  lifecycle checkpoint step.

HdfsToOzoneOperator
-------------------

``HdfsToOzoneOperator`` migrates data from HDFS to Ozone using native
``hadoop distcp``.

Typical use cases:

* initial migration from HDFS to Ozone;
* ingestion pipelines where Ozone is the destination storage;
* bulk copy of large path trees that should stay outside Python memory.

Runtime notes:

* the worker must have ``hadoop`` in ``PATH``;
* the operator validates runtime dependencies before starting DistCp;
* HDFS SSL and Kerberos settings are read from ``hdfs_conn_id`` and applied
  only to the DistCp subprocess environment;
* secure workers without YARN/MapReduce cluster configuration can set
  ``hdfs_distcp_mapreduce_local`` and ``hdfs_distcp_renewer_principal`` in
  ``hdfs_conn_id`` extra so DistCp can acquire HDFS/Ozone delegation tokens
  and run through the local MapReduce runner;
* if ``hdfs_conn_id`` is omitted, no HDFS-specific Airflow connection settings
  are injected and DistCp relies on the worker's local Hadoop runtime setup;
* a dedicated ``apache-airflow-providers-apache-hdfs`` package is not required
  by this operator implementation;
* Ozone destination access is resolved by the Hadoop/Ozone client runtime
  available on the worker, not by a dedicated ``ozone_conn_id`` argument in
  this operator.

Example:

.. code-block:: python

    from airflow.providers.apache.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator

    migrate_to_ozone = HdfsToOzoneOperator(
        task_id="migrate_to_ozone",
        source_path="hdfs:///warehouse/source_table",
        dest_path="ofs://om-service/analytics/landing/source_table",
        hdfs_conn_id="hdfs_admin_default",
    )

OzoneBackupOperator
-------------------

``OzoneBackupOperator`` creates a bucket snapshot through native Ozone admin
CLI commands.

Typical use cases:

* create a recovery point before cleanup;
* preserve a landing bucket before archive/move operations;
* add an idempotent safety checkpoint to lifecycle DAGs.

Behavior notes:

* ``volume``, ``bucket``, and ``snapshot_name`` are templated;
* if the target snapshot already exists, the operator treats it as success;
* runtime and authentication are handled through ``ozone_admin_default`` or a
  custom admin connection.

Example:

.. code-block:: python

    from airflow.providers.apache.ozone.transfers.ozone_backup import OzoneBackupOperator

    create_snapshot = OzoneBackupOperator(
        task_id="create_snapshot",
        volume="analytics",
        bucket="landing",
        snapshot_name="landing_{{ ts_nodash }}",
        ozone_conn_id="ozone_admin_default",
    )
