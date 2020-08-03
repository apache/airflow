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

Transfer data in Google Cloud Storage
=====================================

The `Google Cloud Storage <https://cloud.google.com/storage/>`__  (GCS) is used to store large data from various applications.
Note that files are called objects in GCS terminology, so the use of the term "object" and "file" in this guide is
interchangeable. There are several operators for whose purpose is to copy data as part of the Google CLoud Service.
This page shows how to use these operators.

Overview
--------

Cloud Storage Transfer Service
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are many operators that manage the Google Cloud Data Transfer service. If you want to create a new data transfer
task, use the operator
:class:`~airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator`
You can also use the previous operator for this service -
:class:`~airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceGCSToGCSOperator`

These operators do not control the copying process locally, but uses Google resources, which allows them to
perform this task faster and more economically. The economic effects are especially prominent when
Airflow is not hosted in Google Cloud Platform, because these operators reduce egress traffic.

These operators modify source objects if the option that specifies whether objects should be deleted
from the source after they are transferred to the sink is enabled.

When you use the Google Cloud Data Transfer service, you can specify whether overwriting objects that already exist in
the sink is allowed, whether objects that exist only in the sink should be deleted, or whether objects should be deleted
from the source after they are transferred to the sink.

Source objects can be specified using include and exclusion prefixes, as well as based on the file
modification date.

If you need information on how to use it, look at the guide: :doc:`/howto/operator/google/cloud/cloud_storage_transfer_service`

Local transfer
~~~~~~~~~~~~~~

There are two operators that are used to copy data, where the entire process is controlled locally.

In the next section they will be described.

Prerequisite Tasks
------------------

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst


Operators
---------

.. _howto/operator:GCSToGCSOperator:

GCSToGCSOperator
~~~~~~~~~~~~~~~~

:class:`~airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSToGCSOperator` allows you to copy
one or more files within GCS. The files may be copied between two different buckets or within one bucket.
The copying always takes place without taking into account the initial state of the destination bucket.

This operator only deletes objects in the source bucket if the file move option is active. When copying files
between two different buckets, this operator never deletes data in the destination bucket.

When you use this operator, you can specify whether objects should be deleted from the source after
they are transferred to the sink. Source objects can be specified using a single wildcard, as
well as based on the file modification date.

The way this operator works by default can be compared to the ``cp`` command. When the file move option is active, this
operator functions like the ``mv`` command.

Below are examples of using the GCSToGCSOperator to copy a single file, to copy multiple files with a wild card,
to copy multiple files, to move a single file, and to move multiple files.

Copy single file
----------------

The following example would copy a single file, ``OBJECT_1`` from the ``BUCKET_1_SRC`` GCS bucket to the ``BUCKET_1_DST`` bucket.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_gcs_single_file]
    :end-before: [END howto_operator_gcs_to_gcs_single_file]

Copy multiple files
-------------------

There are several ways to copy multiple files, various examples of which are presented following.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_gcs_wildcard]
    :end-before: [END howto_operator_gcs_to_gcs_wildcard]

The ``source_object`` value may contain one wild card, denoted as "*". All files matching the wild card expression will
be copied. In this example, all root level files ending with ``.txt`` in ``BUCKET_1_SRC`` will be copied to the ``data``
folder in ``BUCKET_1_DST``, with file names unchanged.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_gcs_delimiter]
    :end-before: [END howto_operator_gcs_to_gcs_delimiter]

The delimiter filed may be specified to select any source files starting with ``source_object`` and ending with the
value supplied to ``delimiter``. This example uses the ``delimiter`` value to implement the same functionality as the
prior example.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_gcs_list]
    :end-before: [END howto_operator_gcs_to_gcs_list]

Lastly, files may be copied by omitting the ``source_object`` argument and instead supplying a list to ``source_objects``
argument. In this example, ``OBJECT_1`` and ``OBJECT_2`` will be copied from ``BUCKET_1_SRC`` to ``BUCKET_1_DST``. Instead
of specific file names, the list can contain one or more wild card expressions, each with no more than one wild card.
Supplying a list of size 1 functions the same as supplying a value to the ``source_object`` argument.

Move single file
----------------

Supplying ``True`` to the ``move`` argument causes the operator to delete ``source_object`` once the copy is complete.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_gcs_single_file_move]
    :end-before: [END howto_operator_gcs_to_gcs_single_file_move]

Move multiple files
-------------------

Multiple files may be moved by supplying ``True`` to the ``move`` argument. The same rules concerning wild cards and
the ``delimiter`` argument apply to moves as well as copies.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_gcs_list_move]
    :end-before: [END howto_operator_gcs_to_gcs_list_move]


.. _howto/operator:GCSSynchronizeBuckets:

GCSSynchronizeBuckets
~~~~~~~~~~~~~~~~~~~~~

The :class:`~airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSSynchronizeBuckets`
operator checks the initial state of the destination bucket, and then compares it with the source bucket.
Based on this, it creates an operation plan that describes which objects should be deleted from
the destination bucket, which should be overwritten, and which should be copied.

This operator never modifies data in the source bucket.

When you use this operator, you can specify whether
overwriting objects that already exist in the sink is allowed, whether
objects that exist only in the sink should be deleted, whether subdirectories are to be processed or
which subdirectory is to be processed.

The way this operator works can be compared to the ``rsync`` command.

Basic Synchronization
---------------------

The following example will ensure all files in ``BUCKET_1_SRC``, including any in subdirectories, are also in
``BUCKET_1_DST``. It will not overwrite identically named files in ``BUCKET_1_DST`` if they already exist. It will not
delete any files in ``BUCKET_1_DST`` not in ``BUCKET_1_SRC``.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_synch_bucket]
    :end-before: [END howto_synch_bucket]

Full Bucket Synchronization
---------------------------

This example will ensure all files in ``BUCKET_1_SRC``, including any in subdirectories, are also in
``BUCKET_1_DST``. It will overwrite identically named files in ``BUCKET_1_DST`` if they already exist. It will
delete any files in ``BUCKET_1_DST`` not in ``BUCKET_1_SRC``.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_synch_full_bucket]
    :end-before: [END howto_synch_full_bucket]

Synchronize to a Subdirectory
-----------------------------

The following example will ensure all files in ``BUCKET_1_SRC``, including any in subdirectories, are also in the
``subdir`` folder in ``BUCKET_1_DST``. It will not overwrite identically named files in ``BUCKET_1_DST/subdir`` if they
already exist and it will not delete any files in ``BUCKET_1_DST/subdir`` not in ``BUCKET_1_SRC``.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_synch_to_subdir]
    :end-before: [END howto_synch_to_subdir]

Synchronize from a Subdirectory
-------------------------------

This example will ensure all files in ``BUCKET_1_SRC/subdir``, including any in subdirectories, are also in the
in ``BUCKET_1_DST``. It will not overwrite identically named files in ``BUCKET_1_DST`` if they
already exist and it will not delete any files in ``BUCKET_1_DST`` not in ``BUCKET_1_SRC/subdir``.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_gcs_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sync_from_subdir]
    :end-before: [END howto_sync_from_subdir]

Reference
---------

For further information, look at:

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
* `Google Cloud Storage Utilities rsync command <https://cloud.google.com/storage/docs/gsutil/commands/rsync>`__
