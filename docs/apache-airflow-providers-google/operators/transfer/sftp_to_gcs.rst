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


SFTP to Google Cloud Storage Transfer Operator
==============================================

Google has a service `Google Cloud Storage <https://cloud.google.com/storage/>`__. This service is
used to store large data from various applications.
SFTP (SSH File Transfer Protocol) is a secure file transfer protocol.
It runs over the SSH protocol. It supports the full security and authentication functionality of the SSH.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:SFTPToGCSOperator:

Operator
^^^^^^^^

Transfer files between SFTP and Google Storage is performed with the
:class:`~airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPToGCSOperator` operator.

Use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.transfers.sftp_to_gcs.SFTPToGCSOperator`
to define values dynamically.

Copying single files
--------------------

The following Operator copies a single file.

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_sftp_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sftp_to_gcs_copy_single_file]
    :end-before: [END howto_operator_sftp_to_gcs_copy_single_file]

Moving a single file
--------------------

To move the file use the ``move_object`` parameter. Once the file is copied to Google Storage,
the original file from the SFTP is deleted.
The ``destination_path`` parameter defines the full path of the file in the bucket.

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_sftp_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sftp_to_gcs_move_single_file_destination]
    :end-before: [END howto_operator_sftp_to_gcs_move_single_file_destination]


Copying directory
-----------------

Use the ``wildcard`` in ``source_path`` parameter to copy the directory.

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_sftp_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sftp_to_gcs_copy_directory]
    :end-before: [END howto_operator_sftp_to_gcs_copy_directory]

Moving specific files
---------------------

Use the ``wildcard`` in ``source_path`` parameter to move the specific files.
You can use only one wildcard within your path.
The ``destination_path`` defines the path that is prefixed to all copied files,
e.g. ``tests_sftp_hook_dir/subdir/parent-1.bin`` is copied to ``specific_files/parent-1.bin``,
and ``tests_sftp_hook_dir/subdir/parent-2.bin`` is copied to ``specific_files/parent-2.bin`` .
``tests_sftp_hook_dir/subdir/parent-3.txt`` is skipped.

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_sftp_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sftp_to_gcs_move_specific_files]
    :end-before: [END howto_operator_sftp_to_gcs_move_specific_files]

Stream-Based File Transfer
--------------------------------------

The ``SFTPToGCSOperator`` now supports more advanced options for file transfer, 
particularly useful for handling large files or optimizing network usage. 
These include stream-based file transfers and additional parameters for finer control.

1. Stream-Based File Transfer: When ``use_stream`` is set to ``True``, 
files are streamed directly from SFTP to GCS, without being stored temporarily on the worker's local disk. 
This method is more efficient for large files or in environments with limited disk space.

2. Enhanced Parameters for Streaming:
    - ``sftp_prefetch``: Optimizes file transfer speed when using the ``"getfo"`` stream method.
    - ``stream_method``: Choose between ``"upload_from_file"`` for reliability or ``"getfo"`` for speed and efficiency.
    - ``max_concurrent_prefetch_requests``: Control the concurrency level for prefetching in the ``"getfo"`` method.
    - ``callback``: Provide a custom callback function for progress tracking during file transfer.

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_sftp_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sftp_to_gcs_copy_single_file_with_stream]
    :end-before: [END howto_operator_sftp_to_gcs_copy_single_file_with_stream]

Reference
^^^^^^^^^

For more information, see

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
