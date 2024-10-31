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


Google Cloud Storage Transfer Operator to Samba
===============================================

Google has a service `Google Cloud Storage <https://cloud.google.com/storage/>`__.
This service is used to store large data from various applications.
Samba is the standard Windows interoperability suite of programs for Linux and Unix.
Samba has provided secure, stable and fast file and print services for clients using the SMB/CIFS protocol

.. _howto/operator:GCSToSambaOperator:

Operator
^^^^^^^^

Transfer files between Google Storage and Samba is performed with the
:class:`~airflow.providers.samba.transfers.gcs_to_samba.GCSToSambaOperator` operator.

Use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.samba.transfers.gcs_to_samba.GCSToSambaOperator`
to define values dynamically.


Copying a single file
---------------------

The following Operator copies a single file.

.. exampleinclude:: /../../providers/tests/system/samba/example_gcs_to_samba.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_samba_copy_single_file]
    :end-before: [END howto_operator_gcs_to_samba_copy_single_file]

Moving a single file
--------------------

To move the file use the ``move_object`` parameter. Once the file is copied to SMB,
the original file from the Google Storage is deleted. The ``destination_path`` parameter defines the
full path of the file on the Samba server.

.. exampleinclude:: /../../providers/tests/system/samba/example_gcs_to_samba.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_samba_move_single_file_destination]
    :end-before: [END howto_operator_gcs_to_samba_move_single_file_destination]


Copying a directory
-------------------

Use the ``wildcard`` in ``source_path`` parameter to copy a directory.

.. exampleinclude:: /../../providers/tests/system/samba/example_gcs_to_samba.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_samba_copy_directory]
    :end-before: [END howto_operator_gcs_to_samba_copy_directory]

Moving specific files
---------------------

Use the ``wildcard`` in ``source_path`` parameter to move the specific files.
The ``destination_path`` defines the path that is prefixed to all copied files.

.. exampleinclude:: /../../providers/tests/system/samba/example_gcs_to_samba.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_samba_move_specific_files]
    :end-before: [END howto_operator_gcs_to_samba_move_specific_files]
