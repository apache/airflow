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


Google Cloud Storage Transfer Operator to SFTP
==============================================

Google has service `Google Cloud Storage <https://cloud.google.com/storage/>`__ that is
used to store large data from various applications.
SFTP (SSH File Transfer Protocol) is a secure file transfer protocol.
It runs over the SSH protocol. It supports the full security and authentication functionality of SSH.


.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst

.. _howto/operator:GoogleCloudStorageToSFTPOperator:


Operator
^^^^^^^^

Transfer files between SFTP and Google Storage is performed with the
:class:`~airflow.operators.gcs_to_sftp.GoogleCloudStorageToSFTPOperator` operator.

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.operators.gcs_to_sftp.GoogleCloudStorageToSFTPOperator`
parameters which allows you to dynamically determine values.


Arguments
^^^^^^^^^

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/example_dags/example_gcs_to_sftp.py
    :language: python
    :start-after: [START howto_operator_gcs_to_sftp_args_common]
    :end-before: [END howto_operator_gcs_to_sftp_args_common]


Copy single files
-----------------

The following Operator would copy a single file.

.. exampleinclude:: ../../../../airflow/example_dags/example_gcs_to_sftp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_sftp_copy_single_file]
    :end-before: [END howto_operator_gcs_to_sftp_copy_single_file]

Move single file
----------------

Using the ``move_object`` parameter allows you to move the file. After copying the file to SFTP,
the original file from the Google Storage is deleted. Using the ``destination_path`` defines the path to which file
will be present on the SFTP server.

.. exampleinclude:: ../../../../airflow/example_dags/example_gcs_to_sftp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_sftp_move_single_file_destination]
    :end-before: [END howto_operator_gcs_to_sftp_move_single_file_destination]


Copy directory
--------------

Using the ``wildcard`` in ``source_object`` parameter allows you to copy the directory.

.. exampleinclude:: ../../../../airflow/example_dags/example_gcs_to_sftp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_sftp_copy_directory]
    :end-before: [END howto_operator_gcs_to_sftp_copy_directory]

Move specific files
-------------------

Using the ``wildcard`` in ``source_object`` parameter allows you to move the specific files.
Using the ``destination_path`` defines the path which will be prefixed to all copied files.

.. exampleinclude:: ../../../../airflow/example_dags/example_gcs_to_sftp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_sftp_move_specific_files]
    :end-before: [END howto_operator_gcs_to_sftp_move_specific_files]

Reference
^^^^^^^^^

For further information, look at:

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
