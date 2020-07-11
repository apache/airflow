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


Google Cloud Storage to Google Drive Transfer Operators
=======================================================

Google has two services that store data. The `Google Cloud Storage <https://cloud.google.com/storage/>`__ is
used to store large data from various applications. The `Google Drive <https://www.google.com/drive/>`__ is
used to store daily use data, including documents and photos. Google Cloud Storage has strong integration
with Google Cloud Platform services. Google Drive has built-in mechanisms to facilitate group work e.g.
document editor, file sharing mechanisms.


.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /howto/operator/gcp/_partials/prerequisite_tasks.rst

.. _howto/operator:GCSToGoogleDriveOperator:

Operator
^^^^^^^^

Transfer files between Google Storage and Google Drive is performed with the
:class:`~airflow.providers.google.suite.transfers.gcs_to_gdrive.GCSToGoogleDriveOperator` operator.

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.suite.transfers.gcs_to_gdrive.GCSToGoogleDriveOperator`
parameters which allows you to dynamically determine values.

Copy single files
-----------------

The following Operator would copy a single file.

.. exampleinclude:: ../../../../airflow/providers/google/suite/example_dags/example_gcs_to_gdrive.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_gdrive_copy_single_file]
    :end-before: [END howto_operator_gcs_to_gdrive_copy_single_file]

Copy multiple files
-------------------

The following Operator would copy all the multiples files (i.e. using wildcard).

.. exampleinclude:: ../../../../airflow/providers/google/suite/example_dags/example_gcs_to_gdrive.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_gdrive_copy_files]
    :end-before: [END howto_operator_gcs_to_gdrive_copy_files]

Move files
----------

Using the ``move_object`` parameter allows you to move the files. After copying the file to Google Drive,
the original file from the bucket is deleted.

.. exampleinclude:: ../../../../airflow/providers/google/suite/example_dags/example_gcs_to_gdrive.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_gdrive_move_files]
    :end-before: [END howto_operator_gcs_to_gdrive_move_files]

Reference
^^^^^^^^^

For further information, look at:

* `Google Drive API Documentation <https://developers.google.com/drive/api/v3/about-sdk>`__
* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
