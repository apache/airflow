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


Google Drive to Google Cloud Storage Transfer Operator
=======================================================

Google has two services that store data. The `Google Cloud Storage <https://cloud.google.com/storage/>`__ is
used to store large data from various applications. The `Google Drive <https://www.google.com/drive/>`__ is
used to store daily use data, including documents and photos. Google Cloud Storage has strong integration
with Google Cloud services. Google Drive has built-in mechanisms to facilitate group work e.g.
document editor, file sharing mechanisms.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleDriveToGCSOperator:

Operator
^^^^^^^^

Transfer files between Google Storage and Google Drive is performed with the
:class:`~airflow.providers.google.cloud.transfers.gdrive_to_gcs.GoogleDriveToGCSOperator` operator.


Copy single files
-----------------

The following Operator copies a single file from a shared Google Drive folder to a Google Cloud Storage Bucket.

Note that you can transfer a file from the root folder of a shared drive by passing the id of the shared
drive to both the ``folder_id`` and ``drive_id`` parameters.

.. exampleinclude:: /../../providers/tests/system/google/cloud/gcs/example_gdrive_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START upload_gdrive_to_gcs]
    :end-before: [END upload_gdrive_to_gcs]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.transfers.gdrive_to_gcs.GoogleDriveToGCSOperator`
parameters which allows you to dynamically determine values.

Reference
^^^^^^^^^

For further information, look at:

* `Google Drive API Documentation <https://developers.google.com/drive/api/v3/about-sdk>`__
* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
