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


HTTP to Google Cloud Storage Transfer Operator
==============================================

Google has a service `Google Cloud Storage <https://cloud.google.com/storage/>`__. This service is
used to store large data from various applications.
HTTP (Hypertext Transfer Protocol) HTTP is an application layer protocol
designed to transfer information between networked devices
and runs on top of other layers of the network protocol stack.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:HttpToGCSOperator:

Operator
^^^^^^^^

Transfer files between HTTP and Google Storage is performed with the
:class:`~airflow.providers.google.cloud.transfers.http_to_gcs.HttpToGCSOperator` operator.

Use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.transfers.http_to_gcs.HttpToGCSOperator`
to define values dynamically.

Copying single files
--------------------

The following Operator copies a single file.

.. exampleinclude:: /../../google/tests/system/google/cloud/gcs/example_http_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_http_to_gcs]
    :end-before: [END howto_transfer_http_to_gcs]

Reference
^^^^^^^^^

For more information, see

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
