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



Google Cloud Firestore Operators
================================

`Cloud Firestore <https://firebase.google.com/docs/firestore>`__ is a flexible, scalable database for mobile,
web, and server development from Firebase and Google Cloud. Like Firebase Realtime Database, it keeps
your data in sync across client apps through realtime listeners and offers offline support for mobile and web
so you can build responsive apps that work regardless of network latency or Internet connectivity. Cloud
Firestore also offers seamless integration with other Firebase and Google Cloud products, including
Cloud Functions.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst


.. _howto/operator:CloudFirestoreExportDatabaseOperator:

Export database
^^^^^^^^^^^^^^^

Exports a copy of all or a subset of documents from Google Cloud Firestore to Google Cloud Storage is performed with the
:class:`~airflow.providers.google.firebase.operators.firestore.CloudFirestoreExportDatabaseOperator` operator.

.. exampleinclude:: /../../airflow/providers/google/firebase/example_dags/example_firestore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_export_database_to_gcs]
    :end-before: [END howto_operator_export_database_to_gcs]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.firebase.operators.firestore.CloudFirestoreExportDatabaseOperator`
parameters which allows you to dynamically determine values.


Reference
^^^^^^^^^

For further information, look at:

* `Product Documentation <https://firebase.google.com/docs/firestore>`__
* `Client Library Documentation <http://googleapis.github.io/google-api-python-client/docs/dyn/firestore_v1.html>`__
