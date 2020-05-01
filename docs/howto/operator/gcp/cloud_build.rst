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



Google Cloud Build Operators
============================

The `GoogleCloud Build <https://cloud.google.com/cloud-build/>`__ is a service that executes your builds on Google Cloud Platform
infrastructure. Cloud Build can import source code from Google Cloud Storage,
Cloud Source Repositories, execute a build to your specifications, and produce
artifacts such as Docker containers or Java archives.

.. contents::
  :depth: 1
  :local:


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst

.. _howto/operator:CloudBuildBuild:

Build configuration overview
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to trigger a build, it is necessary to pass the build configuration.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_build.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_gcp_create_build_from_storage_body]
    :end-before: [END howto_operator_gcp_create_build_from_storage_body]

The source code for the build can come from `Google Cloud Build Storage <https://cloud.google.com/storage/>`__:

.. literalinclude:: ../../../../tests/providers/google/cloud/operators/test_cloud_build.py
    :language: python
    :dedent: 12
    :start-after: [START howto_operator_gcp_cloud_build_source_gcs_dict]
    :end-before: [END howto_operator_gcp_cloud_build_source_gcs_dict]

It is also possible to specify it using the URL:

.. literalinclude:: ../../../../tests/providers/google/cloud/operators/test_cloud_build.py
    :language: python
    :dedent: 12
    :start-after: [START howto_operator_gcp_cloud_build_source_gcs_url]
    :end-before: [END howto_operator_gcp_cloud_build_source_gcs_url]

In addition, a build can refer to source stored in `Google Cloud Source Repositories <https://cloud.google.com/source-repositories/docs/>`__.

.. literalinclude:: ../../../../tests/providers/google/cloud/operators/test_cloud_build.py
    :language: python
    :dedent: 12
    :start-after: [START howto_operator_gcp_cloud_build_source_repo_dict]
    :end-before: [END howto_operator_gcp_cloud_build_source_repo_dict]

It is also possible to specify it using the URL:

.. literalinclude:: ../../../../tests/providers/google/cloud/operators/test_cloud_build.py
    :language: python
    :dedent: 12
    :start-after: [START howto_operator_gcp_cloud_build_source_repo_url]
    :end-before: [END howto_operator_gcp_cloud_build_source_repo_url]

Read `Build Configuration Overview <https://cloud.google.com/cloud-build/docs/build-config>`__ to understand all the fields you can include in a build config file.


.. _howto/operator:CloudBuildCreateOperator:

Trigger a build
^^^^^^^^^^^^^^^

Trigger a build is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_from_storage]
    :end-before: [END howto_operator_create_build_from_storage]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_from_storage_result]
    :end-before: [END howto_operator_create_build_from_storage_result]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://developers.google.com/resources/api-libraries/documentation/cloudbuild/v1/python/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/natural-language/docs/>`__
