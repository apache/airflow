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

The `Google Cloud Build <https://cloud.google.com/cloud-build/>`__ is a service that executes your builds on Google Cloud Platform
infrastructure. Cloud Build can import source code from Google Cloud Storage,
Cloud Source Repositories, execute a build to your specifications, and produce
artifacts such as Docker containers or Java archives.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudBuildCancelBuildOperator:

CloudBuildCancelBuildOperator
-----------------------------

Cancels a build in progress.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildCancelBuildOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Cancel a build in progress with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildCancelBuildOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cancel_build]
    :end-before: [END howto_operator_cancel_build]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildCancelBuildOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. _howto/operator:CloudBuildCreateBuildOperator:

CloudBuildCreateBuildOperator
-----------------------------

Starts a build with the specified configuration. This generated build ID of the created build, as the result of this operator,
is not idempotent.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildOperator`

Build configuration
^^^^^^^^^^^^^^^^^^^

In order to trigger a build, it is necessary to pass the build configuration.


.. exampleinclude:: /../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_gcp_create_build_from_storage_body]
    :end-before: [END howto_operator_gcp_create_build_from_storage_body]

In addition, a build can refer to source stored in `Google Cloud Source Repositories <https://cloud.google.com/source-repositories/docs/>`__.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_create_build_from_repo_body]
    :end-before: [END howto_operator_create_build_from_repo_body]

Read `Build Configuration Overview <https://cloud.google.com/cloud-build/docs/build-config>`__ to understand all the fields you can include in a build config file.

Using the operator
^^^^^^^^^^^^^^^^^^

Trigger a build is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_from_storage]
    :end-before: [END howto_operator_create_build_from_storage]

You can use deferrable mode for this action in order to run the operator asynchronously:

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_from_storage_async]
    :end-before: [END howto_operator_create_build_from_storage_async]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_from_storage_result]
    :end-before: [END howto_operator_create_build_from_storage_result]

By default, after the build is created, it will wait for the build operation to complete. If there is no need to wait for complete,
you can pass wait=False as example shown below.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_without_wait]
    :end-before: [END howto_operator_create_build_without_wait]

You can use deferrable mode for this action in order to run the operator asynchronously:

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_without_wait_async]
    :end-before: [END howto_operator_create_build_without_wait_async]

In order to start a build on Cloud Build you can use a build configuration file. A build config file defines the fields
that are needed for Cloud Build to perform your tasks. You can write the build config file using the YAML or the JSON syntax.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_create_build_from_yaml_body]
    :end-before: [END howto_operator_gcp_create_build_from_yaml_body]

You can use deferrable mode for this action in order to run the operator asynchronously:

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_create_build_from_yaml_body_async]
    :end-before: [END howto_operator_gcp_create_build_from_yaml_body_async]

In addition, a Cloud Build can refer to source stored in `Google Cloud Source Repositories <https://cloud.google.com/source-repositories/docs/>`__.
Once build has started, it ill build the code in source repositories.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_create_build_from_repo]
    :end-before: [END howto_operator_create_build_from_repo]

You can use deferrable mode for this action in order to run the operator asynchronously:

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_from_repo_async]
    :end-before: [END howto_operator_create_build_from_repo_async]

.. _howto/operator:CloudBuildCreateBuildTriggerOperator:

CloudBuildCreateBuildTriggerOperator
------------------------------------

Creates a new Cloud Build trigger. This generated build trigger ID of the created build trigger, as the result of this operator,
is not idempotent.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildTriggerOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Creates a new Cloud Build trigger with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildTriggerOperator` operator.

.. exampleinclude:: /../../providers/tests/system/google/cloud/cloud_build/example_cloud_build_trigger.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_trigger]
    :end-before: [END howto_operator_create_build_trigger]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildTriggerOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. _howto/operator:CloudBuildDeleteBuildTriggerOperator:

CloudBuildDeleteBuildTriggerOperator
------------------------------------

Deletes a Cloud Build trigger by its project ID and trigger ID.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildDeleteBuildTriggerOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Deletes a new Cloud Build trigger with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildDeleteBuildTriggerOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build_trigger.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_build_trigger]
    :end-before: [END howto_operator_delete_build_trigger]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildDeleteBuildTriggerOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. _howto/operator:CloudBuildGetBuildOperator:

CloudBuildGetBuildOperator
--------------------------

Returns information about a previously requested build.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildGetBuildOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Returns information about a previously requested build with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildGetBuildOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_get_build]
    :end-before: [END howto_operator_get_build]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildGetBuildOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. _howto/operator:CloudBuildGetBuildTriggerOperator:

CloudBuildGetBuildTriggerOperator
---------------------------------

Returns information about a Cloud Build trigger.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildGetBuildTriggerOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Returns information about a Cloud Build trigger with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildGetBuildTriggerOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build_trigger.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_get_build_trigger]
    :end-before: [END howto_operator_get_build_trigger]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildGetBuildTriggerOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. _howto/operator:CloudBuildListBuildTriggersOperator:

CloudBuildListBuildTriggersOperator
-----------------------------------

Lists all the existing Cloud Build triggers.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildListBuildTriggersOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Lists all the existing Cloud Build triggers with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildListBuildTriggersOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build_trigger.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_list_build_triggers]
    :end-before: [END howto_operator_list_build_triggers]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildListBuildTriggersOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. _howto/operator:CloudBuildListBuildsOperator:

CloudBuildListBuildsOperator
----------------------------

Lists previously requested builds.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildListBuildsOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Lists previously requested builds with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildListBuildsOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_list_builds]
    :end-before: [END howto_operator_list_builds]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildListBuildsOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. _howto/operator:CloudBuildRetryBuildOperator:

CloudBuildRetryBuildOperator
----------------------------

Creates a new build based on the specified build. This method creates a new build
using the original build request, which may or may not result in an identical build.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildRetryBuildOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Creates a new build based on the specified build with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildRetryBuildOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_retry_build]
    :end-before: [END howto_operator_retry_build]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildRetryBuildOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. _howto/operator:CloudBuildRunBuildTriggerOperator:

CloudBuildRunBuildTriggerOperator
---------------------------------

Runs a trigger at a particular source revision.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildRunBuildTriggerOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Runs a trigger at a particular source revision with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildRunBuildTriggerOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build_trigger.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_run_build_trigger]
    :end-before: [END howto_operator_run_build_trigger]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildRunBuildTriggerOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. _howto/operator:CloudBuildUpdateBuildTriggerOperator:

CloudBuildUpdateBuildTriggerOperator
------------------------------------

Updates a Cloud Build trigger by its project ID and trigger ID.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildUpdateBuildTriggerOperator`

Using the operator
^^^^^^^^^^^^^^^^^^

Updates a Cloud Build trigger with the
:class:`~airflow.providers.google.cloud.operators.cloud_build.CloudBuildUpdateBuildTriggerOperator` operator.

.. exampleinclude:: ../../../../providers/tests/system/google/cloud/cloud_build/example_cloud_build_trigger.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_build_trigger]
    :end-before: [END howto_operator_create_build_trigger]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_build.CloudBuildUpdateBuildTriggerOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/cloudbuild/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/natural-language/docs/>`__
