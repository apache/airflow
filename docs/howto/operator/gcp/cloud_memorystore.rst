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



Google Cloud Memorystore Operators
==================================

The `Cloud Memorystore for Redis <https://cloud.google.com/memorystore/docs/redis/>`__ is a fully managed
Redis service for the Google Cloud Platform. Applications running on Google Cloud Platform can achieve
extreme performance by leveraging the highly scalable, available, secure Redis service without the burden
of managing complex Redis deployments.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst


.. _howto/operator:CloudMemorystoreInstance:

Instance
^^^^^^^^

Operators uses a :class:`~google.cloud.redis_v1.types.Instance` for representing instance. The object can be
presented as a compatibile dictonary also.

Here is an example of instance

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :start-after: [START howto_operator_instance]
    :end-before: [END howto_operator_instance]

.. _howto/operator:CloudMemorystoreBucketPermission:

Configuration of bucket permissions for import / export
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is necessary to configure permissions for the bucket to import and export data. Too find the service
account for your instance, run the :class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreCreateInstanceOperator` or
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreGetInstanceOperator` and
make a use of the service account listed under ``persistenceIamIdentity``.

You can use :class:`~airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator`
operator to set permissions.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_set_acl_permission]
    :end-before: [END howto_operator_set_acl_permission]

For futher information look at: `Granting restricted permissions for import and export
<https://cloud.google.com/memorystore/docs/redis/import-export-restricted-permissions>`__

.. _howto/operator:CloudMemorystoreCreateInstanceOperator:

Create instance
^^^^^^^^^^^^^^^

Create a instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreCreateInstanceOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_instance]
    :end-before: [END howto_operator_create_instance]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreCreateInstanceOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_instance_result]
    :end-before: [END howto_operator_create_instance_result]


.. _howto/operator:CloudMemorystoreDeleteInstanceOperator:

Delete instance
^^^^^^^^^^^^^^^

Delete a instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreDeleteInstanceOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_instance]
    :end-before: [END howto_operator_delete_instance]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreDeleteInstanceOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudMemorystoreExportInstanceOperator:

Export instance
^^^^^^^^^^^^^^^

Delete a instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreExportInstanceOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_export_instance]
    :end-before: [END howto_operator_export_instance]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreExportInstanceOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudMemorystoreFailoverInstanceOperator:

Failover instance
^^^^^^^^^^^^^^^^^

Delete a instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreFailoverInstanceOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_failover_instance]
    :end-before: [END howto_operator_failover_instance]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreFailoverInstanceOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudMemorystoreGetInstanceOperator:

Get instance
^^^^^^^^^^^^

Delete a instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreGetInstanceOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_get_instance]
    :end-before: [END howto_operator_get_instance]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreGetInstanceOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudMemorystoreImportOperator:

Import instance
^^^^^^^^^^^^^^^

Delete a instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreImportOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_import_instance]
    :end-before: [END howto_operator_import_instance]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreImportOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudMemorystoreListInstancesOperator:

List instances
^^^^^^^^^^^^^^

List a instances is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreListInstancesOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_list_instances]
    :end-before: [END howto_operator_list_instances]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreListInstancesOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`, which allows it
to be used by other operators.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_list_instances_result]
    :end-before: [END howto_operator_list_instances_result]

.. _howto/operator:CloudMemorystoreUpdateInstanceOperator:

Update instance
^^^^^^^^^^^^^^^

Update a instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreUpdateInstanceOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_update_instance]
    :end-before: [END howto_operator_update_instance]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreUpdateInstanceOperator`
parameters which allows you to dynamically determine values.


.. _howto/operator:CloudMemorystoreScaleInstanceOperator:

Scale instance
^^^^^^^^^^^^^^

Scale a instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreScaleInstanceOperator` operator.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_scale_instance]
    :end-before: [END howto_operator_scale_instance]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreScaleInstanceOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudMemorystoreCreateInstanceAndImportOperator:

Create instance and import
^^^^^^^^^^^^^^^^^^^^^^^^^^

If you want to create instances with imported data, you can use
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreCreateInstanceAndImportOperator` operator.

.. _howto/operator:CloudMemorystoreExportAndDeleteInstanceOperator:

Export and delete instance
^^^^^^^^^^^^^^^^^^^^^^^^^^

If you want to export data and immediately delete instances then you can use
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreExportAndDeleteInstanceOperator` operator.

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreScaleInstanceOperator`
parameters which allows you to dynamically determine values.

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/redis/index.html>`__
* `Product Documentation <https://cloud.google.com/memorystore/docs/redis/>`__
