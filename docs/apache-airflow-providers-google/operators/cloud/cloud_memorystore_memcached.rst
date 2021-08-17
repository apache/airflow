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



Google Cloud Memorystore Memcached Operators
============================================

The `Cloud Memorystore for Memcached <https://cloud.google.com/memorystore/docs/memcached/>`__ is a fully managed
Memcached service for Google Cloud. Applications running on Google Cloud can achieve extreme performance by
leveraging the highly scalable, available, secure Memcached service without the burden of managing complex
Memcached deployments.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst


.. _howto/operator:CloudMemorystoreMemcachedInstance:

Instance
^^^^^^^^

Operators uses a :class:`~google.cloud.memcache_v1beta2.types.cloud_memcache.Instance` for representing instance.
The object can be presented as a compatible dictionary also.

Here is an example of instance

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :start-after: [START howto_operator_memcached_instance]
    :end-before: [END howto_operator_memcached_instance]


.. _howto/operator:CloudMemorystoreMemcachedCreateInstanceOperator:

Create instance
^^^^^^^^^^^^^^^

Create a instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreMemcachedCreateInstanceOperator`
operator.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_instance_memcached]
    :end-before: [END howto_operator_create_instance_memcached]


.. _howto/operator:CloudMemorystoreMemcachedDeleteInstanceOperator:

Delete instance
^^^^^^^^^^^^^^^

Delete an instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreMemcachedDeleteInstanceOperator`
operator.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_instance_memcached]
    :end-before: [END howto_operator_delete_instance_memcached]


.. _howto/operator:CloudMemorystoreMemcachedGetInstanceOperator:

Get instance
^^^^^^^^^^^^

Get an instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreMemcachedGetInstanceOperator`
operator.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_get_instance_memcached]
    :end-before: [END howto_operator_get_instance_memcached]


.. _howto/operator:CloudMemorystoreMemcachedListInstancesOperator:

List instances
^^^^^^^^^^^^^^

List instances is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreMemcachedListInstancesOperator`
operator.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_list_instances_memcached]
    :end-before: [END howto_operator_list_instances_memcached]


.. _howto/operator:CloudMemorystoreMemcachedUpdateInstanceOperator:

Update instance
^^^^^^^^^^^^^^^

Updating an instance is performed with the
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreMemcachedUpdateInstanceOperator`
operator.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_update_instance_memcached]
    :end-before: [END howto_operator_update_instance_memcached]


.. _howto/operator:CloudMemorystoreMemcachedApplyParametersOperator:

Update and apply parameters to an instance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To update and apply Memcached parameters to an instance use
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreMemcachedUpdateParametersOperator`
and
:class:`~airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreMemcachedApplyParametersOperator`
operator.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_memorystore.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_update_and_apply_parameters_memcached]
    :end-before: [END howto_operator_update_and_apply_parameters_memcached]


Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/memcache/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/memorystore/docs/memcached/>`__
