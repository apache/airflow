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

Getting started
===============

Install the provider
--------------------

Install the community provider into an existing Airflow 3 environment:

.. code-block:: bash

   pip install apache-airflow-providers-ray

Install `Helm <https://helm.sh/docs/intro/install/>`__ on workers that will create KubeRay clusters.
Helm is not required when submitting jobs to an existing Ray cluster.

Configure a connection
----------------------

Create a connection with connection type ``Ray``.

For a provider-managed KubeRay cluster, use in-cluster authentication or configure either
``Kubeconfig path`` or ``Kubeconfig (JSON format)``, along with the Kubernetes namespace and
optional cluster context. The two kubeconfig options are mutually exclusive.

The provider installs or upgrades the namespace-wide KubeRay operator when creating a cluster.
Cluster deletion leaves this shared controller installed so concurrent Ray workloads are not disrupted.

For an existing Ray cluster, configure ``Ray dashboard URL``. You can additionally provide
cookies, metadata, headers, and TLS verification settings. The ``RAY_ADDRESS`` environment
variable is used when the connection does not contain a dashboard URL.
