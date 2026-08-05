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

``apache-airflow-providers-ray``
========================================

Use this provider to create and remove KubeRay clusters, submit Ray jobs, monitor jobs asynchronously,
and define Ray tasks with the ``@ray.task`` decorator.

.. toctree::
   :hidden:
   :maxdepth: 1

   Home <self>
   Getting started <getting-started>
   Connection types <connections>
   Operators and decorators <operators>
   Examples <examples>
   Cloud-platform authentication <cloud-platforms>
   Changelog <changelog>
   Security <security>
   Installing from sources <installing-providers-from-sources>

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Resources

   Python API <_api/airflow/providers/ray/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/ray/index>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-ray package
------------------------------------------------------

`Ray <https://docs.ray.io/>`__ provider for Apache Airflow.
Manage Kubernetes-hosted Ray clusters and submit distributed Ray jobs from Airflow Dags.


Release: 0.0.1

Provider package
----------------

This package is for the ``ray`` provider.
All classes for this package are included in the ``airflow.providers.ray`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-ray``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

============================================  ==================
PIP package                                   Version required
============================================  ==================
``apache-airflow``                            ``>=3.0.0``
``apache-airflow-providers-cncf-kubernetes``
``apache-airflow-providers-standard``
``kubernetes``
``pyyaml``
``ray[default]``
``requests``
============================================  ==================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-ray 0.0.1 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ray-0.0.1.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ray-0.0.1.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ray-0.0.1.tar.gz.sha512>`__)
* `The apache-airflow-providers-ray 0.0.1 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ray-0.0.1-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ray-0.0.1-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ray-0.0.1-py3-none-any.whl.sha512>`__)
