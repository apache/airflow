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

``apache-airflow-providers-apache-polaris``
===========================================

`Apache Polaris <https://polaris.apache.org/>`__ is an Apache Iceberg REST catalog. On top of the
Iceberg REST protocol it adds a **Management API** (catalogs, principals, principal and catalog
roles, grants), **Polaris-only catalog APIs** (policies, notifications) and its own
**authentication** (realms, principal-role scopes, external OIDC identity providers).

What this provider is for
-------------------------

This provider manages everything a Polaris catalog defines, including what Polaris adds on top of
the Iceberg REST protocol, which the Iceberg provider does not reach:

* the ``polaris`` connection type (:doc:`connections`): one connection for internal Polaris
  credentials, an external identity provider, or a ready-made token, with realm and TLS
  settings;
* hooks built on the official ``apache-polaris`` Python SDK:
  :class:`~airflow.providers.apache.polaris.hooks.polaris.PolarisManagementHook` for the
  Management API and :class:`~airflow.providers.apache.polaris.hooks.polaris.PolarisCatalogHook`
  for namespaces, tables, policies, notifications and a correctly configured pyiceberg catalog;
* idempotent operators for catalogs, namespaces, tables, principals, roles, grants and policies
  (:doc:`operators/polaris`),
  including :class:`~airflow.providers.apache.polaris.operators.policy.PolarisGetApplicablePoliciesOperator`,
  which turns Polaris's table-maintenance policies into one mapped task per table;
* :func:`~airflow.providers.apache.polaris.utils.maintenance.to_spark_sql`, which turns each
  of those policies into the Iceberg procedure call a Spark task runs
  (:ref:`howto/polaris:maintenance`).

What it deliberately does not do: read or write table data (use pyiceberg, the Iceberg provider
or an engine provider), or handle principal client secrets (operators never return, log or
store them).

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Basics

    Home <self>
    Changelog <changelog>
    Security <security>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

    Connection types <connections>
    Operators <operators/polaris>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/apache/polaris/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/apache/polaris/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example Dags <https://github.com/apache/airflow/tree/providers-apache-polaris/|version|/providers/apache/polaris/tests/system/apache/polaris>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-apache-polaris/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-apache-polaris package
------------------------------------------------------

`Apache Polaris <https://polaris.apache.org/>`__ provider for Apache Airflow.
Owns the ``polaris`` connection type, hooks for the Polaris Management API and catalog
APIs, idempotent operators for catalogs, namespaces, tables, principals, roles,
grants and policies, and a helper that turns table-maintenance policies into Iceberg
Spark procedure calls.


Release: 0.1.0

Provider package
----------------

This package is for the ``apache.polaris`` provider.
All classes for this package are included in the ``airflow.providers.apache.polaris`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-apache-polaris``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.1.1``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.1.1``
``apache-airflow-providers-common-compat``  ``>=1.22.0``
``apache-polaris``                          ``>=1.9.0``
==========================================  ==================

Optional dependencies
---------------------

These extras install optional third-party libraries that enable additional features of the provider.
Install them when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-apache-polaris[iceberg]


===========  =====================
Extra        Dependencies
===========  =====================
``iceberg``  ``pyiceberg>=0.12.0``
===========  =====================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-apache-polaris 0.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_polaris-0.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_polaris-0.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_polaris-0.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-apache-polaris 0.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_polaris-0.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_polaris-0.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_polaris-0.1.0-py3-none-any.whl.sha512>`__)
