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

``apache-airflow-providers-common-sandbox``
===========================================

The Common Sandbox provider contains the reusable executor state machine and
driver contract for vendor-owned sandbox executors. It does not register a
standalone executor or select providers dynamically.

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

    Architecture <architecture>
    Implement a driver <driver>
    Configuration <configurations-ref>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Python API <_api/airflow/providers/common/sandbox/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-common-sandbox/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-common-sandbox package
------------------------------------------------------

Shared executor engine and driver contract for provider-owned ephemeral sandbox executors.


Release: 0.1.0

Provider package
----------------

This package is for the ``common.sandbox`` provider.
All classes for this package are included in the ``airflow.providers.common.sandbox`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-common-sandbox``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.3.0``.

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=3.3.0``
==================  ==================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-common-sandbox 0.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_sandbox-0.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_sandbox-0.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_sandbox-0.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-common-sandbox 0.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_sandbox-0.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_sandbox-0.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_sandbox-0.1.0-py3-none-any.whl.sha512>`__)
