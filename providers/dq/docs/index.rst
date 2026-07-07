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

``apache-airflow-providers-dq``
===============================


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

    Rules, rule sets, and checks <rules>
    Operators <operators>
    Decorators <decorators>
    Assets and quality gating <assets>
    Generating rules with an LLM <agents>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Configuration <configurations-ref>
    Python API <_api/airflow/providers/dq/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-dq/>
    Installing from sources <installing-providers-from-sources>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/dq/index>


apache-airflow-providers-dq package
-----------------------------------

``Data Quality Provider``

Declarative data quality rules with durable, per-rule execution history. Checks run through
``common.sql`` DB-API hooks; results are persisted to a configurable results store (object
storage or local files) so task, run, and rule-level quality can be inspected over time.

See :doc:`rules` for the built-in check catalog (and its cross-database caveats),
:doc:`operators`/:doc:`decorators` for running checks, and :doc:`assets` for attaching rules to
an asset and gating a downstream Dag on its quality score.

Release: 0.1.0

Provider package
----------------

This package is for the ``dq`` provider.
All classes for this package are included in the ``airflow.providers.dq`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-dq``. For the minimum Airflow version supported,
see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0.0``
``apache-airflow-providers-common-compat``  ``>=1.15.0``
``apache-airflow-providers-common-sql``     ``>=2.0.0``
``pydantic``                                ``>=2.11.0``
``pyyaml``                                  ``>=6.0.2``
==========================================  ==================

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-dq package
------------------------------------------------------

``Data Quality Provider``

Declarative data quality rules with durable, per-rule execution history.
Checks run through ``common.sql`` DB-API hooks; results are persisted to a
configurable results store (object storage or local files) so task, run,
and rule-level quality can be inspected over time.


Release: 0.1.0

Provider package
----------------

This package is for the ``dq`` provider.
All classes for this package are included in the ``airflow.providers.dq`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-dq``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0.0``
``apache-airflow-providers-common-compat``  ``>=1.15.0``
``apache-airflow-providers-common-sql``     ``>=2.0.0``
``pydantic``                                ``>=2.11.0``
``pyyaml``                                  ``>=6.0.2``
==========================================  ==================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-dq 0.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_dq-0.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_dq-0.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_dq-0.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-dq 0.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_dq-0.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_dq-0.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_dq-0.1.0-py3-none-any.whl.sha512>`__)
