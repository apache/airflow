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

``apache-airflow-providers-ibm-db2``
=====================================


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

    Connection types <connections/db2>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/ibm/db2/index>
    Dialects <dialects>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/ibm/db2/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-ibm-db2/>
    Example Dags <https://github.com/apache/airflow/tree/providers-ibm-db2/|version|/providers/ibm/db2/tests/system/ibm/db2>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-ibm-db2 package
------------------------------------------------------

`IBM Db2 <https://www.ibm.com/products/db2>`__


Release: 0.1.0

Provider package
----------------

This package is for the ``ibm.db2`` provider.
All classes for this package are included in the ``airflow.providers.ibm.db2`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-ibm-db2``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==========================================  ======================================
PIP package                                 Version required
==========================================  ======================================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.12.0``
``apache-airflow-providers-common-sql``     ``>=1.32.0``
``ibm-db``                                  ``>=3.0.0``
``ibm-db-sa``                               ``>=0.4.0``
``methodtools``                             ``>=0.4.7``
==========================================  ======================================

Optional dependencies
---------------------

These extras install optional third-party libraries that enable additional features of the provider.
Install them when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-ibm-db2[openlineage]


===============  ========================================
Extra            Dependencies
===============  ========================================
``openlineage``  ``apache-airflow-providers-openlineage``
===============  ========================================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-ibm-db2 0.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ibm_db2-0.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ibm_db2-0.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ibm_db2-0.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-ibm-db2 0.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ibm_db2-0.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ibm_db2-0.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_ibm_db2-0.1.0-py3-none-any.whl.sha512>`__)
