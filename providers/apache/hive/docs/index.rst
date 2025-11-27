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

``apache-airflow-providers-apache-hive``
========================================



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

    Connection types <connections/index>
    Operators <operators>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Configuration <configurations-ref>
    Python API <_api/airflow/providers/apache/hive/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/apache/hive/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example Dags <https://github.com/apache/airflow/tree/providers-apache-hive/|version|/providers/apache/hive/tests/system/apache/hive>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-apache-hive/>
    Installing from sources <installing-providers-from-sources>
    Macros <macros>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-apache-hive package
------------------------------------------------------

`Apache Hive <https://hive.apache.org/>`__


Release: 9.2.0

Provider package
----------------

This package is for the ``apache.hive`` provider.
All classes for this package are included in the ``airflow.providers.apache.hive`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-apache-hive``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==========================================  =====================================
PIP package                                 Version required
==========================================  =====================================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.8.0``
``apache-airflow-providers-common-sql``     ``>=1.26.0``
``hmsclient``                               ``>=0.1.0``
``pandas``                                  ``>=2.1.2; python_version < "3.13"``
``pandas``                                  ``>=2.2.3; python_version >= "3.13"``
``pyhive[hive_pure_sasl]``                  ``>=0.7.0``
``jmespath``                                ``>=0.7.0``
==========================================  =====================================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider distributions in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-apache-hive[amazon]


======================================================================================================================  ===================
Dependent package                                                                                                       Extra
======================================================================================================================  ===================
`apache-airflow-providers-amazon <https://airflow.apache.org/docs/apache-airflow-providers-amazon>`_                    ``amazon``
`apache-airflow-providers-common-compat <https://airflow.apache.org/docs/apache-airflow-providers-common-compat>`_      ``common.compat``
`apache-airflow-providers-common-sql <https://airflow.apache.org/docs/apache-airflow-providers-common-sql>`_            ``common.sql``
`apache-airflow-providers-microsoft-mssql <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql>`_  ``microsoft.mssql``
`apache-airflow-providers-mysql <https://airflow.apache.org/docs/apache-airflow-providers-mysql>`_                      ``mysql``
`apache-airflow-providers-presto <https://airflow.apache.org/docs/apache-airflow-providers-presto>`_                    ``presto``
`apache-airflow-providers-samba <https://airflow.apache.org/docs/apache-airflow-providers-samba>`_                      ``samba``
`apache-airflow-providers-vertica <https://airflow.apache.org/docs/apache-airflow-providers-vertica>`_                  ``vertica``
======================================================================================================================  ===================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-apache-hive 9.2.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hive-9.2.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hive-9.2.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hive-9.2.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-apache-hive 9.2.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hive-9.2.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hive-9.2.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hive-9.2.0-py3-none-any.whl.sha512>`__)
