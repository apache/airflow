
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

``apache-airflow-providers-databricks``
============================================


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

    Connection types <connections/databricks>
    Operators <operators/index>
    Plugins <plugins/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/databricks/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/databricks/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example Dags <https://github.com/apache/airflow/tree/providers-databricks/|version|/providers/databricks/tests/system/databricks>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-databricks/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-databricks package
------------------------------------------------------

`Databricks <https://databricks.com/>`__


Release: 7.8.2

Provider package
----------------

This package is for the ``databricks`` provider.
All classes for this package are included in the ``airflow.providers.databricks`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-databricks``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==========================================  ======================================
PIP package                                 Version required
==========================================  ======================================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.10.1``
``apache-airflow-providers-common-sql``     ``>=1.27.0``
``requests``                                ``>=2.32.0,<3``
``databricks-sql-connector``                ``>=4.0.0``
``aiohttp``                                 ``>=3.9.2,<4``
``mergedeep``                               ``>=1.3.4``
``pandas``                                  ``>=2.1.2; python_version < "3.13"``
``pandas``                                  ``>=2.2.3; python_version >= "3.13"``
``pyarrow``                                 ``>=16.1.0; python_version < "3.13"``
``pyarrow``                                 ``>=18.0.0; python_version >= "3.13"``
==========================================  ======================================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider distributions in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-databricks[common.compat]


==================================================================================================================  =================
Dependent package                                                                                                   Extra
==================================================================================================================  =================
`apache-airflow-providers-common-compat <https://airflow.apache.org/docs/apache-airflow-providers-common-compat>`_  ``common.compat``
`apache-airflow-providers-common-sql <https://airflow.apache.org/docs/apache-airflow-providers-common-sql>`_        ``common.sql``
`apache-airflow-providers-openlineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage>`_      ``openlineage``
==================================================================================================================  =================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-databricks 7.8.2 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_databricks-7.8.2.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_databricks-7.8.2.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_databricks-7.8.2.tar.gz.sha512>`__)
* `The apache-airflow-providers-databricks 7.8.2 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_databricks-7.8.2-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_databricks-7.8.2-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_databricks-7.8.2-py3-none-any.whl.sha512>`__)
