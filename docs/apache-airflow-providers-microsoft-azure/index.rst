
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

``apache-airflow-providers-microsoft-azure``
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

    Connection types <connections/index>
    Operators <operators/index>
    Transfers <transfer/index>
    Secrets backends <secrets-backends/azure-key-vault>
    Logging for Tasks <logging/index>
    Sensors <sensors/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Configuration <configurations-ref>
    Python API <_api/airflow/providers/microsoft/azure/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/providers/microsoft/azure/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/providers-microsoft-azure/|version|/tests/system/providers/microsoft/azure>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-microsoft-azure/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-microsoft-azure package
------------------------------------------------------

`Microsoft Azure <https://azure.microsoft.com/>`__


Release: 10.5.1

Provider package
----------------

This package is for the ``microsoft.azure`` provider.
All classes for this package are included in the ``airflow.providers.microsoft.azure`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation via
``pip install apache-airflow-providers-microsoft-azure``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.8.0``.

================================  ===================
PIP package                       Version required
================================  ===================
``apache-airflow``                ``>=2.8.0``
``adlfs``                         ``>=2023.10.0``
``azure-batch``                   ``>=8.0.0``
``azure-cosmos``                  ``>=4.6.0``
``azure-mgmt-cosmosdb``           ``>=3.0.0``
``azure-datalake-store``          ``>=0.0.45``
``azure-identity``                ``>=1.3.1``
``azure-keyvault-secrets``        ``>=4.1.0``
``azure-mgmt-datalake-store``     ``>=0.5.0``
``azure-mgmt-resource``           ``>=2.2.0``
``azure-storage-blob``            ``>=12.14.0``
``azure-mgmt-storage``            ``>=16.0.0``
``azure-storage-file-share``      ``>=12.7.0``
``azure-servicebus``              ``>=7.12.1``
``azure-synapse-spark``           ``>=0.2.0``
``azure-synapse-artifacts``       ``>=0.17.0``
``adal``                          ``>=1.2.7``
``azure-storage-file-datalake``   ``>=12.9.1``
``azure-kusto-data``              ``>=4.1.0,!=4.6.0``
``azure-mgmt-datafactory``        ``>=2.0.0``
``azure-mgmt-containerregistry``  ``>=8.0.0``
``azure-mgmt-containerinstance``  ``>=10.1.0``
``msgraph-core``                  ``>=1.0.0``
================================  ===================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-microsoft-azure[amazon]


====================================================================================================  ==========
Dependent package                                                                                     Extra
====================================================================================================  ==========
`apache-airflow-providers-amazon <https://airflow.apache.org/docs/apache-airflow-providers-amazon>`_  ``amazon``
`apache-airflow-providers-google <https://airflow.apache.org/docs/apache-airflow-providers-google>`_  ``google``
`apache-airflow-providers-oracle <https://airflow.apache.org/docs/apache-airflow-providers-oracle>`_  ``oracle``
`apache-airflow-providers-sftp <https://airflow.apache.org/docs/apache-airflow-providers-sftp>`_      ``sftp``
====================================================================================================  ==========

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-microsoft-azure 10.5.1 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-10.5.1.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-10.5.1.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-10.5.1.tar.gz.sha512>`__)
* `The apache-airflow-providers-microsoft-azure 10.5.1 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-10.5.1-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-10.5.1-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-10.5.1-py3-none-any.whl.sha512>`__)
