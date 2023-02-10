
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

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/index>
    Operators <operators/index>
    Secrets backends <secrets-backends/azure-key-vault>
    Logging for Tasks <logging/index>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/microsoft/azure/index>

.. toctree::
    :hidden:
    :caption: System tests

    System Tests <_api/tests/system/providers/microsoft/azure/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/providers-microsoft-azure/|version|/tests/system/providers/microsoft/azure>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-microsoft-azure/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-microsoft-azure
------------------------------------------------------

`Microsoft Azure <https://azure.microsoft.com/>`__


Release: 5.2.0

Provider package
----------------

This is a provider package for ``microsoft.azure`` provider. All classes for this provider package
are in ``airflow.providers.microsoft.azure`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-microsoft-azure``

Requirements
------------

================================  ==================
PIP package                       Version required
================================  ==================
``apache-airflow``                ``>=2.3.0``
``azure-batch``                   ``>=8.0.0``
``azure-cosmos``                  ``>=4.0.0``
``azure-datalake-store``          ``>=0.0.45``
``azure-identity``                ``>=1.3.1``
``azure-keyvault-secrets``        ``>=4.1.0,<5.0``
``azure-kusto-data``              ``>=0.0.43,<0.1``
``azure-mgmt-containerinstance``  ``>=1.5.0,<2.0``
``azure-mgmt-datafactory``        ``>=1.0.0,<2.0``
``azure-mgmt-datalake-store``     ``>=0.5.0``
``azure-mgmt-resource``           ``>=2.2.0``
``azure-storage-blob``            ``>=12.14.0``
``azure-storage-common``          ``>=2.1.0``
``azure-storage-file``            ``>=2.1.0``
``azure-servicebus``              ``>=7.6.1``
``azure-synapse-spark``
``adal``                          ``>=1.2.7``
``azure-storage-file-datalake``   ``>=12.9.1``
================================  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-microsoft-azure[google]


====================================================================================================  ==========
Dependent package                                                                                     Extra
====================================================================================================  ==========
`apache-airflow-providers-google <https://airflow.apache.org/docs/apache-airflow-providers-google>`_  ``google``
`apache-airflow-providers-oracle <https://airflow.apache.org/docs/apache-airflow-providers-oracle>`_  ``oracle``
`apache-airflow-providers-sftp <https://airflow.apache.org/docs/apache-airflow-providers-sftp>`_      ``sftp``
====================================================================================================  ==========

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-microsoft-azure 5.2.0 sdist package <https://downloads.apache.org/airflow/providers/apache-airflow-providers-microsoft-azure-5.2.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache-airflow-providers-microsoft-azure-5.2.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache-airflow-providers-microsoft-azure-5.2.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-microsoft-azure 5.2.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-5.2.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-5.2.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-5.2.0-py3-none-any.whl.sha512>`__)

.. include:: ../../airflow/providers/microsoft/azure/CHANGELOG.rst
