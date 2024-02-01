
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

``apache-airflow-providers-pgvector``
======================================


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
    Operators <operators/pgvector>


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Python API <_api/airflow/providers/pgvector/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-pgvector/>
    Installing from sources <installing-providers-from-sources>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/providers/pgvector/index>

Package apache-airflow-providers-pgvector
-----------------------------------------

`pgvector <https://github.com/pgvector/pgvector>`__


Release: 1.0.0

Provider package
----------------

This is a provider package for ``pgvector``. All classes for this provider package
are in ``airflow.providers.pgvector`` python module.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-pgvector``


Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.5.0``.


=====================================  ==================
PIP package                            Version required
=====================================  ==================
``apache-airflow``                     ``>=2.5.0``
``apache-airflow-providers-postgres``  ``>=5.7.1``
``pgvector``                           ``>=0.2.3``
=====================================  ==================

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-pgvector
------------------------------------------------------

`pgvector <https://github.com/pgvector/pgvector>`__


Release: 1.1.0

Provider package
----------------

This is a provider package for ``pgvector`` provider. All classes for this provider package
are in ``airflow.providers.pgvector`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-pgvector``

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.6.0``.

=====================================  ==================
PIP package                            Version required
=====================================  ==================
``apache-airflow``                     ``>=2.6.0``
``apache-airflow-providers-postgres``  ``>=5.7.1``
``pgvector``                           ``>=0.2.3``
=====================================  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-pgvector[common.sql]


============================================================================================================  ==============
Dependent package                                                                                             Extra
============================================================================================================  ==============
`apache-airflow-providers-common-sql <https://airflow.apache.org/docs/apache-airflow-providers-common-sql>`_  ``common.sql``
`apache-airflow-providers-postgres <https://airflow.apache.org/docs/apache-airflow-providers-postgres>`_      ``postgres``
============================================================================================================  ==============

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-pgvector 1.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache-airflow-providers-pgvector-1.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache-airflow-providers-pgvector-1.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache-airflow-providers-pgvector-1.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-pgvector 1.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_pgvector-1.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_pgvector-1.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_pgvector-1.1.0-py3-none-any.whl.sha512>`__)
