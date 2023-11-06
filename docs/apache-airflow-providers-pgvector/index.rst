
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
