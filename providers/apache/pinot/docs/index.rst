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

``apache-airflow-providers-apache-pinot``
=========================================


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

    Operators <operators>
    Apache Pinot Hooks <hooks>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Example Dags <https://github.com/apache/airflow/tree/providers-apache-pinot/|version|/providers/apache/pinot/tests/system/apache/pinot>
    Python API <_api/airflow/providers/apache/pinot/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-apache-pinot/>
    Installing from sources <installing-providers-from-sources>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/apache/pinot/index>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-apache-pinot package
------------------------------------------------------

`Apache Pinot <https://pinot.apache.org/>`__


Release: 4.10.0

Provider package
----------------

This package is for the ``apache.pinot`` provider.
All classes for this package are included in the ``airflow.providers.apache.pinot`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-apache-pinot``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.10.1``
``apache-airflow-providers-common-sql``     ``>=1.32.0``
``pinotdb``                                 ``>=5.1.0``
==========================================  ==================
