
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

``apache-airflow-providers-common-opendal``
=====================================================

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

    Connection Types <connections/opendal>
    Operators <operators/opendal>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/common/opendal/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/common/opendal/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/providers-common-opendal/|version|/providers/common/opendal/tests/system/opendal>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-common-opendal/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-common-opendal package
------------------------------------------------------

`Apache OpenDAL <https://opendal.apache.org/>`_ is a Open Data Access Layer. it provides a abstraction on various
object storage systems, and key value stores to access data and manipulate data.


Release: 1.0.0

Provider package
----------------

This package is for the ``common.opendal`` provider.
All classes for this package are included in the ``airflow.providers.common.opendal`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation via
``pip install apache-airflow-providers-common-opendal``
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.10.0``.

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=2.10.0``
``opendal``   ``>=0.45.18``
==================  ==================
