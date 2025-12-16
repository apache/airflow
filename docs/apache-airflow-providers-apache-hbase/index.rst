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

``apache-airflow-providers-apache-hbase``
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

    Connection types <connections/hbase>
    Operators <operators>
    Sensors <sensors>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/apache/hbase/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/providers/apache/hbase/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/main/airflow/providers/hbase/example_dags>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-apache-hbase package
----------------------------------------------

`Apache HBase <https://hbase.apache.org/>`__.


Release: 1.0.0

Provider package
----------------

This package is for the ``hbase`` provider.
All classes for this package are included in the ``airflow.providers.hbase`` python package.

Installation
------------

This provider is included as part of Apache Airflow starting from version 2.7.0.
No separate installation is required - the HBase provider is available when you install Airflow.

To use HBase functionality, you need to install the ``happybase`` dependency:

.. code-block:: bash

    pip install 'apache-airflow[hbase]'

Or install the dependency directly:

.. code-block:: bash

    pip install happybase>=1.2.0

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.7.0``.

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=2.7.0``
``happybase``       ``>=1.2.0``
==================  ==================