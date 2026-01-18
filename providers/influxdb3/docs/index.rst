
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

``apache-airflow-providers-influxdb3``
=======================================

This provider package integrates `InfluxDB 3.x <https://www.influxdata.com/>`__
(Core/Enterprise/Cloud Dedicated) with Apache Airflow.

**Note:** This provider is specifically for InfluxDB 3.x. For InfluxDB 2.x support,
use the ``apache-airflow-providers-influxdb`` provider.

InfluxDB 3.x uses SQL queries and a different API compared to InfluxDB 2.x.

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

    Connection types <connections/influxdb3>
    Operators <operators/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/influxdb3/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    Example DAGs <https://github.com/apache/airflow/tree/providers-influxdb3/|version|/providers/influxdb3/tests/system/influxdb3/example_influxdb3.py>

Installation
------------

You can install this package on top of an existing Airflow installation via

.. code-block:: bash

    pip install apache-airflow-providers-influxdb3

Requirements
------------

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.8.0``
``influxdb3-python``                       ``>=0.7.0``
``requests``                                ``>=2.32.0,<3``
==========================================  ==================
