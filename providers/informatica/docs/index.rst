
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

========================================
``apache-airflow-providers-informatica``
========================================

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Basics

   Home <self>
   Security <security>
   Changelog <changelog>

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Guides

   Usage <guides/usage>
   API Reference <guides/api>
   Configuration <guides/configuration>

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: References

   Configuration <configurations-ref>
   Python API <_api/airflow/providers/informatica/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-informatica/>
    Installing from sources <installing-providers-from-sources>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>

Apache Airflow Informatica Provider
===================================


The Informatica provider integrates Apache Airflow with Informatica Enterprise Data Catalog (EDC) for advanced data lineage tracking and asset discovery.

Overview
--------

This provider enables automatic lineage extraction and tracking between Airflow tasks and Informatica EDC catalog objects. When tasks define inlets and outlets with EDC object URIs, the provider automatically:

- Resolves object identifiers using EDC API
- Creates lineage relationships between source and target objects
- Integrates with Airflow's native lineage system

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-informatica``.
For the minimum Airflow version supported, see ``Requirements`` below.


Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0.0``
``apache-airflow-providers-common-compat``  ``>=1.12.0``
``apache-airflow-providers-http``           ``>=4.13.2``
==========================================  ==================

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-informatica package
------------------------------------------------------

`Informatica <https://www.informatica.com//>`__


Release: 0.1.1

Provider package
----------------

This package is for the ``informatica`` provider.
All classes for this package are included in the ``airflow.providers.informatica`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-informatica``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0.0``
``apache-airflow-providers-common-compat``  ``>=1.12.0``
``apache-airflow-providers-http``           ``>=4.13.2``
==========================================  ==================
