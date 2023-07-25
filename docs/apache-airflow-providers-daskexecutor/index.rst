
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

``apache-airflow-providers-daskexecutor``
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
    :caption: References

    Configuration <configurations-ref>
    Python API <_api/airflow/providers/daskexecutor/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-daskexecutor/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-daskexecutor
------------------------------------------------------

`Dask <https://www.dask.org/>`__


Release: 1.0.0

Provider package
----------------

This is a provider package for ``daskexecutor`` provider. All classes for this provider package
are in ``airflow.providers.daskexecutor`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-daskexecutor``

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.4.0``.

==================  ==================================
PIP package         Version required
==================  ==================================
``apache-airflow``  ``>=2.4.0``
``cloudpickle``     ``>=1.4.1``
``dask``            ``>=2.9.0,!=2022.10.1,!=2023.5.0``
``distributed``     ``>=2.11.1,!=2023.5.0``
==================  ==================================
