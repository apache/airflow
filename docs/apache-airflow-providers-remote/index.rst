
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

``apache-airflow-providers-remote``
===================================


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
    :caption: Executors

    RemoteExecutor details <remote_executor>


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Configuration <configurations-ref>
    CLI <cli-ref>
    Python API <_api/airflow/providers/remote/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-remote/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-remote package
------------------------------------------------------

Handle remote workers via HTTP(s) connection and distribute work over distributed sites


Release: 0.1.0pre0

Provider package
----------------

This package is for the ``remote`` provider.
All classes for this package are included in the ``airflow.providers.remote`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation via
``pip install apache-airflow-providers-remote``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.10.0``.

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=2.10.0``
``pydantic``
==================  ==================
