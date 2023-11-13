
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

``apache-airflow-providers-qubole``
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
    :caption: Guides

    Operators <operators/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/qubole/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/providers/qubole/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/providers-qubole/|version|/tests/system/providers/qubole>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-qubole/>
    Installing from sources <installing-providers-from-sources>


.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!
.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-qubole
------------------------------------------------------

`Qubole <https://www.qubole.com/>`__


Release: 3.4.3

Provider package
----------------

This is a provider package for ``qubole`` provider. All classes for this provider package
are in ``airflow.providers.qubole`` python package.

    .. warning::

        This provider is not maintained anymore by the community. It has been removed and is not going to be
        updated anymore. The removal was done according to the process described in
        `Removing community providers <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#removing-community-providers>`_

        Feel free to contact Airflow Development Mailing List if you have any questions.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-qubole``

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.5.0``.

=======================================  ==================
PIP package                              Version required
=======================================  ==================
``apache-airflow``                       ``>=2.5.0``
``apache-airflow-providers-common-sql``  ``>=1.3.1``
``qds-sdk``                              ``>=1.10.4``
=======================================  ==================
