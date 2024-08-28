
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

``apache-airflow-providers-tabular``
====================================


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

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/providers-tabular/|version|/tests/system/providers/tabular>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-tabular/>
    Installing from sources <installing-providers-from-sources>
    Python API <_api/airflow/providers/tabular/index>


.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-tabular package
------------------------------------------------------

`Tabular <https://tabular.io/>`__


Release: 1.6.1

Provider package
----------------

This package is for the ``tabular`` provider.
All classes for this package are included in the ``airflow.providers.tabular`` python package.

    .. warning::

        This provider is not maintained anymore by the community. It has been removed and is not going to be
        updated anymore. The removal was done according to the process described in
        `Removing community providers <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#removing-community-providers>`_

        Feel free to contact Airflow Development Mailing List if you have any questions.

Installation
------------

You can install this package on top of an existing Airflow 2 installation via
``pip install apache-airflow-providers-tabular``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.8.0``.

===========================================  ==================
PIP package                                  Version required
===========================================  ==================
``apache-airflow``                           ``>=2.8.0``
``apache-airflow-providers-apache-iceberg``
===========================================  ==================
