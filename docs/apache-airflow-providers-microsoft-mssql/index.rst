
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

``apache-airflow-providers-microsoft-mssql``
============================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/mssql>
    Operators <operators>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/microsoft/mssql/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/main/airflow/providers/microsoft/mssql/example_dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-microsoft-mssql/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-microsoft-mssql
------------------------------------------------------

`Microsoft SQL Server (MSSQL) <https://www.microsoft.com/en-us/sql-server/sql-server-downloads>`__


Release: 2.1.2

Provider package
----------------

This is a provider package for ``microsoft.mssql`` provider. All classes for this provider package
are in ``airflow.providers.microsoft.mssql`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2.1+ installation via
``pip install apache-airflow-providers-microsoft-mssql``

PIP requirements
----------------

==================  ==========================================
PIP package         Version required
==================  ==========================================
``apache-airflow``  ``>=2.1.0``
``pymssql``         ``>=2.1.5; platform_machine != "aarch64"``
==================  ==========================================

.. include:: ../../airflow/providers/microsoft/mssql/CHANGELOG.rst
