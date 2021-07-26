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

``apache-airflow-providers-apache-hive``
========================================


Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/index>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/apache/hive/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/main/airflow/providers/apache/hive/example_dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-apache-hive/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-apache-hive
------------------------------------------------------

`Apache Hive <https://hive.apache.org/>`__


Release: 2.0.1

Provider package
----------------

This is a provider package for ``apache.hive`` provider. All classes for this provider package
are in ``airflow.providers.apache.hive`` python package.

Installation
------------

You can install this package on top of an existing airflow 2.1+ installation via
``pip install apache-airflow-providers-apache-hive``

PIP requirements
----------------

==================  ================================
PIP package         Version required
==================  ================================
``apache-airflow``  ``>=2.1.0``
``hmsclient``       ``>=0.1.0``
``pyhive[hive]``    ``>=0.6.0;python_version<"3.9"``
``thrift``          ``>=0.9.2``
==================  ================================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-apache-hive[amazon]


======================================================================================================================  ===================
Dependent package                                                                                                       Extra
======================================================================================================================  ===================
`apache-airflow-providers-amazon <https://airflow.apache.org/docs/apache-airflow-providers-amazon>`_                    ``amazon``
`apache-airflow-providers-microsoft-mssql <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql>`_  ``microsoft.mssql``
`apache-airflow-providers-mysql <https://airflow.apache.org/docs/apache-airflow-providers-mysql>`_                      ``mysql``
`apache-airflow-providers-presto <https://airflow.apache.org/docs/apache-airflow-providers-presto>`_                    ``presto``
`apache-airflow-providers-samba <https://airflow.apache.org/docs/apache-airflow-providers-samba>`_                      ``samba``
`apache-airflow-providers-vertica <https://airflow.apache.org/docs/apache-airflow-providers-vertica>`_                  ``vertica``
======================================================================================================================  ===================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-apache-hive 2.0.1 sdist package <https://downloads.apache.org/airflow/providers/apache-airflow-providers-apache-hive-2.0.1.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache-airflow-providers-apache-hive-2.0.1.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache-airflow-providers-apache-hive-2.0.1.tar.gz.sha512>`__)
* `The apache-airflow-providers-apache-hive 2.0.1 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hive-2.0.1-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hive-2.0.1-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hive-2.0.1-py3-none-any.whl.sha512>`__)

.. include:: ../../airflow/providers/apache/hive/CHANGELOG.rst
