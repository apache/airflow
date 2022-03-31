
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

``apache-airflow-providers-databricks``
============================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/index>
    Operators <operators/index>
    Sensors <sensors/hive_partition>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/cloudera/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-cloudera/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-cloudera
------------------------------------------------------

`Cloudera <https://cloudera.com/>`__


Release: 1.0.0

Provider package
----------------

This is a provider package for ``cloudera`` provider. All classes for this provider package
are in ``airflow.providers.cloudera`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2.1+ installation via
``pip install apache-airflow-providers-cloudera``

PIP requirements
----------------

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=2.0.0``
``cryptography``    ``>=3.3.2``
``pathlib``
``pure25519``
``requests``
``tenacity``
==================  ==================

.. include:: ../../airflow/providers/cloudera/CHANGELOG.rst
