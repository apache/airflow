
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

``apache-airflow-providers-fab``
================================


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

    FAB auth manager <auth-manager/index>
    Upgrading <upgrading>

.. toctree::
    :hidden:
    :caption: Internal DB details

    Database Migrations <migrations-ref>

.. toctree::
    :hidden:
    :caption: References

    Fab auth manager API <api-ref/fab-public-api-ref>
    Fab auth manager token API <api-ref/fab-token-api-ref>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Configuration <configurations-ref>
    CLI <cli-ref>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Python API <_api/airflow/providers/fab/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-fab/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-fab package
------------------------------------------------------

`Flask App Builder <https://flask-appbuilder.readthedocs.io/>`__


Release: 3.0.3

Provider package
----------------

This package is for the ``fab`` provider.
All classes for this package are included in the ``airflow.providers.fab`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-fab``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.2``.

==========================================  ==========================================
PIP package                                 Version required
==========================================  ==========================================
``apache-airflow``                          ``>=3.0.2``
``apache-airflow-providers-common-compat``  ``>=1.8.0``
``blinker``                                 ``>=1.6.2; python_version < "3.13"``
``flask``                                   ``>=2.2.1,<2.3; python_version < "3.13"``
``flask-appbuilder``                        ``==5.0.1; python_version < "3.13"``
``flask-login``                             ``>=0.6.2; python_version < "3.13"``
``flask-session``                           ``>=0.8.0; python_version < "3.13"``
``msgpack``                                 ``>=1.0.0; python_version < "3.13"``
``flask-sqlalchemy``                        ``>=3.0.5; python_version < "3.13"``
``sqlalchemy``                              ``>=1.4.36,<2; python_version < "3.13"``
``flask-wtf``                               ``>=1.1.0; python_version < "3.13"``
``connexion[flask]``                        ``>=2.14.2,<3.0; python_version < "3.13"``
``jmespath``                                ``>=0.7.0; python_version < "3.13"``
``werkzeug``                                ``>=2.2,<4; python_version < "3.13"``
``wtforms``                                 ``>=3.0,<4; python_version < "3.13"``
``flask_limiter``                           ``>3,!=3.13,<4``
==========================================  ==========================================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider distributions in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-fab[common.compat]


==================================================================================================================  =================
Dependent package                                                                                                   Extra
==================================================================================================================  =================
`apache-airflow-providers-common-compat <https://airflow.apache.org/docs/apache-airflow-providers-common-compat>`_  ``common.compat``
==================================================================================================================  =================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-fab 3.0.3 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_fab-3.0.3.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_fab-3.0.3.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_fab-3.0.3.tar.gz.sha512>`__)
* `The apache-airflow-providers-fab 3.0.3 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_fab-3.0.3-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_fab-3.0.3-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_fab-3.0.3-py3-none-any.whl.sha512>`__)
