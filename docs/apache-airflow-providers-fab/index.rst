
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

    Auth manager <auth-manager/index>
    Upgrading <upgrading>

.. toctree::
    :hidden:
    :caption: Internal DB details

    Database Migrations <migrations-ref>

.. toctree::
    :hidden:
    :caption: References

    Stable REST API <stable-rest-api-ref>

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


Release: 1.4.0

Provider package
----------------

This package is for the ``fab`` provider.
All classes for this package are included in the ``airflow.providers.fab`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation via
``pip install apache-airflow-providers-fab``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.9.0``.

====================  ==================
PIP package           Version required
====================  ==================
``apache-airflow``    ``>=2.9.0``
``flask``             ``>=2.2,<2.3``
``flask-appbuilder``  ``==4.5.0``
``flask-login``       ``>=0.6.2``
``google-re2``        ``>=1.0``
``jmespath``          ``>=0.7.0``
====================  ==================
