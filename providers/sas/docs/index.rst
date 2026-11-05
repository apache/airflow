
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

``apache-airflow-providers-sas``
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

    Connection Types <connections/sas>
    Using the hook <operators/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/sas/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-sas/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-sas package
------------------------------------------------------

`SAS <https://www.sas.com/>`__


Release: 1.0.0

Provider package
----------------

This package is for the ``sas`` provider.
All classes for this package are included in the ``airflow.providers.sas`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-sas``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==========================================  ==================================================================
PIP package                                 Version required
==========================================  ==================================================================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.21.0``
``apache-airflow-providers-common-sql``     ``>=1.32.0``
``apache-airflow-providers-ssh``            ``>=7.0.1``
``asyncssh``                                ``>=2.15.0; python_version < "3.14"``
``asyncssh``                                ``>=2.22.0; python_version >= "3.14"``
``methodtools``                             ``>=0.4.7``
``pandas``                                  ``>=2.1.2; python_version < "3.13"``
``pandas``                                  ``>=2.2.3; python_version >= "3.13" and python_version < "3.14"``
``pandas``                                  ``>=2.3.3; python_version >= "3.14"``
``paramiko``                                ``>=4.0.0``
``pyarrow``                                 ``>=16.1.0; python_version < "3.13"``
``pyarrow``                                 ``>=18.0.0; python_version >= "3.13" and python_version < "3.14"``
``pyarrow``                                 ``>=22.0.0; python_version >= "3.14"``
==========================================  ==================================================================

Optional dependencies
---------------------

These extras install optional third-party libraries that enable additional features of the provider.
Install them when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-sas[polars]


==========  ==================
Extra       Dependencies
==========  ==================
``polars``  ``polars>=1.26.0``
==========  ==================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-sas 1.0.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_sas-1.0.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_sas-1.0.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_sas-1.0.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-sas 1.0.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_sas-1.0.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_sas-1.0.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_sas-1.0.0-py3-none-any.whl.sha512>`__)
