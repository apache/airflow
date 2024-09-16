
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

``apache-airflow-providers-celery``
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

    CeleryExecutor details <celery_executor>
    CeleryKubernetesExecutor details <celery_kubernetes_executor>


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Configuration <configurations-ref>
    CLI <cli-ref>
    Python API <_api/airflow/providers/celery/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-celery/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-celery package
------------------------------------------------------

`Celery <https://docs.celeryq.dev/en/stable/>`__


Release: 3.8.1

Provider package
----------------

This package is for the ``celery`` provider.
All classes for this package are included in the ``airflow.providers.celery`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation via
``pip install apache-airflow-providers-celery``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.8.0``.

==================  ==============================
PIP package         Version required
==================  ==============================
``apache-airflow``  ``>=2.8.0``
``celery[redis]``   ``>=5.3.0,!=5.3.2,!=5.3.3,<6``
``flower``          ``>=1.0.0``
``google-re2``      ``>=1.0``
==================  ==============================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-celery[cncf.kubernetes]


======================================================================================================================  ===================
Dependent package                                                                                                       Extra
======================================================================================================================  ===================
`apache-airflow-providers-cncf-kubernetes <https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes>`_  ``cncf.kubernetes``
======================================================================================================================  ===================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-celery 3.8.1 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_celery-3.8.1.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_celery-3.8.1.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_celery-3.8.1.tar.gz.sha512>`__)
* `The apache-airflow-providers-celery 3.8.1 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_celery-3.8.1-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_celery-3.8.1-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_celery-3.8.1-py3-none-any.whl.sha512>`__)
