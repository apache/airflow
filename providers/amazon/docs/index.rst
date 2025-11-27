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

``apache-airflow-providers-amazon``
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

    Connection types <connections/index>
    Notifications <notifications/index>
    Operators <operators/index>
    Transfers <transfer/index>
    Deferrable Operators <deferrable>
    Secrets backends <secrets-backends/index>
    Logging for Tasks <logging/index>
    Configuration <configurations-ref>
    Executors <executors/index>
    Message Queues <message-queues/index>
    AWS Auth manager <auth-manager/index>
    CLI <cli-ref>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/amazon/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/amazon/index>
    System Tests Dashboard <https://aws-mwaa.github.io/open-source/system-tests/dashboard.html>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example Dags <example-dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-amazon/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-amazon package
------------------------------------------------------

Amazon integration (including `Amazon Web Services (AWS) <https://aws.amazon.com/>`__).


Release: 9.18.0

Provider package
----------------

This package is for the ``amazon`` provider.
All classes for this package are included in the ``airflow.providers.amazon`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-amazon``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==========================================  ======================================
PIP package                                 Version required
==========================================  ======================================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.8.0``
``apache-airflow-providers-common-sql``     ``>=1.27.0``
``apache-airflow-providers-http``
``boto3``                                   ``>=1.37.2``
``botocore``                                ``>=1.37.2``
``inflection``                              ``>=0.5.1``
``watchtower``                              ``>=3.3.1,<4``
``jsonpath_ng``                             ``>=1.5.3``
``redshift_connector``                      ``>=2.1.3``
``asgiref``                                 ``>=2.3.0``
``PyAthena``                                ``>=3.10.0``
``jmespath``                                ``>=0.7.0``
``sagemaker-studio``                        ``>=1.0.9``
``pydynamodb``                              ``>=0.7.5; python_version >= "3.13"``
``sqlean.py``                               ``>=3.47.0; python_version >= "3.13"``
``marshmallow``                             ``>=3``
==========================================  ======================================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider distributions in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-amazon[apache.hive]


========================================================================================================================  ====================
Dependent package                                                                                                         Extra
========================================================================================================================  ====================
`apache-airflow-providers-apache-hive <https://airflow.apache.org/docs/apache-airflow-providers-apache-hive>`_            ``apache.hive``
`apache-airflow-providers-cncf-kubernetes <https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes>`_    ``cncf.kubernetes``
`apache-airflow-providers-common-compat <https://airflow.apache.org/docs/apache-airflow-providers-common-compat>`_        ``common.compat``
`apache-airflow-providers-common-messaging <https://airflow.apache.org/docs/apache-airflow-providers-common-messaging>`_  ``common.messaging``
`apache-airflow-providers-common-sql <https://airflow.apache.org/docs/apache-airflow-providers-common-sql>`_              ``common.sql``
`apache-airflow-providers-exasol <https://airflow.apache.org/docs/apache-airflow-providers-exasol>`_                      ``exasol``
`apache-airflow-providers-ftp <https://airflow.apache.org/docs/apache-airflow-providers-ftp>`_                            ``ftp``
`apache-airflow-providers-google <https://airflow.apache.org/docs/apache-airflow-providers-google>`_                      ``google``
`apache-airflow-providers-http <https://airflow.apache.org/docs/apache-airflow-providers-http>`_                          ``http``
`apache-airflow-providers-imap <https://airflow.apache.org/docs/apache-airflow-providers-imap>`_                          ``imap``
`apache-airflow-providers-microsoft-azure <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure>`_    ``microsoft.azure``
`apache-airflow-providers-mongo <https://airflow.apache.org/docs/apache-airflow-providers-mongo>`_                        ``mongo``
`apache-airflow-providers-openlineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage>`_            ``openlineage``
`apache-airflow-providers-salesforce <https://airflow.apache.org/docs/apache-airflow-providers-salesforce>`_              ``salesforce``
`apache-airflow-providers-ssh <https://airflow.apache.org/docs/apache-airflow-providers-ssh>`_                            ``ssh``
========================================================================================================================  ====================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-amazon 9.18.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_amazon-9.18.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_amazon-9.18.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_amazon-9.18.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-amazon 9.18.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_amazon-9.18.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_amazon-9.18.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_amazon-9.18.0-py3-none-any.whl.sha512>`__)
