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

``apache-airflow-providers-google``
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
    Logging handlers <logging/index>
    Secrets backends <secrets-backends/google-cloud-secret-manager-backend>
    API Authentication backend <api-auth-backend/google-openid>
    Operators <operators/index>
    Sensors <sensors/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/google/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/google/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example DAGs <example-dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-google/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-google package
------------------------------------------------------

Google services including:

  - `Google Ads <https://ads.google.com/>`__
  - `Google Cloud (GCP) <https://cloud.google.com/>`__
  - `Google Firebase <https://firebase.google.com/>`__
  - `Google LevelDB <https://github.com/google/leveldb/>`__
  - `Google Marketing Platform <https://marketingplatform.google.com/>`__
  - `Google Workspace <https://workspace.google.com/>`__ (formerly Google Suite)


Release: 10.24.0

Provider package
----------------

This package is for the ``google`` provider.
All classes for this package are included in the ``airflow.providers.google`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation via
``pip install apache-airflow-providers-google``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.8.0``.

==========================================  =========================================
PIP package                                 Version required
==========================================  =========================================
``apache-airflow``                          ``>=2.8.0``
``apache-airflow-providers-common-compat``  ``>=1.1.0``
``apache-airflow-providers-common-sql``     ``>=1.7.2``
``asgiref``                                 ``>=3.5.2``
``dill``                                    ``>=0.2.3``
``gcloud-aio-auth``                         ``>=5.2.0``
``gcloud-aio-bigquery``                     ``>=6.1.2``
``gcloud-aio-storage``                      ``>=9.0.0``
``gcsfs``                                   ``>=2023.10.0``
``google-ads``                              ``>=25.0.0``
``google-analytics-admin``                  ``>=0.9.0``
``google-api-core``                         ``>=2.11.0,!=2.16.0,!=2.18.0``
``google-api-python-client``                ``>=2.0.2``
``google-auth``                             ``>=2.29.0``
``google-auth-httplib2``                    ``>=0.0.1``
``google-cloud-aiplatform``                 ``>=1.63.0``
``google-cloud-automl``                     ``>=2.12.0``
``google-cloud-bigquery``                   ``!=3.21.*,!=3.22.0,!=3.23.*,>=3.4.0``
``google-cloud-bigquery-datatransfer``      ``>=3.13.0``
``google-cloud-bigtable``                   ``>=2.17.0``
``google-cloud-build``                      ``>=3.22.0``
``google-cloud-compute``                    ``>=1.10.0``
``google-cloud-container``                  ``>=2.17.4``
``google-cloud-datacatalog``                ``>=3.11.1``
``google-cloud-dataflow-client``            ``>=0.8.6``
``google-cloud-dataform``                   ``>=0.5.0``
``google-cloud-dataplex``                   ``>=1.10.0``
``google-cloud-dataproc``                   ``>=5.12.0``
``google-cloud-dataproc-metastore``         ``>=1.12.0``
``google-cloud-dlp``                        ``>=3.12.0``
``google-cloud-kms``                        ``>=2.15.0``
``google-cloud-language``                   ``>=2.9.0``
``google-cloud-logging``                    ``>=3.5.0``
``google-cloud-memcache``                   ``>=1.7.0``
``google-cloud-monitoring``                 ``>=2.18.0``
``google-cloud-orchestration-airflow``      ``>=1.10.0``
``google-cloud-os-login``                   ``>=2.9.1``
``google-cloud-pubsub``                     ``>=2.19.0``
``google-cloud-redis``                      ``>=2.12.0``
``google-cloud-secret-manager``             ``>=2.16.0``
``google-cloud-spanner``                    ``>=3.11.1,!=3.49.0``
``google-cloud-speech``                     ``>=2.18.0``
``google-cloud-storage``                    ``>=2.7.0``
``google-cloud-storage-transfer``           ``>=1.4.1``
``google-cloud-tasks``                      ``>=2.13.0``
``google-cloud-texttospeech``               ``>=2.14.1``
``google-cloud-translate``                  ``>=3.11.0``
``google-cloud-videointelligence``          ``>=2.11.0``
``google-cloud-vision``                     ``>=3.4.0``
``google-cloud-workflows``                  ``>=1.10.0``
``google-cloud-run``                        ``>=0.10.0``
``google-cloud-batch``                      ``>=0.13.0``
``grpcio-gcp``                              ``>=0.2.2``
``httpx``                                   ``>=0.25.0``
``json-merge-patch``                        ``>=0.2``
``looker-sdk``                              ``>=22.4.0``
``pandas-gbq``                              ``>=0.7.0``
``pandas``                                  ``>=2.1.2,<2.2; python_version >= "3.9"``
``pandas``                                  ``>=1.5.3,<2.2; python_version < "3.9"``
``proto-plus``                              ``>=1.19.6``
``python-slugify``                          ``>=7.0.0``
``PyOpenSSL``                               ``>=23.0.0``
``sqlalchemy-bigquery``                     ``>=1.2.1``
``sqlalchemy-spanner``                      ``>=1.6.2``
``tenacity``                                ``>=8.1.0``
``immutabledict``                           ``>=4.2.0``
==========================================  =========================================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-google[amazon]


========================================================================================================================  ====================
Dependent package                                                                                                         Extra
========================================================================================================================  ====================
`apache-airflow-providers-amazon <https://airflow.apache.org/docs/apache-airflow-providers-amazon>`_                      ``amazon``
`apache-airflow-providers-apache-beam <https://airflow.apache.org/docs/apache-airflow-providers-apache-beam>`_            ``apache.beam``
`apache-airflow-providers-apache-cassandra <https://airflow.apache.org/docs/apache-airflow-providers-apache-cassandra>`_  ``apache.cassandra``
`apache-airflow-providers-cncf-kubernetes <https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes>`_    ``cncf.kubernetes``
`apache-airflow-providers-common-compat <https://airflow.apache.org/docs/apache-airflow-providers-common-compat>`_        ``common.compat``
`apache-airflow-providers-common-sql <https://airflow.apache.org/docs/apache-airflow-providers-common-sql>`_              ``common.sql``
`apache-airflow-providers-facebook <https://airflow.apache.org/docs/apache-airflow-providers-facebook>`_                  ``facebook``
`apache-airflow-providers-microsoft-azure <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure>`_    ``microsoft.azure``
`apache-airflow-providers-microsoft-mssql <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql>`_    ``microsoft.mssql``
`apache-airflow-providers-mysql <https://airflow.apache.org/docs/apache-airflow-providers-mysql>`_                        ``mysql``
`apache-airflow-providers-openlineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage>`_            ``openlineage``
`apache-airflow-providers-oracle <https://airflow.apache.org/docs/apache-airflow-providers-oracle>`_                      ``oracle``
`apache-airflow-providers-postgres <https://airflow.apache.org/docs/apache-airflow-providers-postgres>`_                  ``postgres``
`apache-airflow-providers-presto <https://airflow.apache.org/docs/apache-airflow-providers-presto>`_                      ``presto``
`apache-airflow-providers-salesforce <https://airflow.apache.org/docs/apache-airflow-providers-salesforce>`_              ``salesforce``
`apache-airflow-providers-sftp <https://airflow.apache.org/docs/apache-airflow-providers-sftp>`_                          ``sftp``
`apache-airflow-providers-ssh <https://airflow.apache.org/docs/apache-airflow-providers-ssh>`_                            ``ssh``
`apache-airflow-providers-trino <https://airflow.apache.org/docs/apache-airflow-providers-trino>`_                        ``trino``
========================================================================================================================  ====================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-google 10.24.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_google-10.24.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_google-10.24.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_google-10.24.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-google 10.24.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_google-10.24.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_google-10.24.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_google-10.24.0-py3-none-any.whl.sha512>`__)
