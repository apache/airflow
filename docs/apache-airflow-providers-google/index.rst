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

    System Tests <_api/tests/system/providers/google/index>

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


Package apache-airflow-providers-google
------------------------------------------------------

Google services including:

  - `Google Ads <https://ads.google.com/>`__
  - `Google Cloud (GCP) <https://cloud.google.com/>`__
  - `Google Firebase <https://firebase.google.com/>`__
  - `Google LevelDB <https://github.com/google/leveldb/>`__
  - `Google Marketing Platform <https://marketingplatform.google.com/>`__
  - `Google Workspace <https://workspace.google.com/>`__ (formerly Google Suite)


Release: 10.11.1

Provider package
----------------

This is a provider package for ``google`` provider. All classes for this provider package
are in ``airflow.providers.google`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-google``

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.5.0``.

=======================================  ==================
PIP package                              Version required
=======================================  ==================
``apache-airflow``                       ``>=2.5.0``
``apache-airflow-providers-common-sql``  ``>=1.7.2``
``asgiref``                              ``>=3.5.2``
``gcloud-aio-auth``                      ``>=4.0.0,<5.0.0``
``gcloud-aio-bigquery``                  ``>=6.1.2``
``gcloud-aio-storage``
``gcsfs``                                ``>=2023.9.2``
``google-ads``                           ``>=22.1.0``
``google-api-core``                      ``>=2.11.0``
``google-api-python-client``             ``>=1.6.0``
``google-auth``                          ``>=1.0.0``
``google-auth-httplib2``                 ``>=0.0.1``
``google-cloud-aiplatform``              ``>=1.22.1``
``google-cloud-automl``                  ``>=2.11.0``
``google-cloud-bigquery-datatransfer``   ``>=3.11.0``
``google-cloud-bigtable``                ``>=2.17.0``
``google-cloud-build``                   ``>=3.13.0``
``google-cloud-compute``                 ``>=1.10.0``
``google-cloud-container``               ``>=2.17.4``
``google-cloud-datacatalog``             ``>=3.11.1``
``google-cloud-dataflow-client``         ``>=0.8.2``
``google-cloud-dataform``                ``>=0.5.0``
``google-cloud-dataplex``                ``>=1.4.2``
``google-cloud-dataproc``                ``>=5.4.0``
``google-cloud-dataproc-metastore``      ``>=1.12.0``
``google-cloud-dlp``                     ``>=3.12.0``
``google-cloud-kms``                     ``>=2.15.0``
``google-cloud-language``                ``>=2.9.0``
``google-cloud-logging``                 ``>=3.5.0``
``google-cloud-memcache``                ``>=1.7.0``
``google-cloud-monitoring``              ``>=2.14.1``
``google-cloud-orchestration-airflow``   ``>=1.7.0``
``google-cloud-os-login``                ``>=2.9.1``
``google-cloud-pubsub``                  ``>=2.15.0``
``google-cloud-redis``                   ``>=2.12.0``
``google-cloud-secret-manager``          ``>=2.16.0``
``google-cloud-spanner``                 ``>=3.11.1``
``google-cloud-speech``                  ``>=2.18.0``
``google-cloud-storage``                 ``>=2.7.0``
``google-cloud-storage-transfer``        ``>=1.4.1``
``google-cloud-tasks``                   ``>=2.13.0``
``google-cloud-texttospeech``            ``>=2.14.1``
``google-cloud-translate``               ``>=3.11.0``
``google-cloud-videointelligence``       ``>=2.11.0``
``google-cloud-vision``                  ``>=3.4.0``
``google-cloud-workflows``               ``>=1.10.0``
``google-cloud-run``                     ``>=0.9.0``
``google-cloud-batch``                   ``>=0.13.0``
``grpcio-gcp``                           ``>=0.2.2``
``httpx``
``json-merge-patch``                     ``>=0.2``
``looker-sdk``                           ``>=22.2.0``
``pandas-gbq``
``pandas``                               ``>=0.17.1``
``proto-plus``                           ``>=1.19.6``
``PyOpenSSL``
``sqlalchemy-bigquery``                  ``>=1.2.1``
``sqlalchemy-spanner``                   ``>=1.6.2``
=======================================  ==================

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

* `The apache-airflow-providers-google 10.11.1 sdist package <https://downloads.apache.org/airflow/providers/apache-airflow-providers-google-10.11.1.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache-airflow-providers-google-10.11.1.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache-airflow-providers-google-10.11.1.tar.gz.sha512>`__)
* `The apache-airflow-providers-google 10.11.1 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_google-10.11.1-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_google-10.11.1-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_google-10.11.1-py3-none-any.whl.sha512>`__)
