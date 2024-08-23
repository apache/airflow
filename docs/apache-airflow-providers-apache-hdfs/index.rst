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

``apache-airflow-providers-apache-hdfs``
========================================



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

    Connection types <connections>
    Operators <operators/index>
    Logging for Tasks <logging/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/apache/hdfs/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-apache-hdfs/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-apache-hdfs package
------------------------------------------------------

`Hadoop Distributed File System (HDFS) <https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html>`__
and `WebHDFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`__.


Release: 4.5.0

Provider package
----------------

This package is for the ``apache.hdfs`` provider.
All classes for this package are included in the ``airflow.providers.apache.hdfs`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation via
``pip install apache-airflow-providers-apache-hdfs``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.8.0``.

=================================  =========================================
PIP package                        Version required
=================================  =========================================
``apache-airflow``                 ``>=2.8.0``
``hdfs[avro,dataframe,kerberos]``  ``>=2.5.4; python_version < "3.12"``
``hdfs[avro,dataframe,kerberos]``  ``>=2.7.3; python_version >= "3.12"``
``pandas``                         ``>=2.1.2,<2.2; python_version >= "3.9"``
``pandas``                         ``>=1.5.3,<2.2; python_version < "3.9"``
=================================  =========================================
