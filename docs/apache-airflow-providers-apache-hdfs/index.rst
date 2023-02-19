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


Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections>
    Operators <operators/index>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/apache/hdfs/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-apache-hdfs/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-apache-hdfs
------------------------------------------------------

`Hadoop Distributed File System (HDFS) <https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html>`__
and `WebHDFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`__.

.. note::

   The ``snakebite-py3`` used by the provider is an old package and it has an old version of ``argparse`` in
   its dependencies and it might cause in some cases running airflow commands raises the error similar to
   ``TypeError: __init__() got an unexpected keyword argument 'encoding'``. In this case make
   sure to remove ``argparse`` with ``pip uninstall argparse`` command to get rid of this error.


Release: 3.2.0

Provider package
----------------

This is a provider package for ``apache.hdfs`` provider. All classes for this provider package
are in ``airflow.providers.apache.hdfs`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-apache-hdfs``

Requirements
------------

=================================  ==================
PIP package                        Version required
=================================  ==================
``apache-airflow``                 ``>=2.3.0``
``snakebite-py3``
``hdfs[avro,dataframe,kerberos]``  ``>=2.0.4``
=================================  ==================

.. include:: ../../airflow/providers/apache/hdfs/CHANGELOG.rst
