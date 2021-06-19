
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

``apache-airflow-providers-grpc``
=================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/grpc>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/grpc/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-grpc/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-grpc
------------------------------------------------------

`gRPC <https://grpc.io/>`__


Release: 2.0.0

Provider package
----------------

This is a provider package for ``grpc`` provider. All classes for this provider package
are in ``airflow.providers.grpc`` python package.

Installation
------------

You can install this package on top of an existing airflow 2.1+ installation via
``pip install apache-airflow-providers-grpc``

PIP requirements
----------------

========================  ======================
PIP package               Version required
========================  ======================
``apache-airflow``        ``>=2.1.0``
``google-auth-httplib2``  ``>=0.0.1``
``google-auth``           ``>=1.0.0, <2.0.0dev``
``grpcio``                ``>=1.15.0``
========================  ======================

.. include:: ../../airflow/providers/grpc/CHANGELOG.rst
