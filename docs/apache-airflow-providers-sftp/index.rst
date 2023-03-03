
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

``apache-airflow-providers-sftp``
=================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: References

    Connection types <connections/sftp>
    Python API <_api/airflow/providers/sftp/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-sftp/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-sftp
------------------------------------------------------

`SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__


Release: 4.2.4

Provider package
----------------

This is a provider package for ``sftp`` provider. All classes for this provider package
are in ``airflow.providers.sftp`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-sftp``

Requirements
------------

================================  ==================
PIP package                       Version required
================================  ==================
``apache-airflow``                ``>=2.3.0``
``apache-airflow-providers-ssh``  ``>=2.1.0``
================================  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-sftp[ssh]


==============================================================================================  =======
Dependent package                                                                               Extra
==============================================================================================  =======
`apache-airflow-providers-ssh <https://airflow.apache.org/docs/apache-airflow-providers-ssh>`_  ``ssh``
==============================================================================================  =======

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-sftp 4.2.4 sdist package <https://downloads.apache.org/airflow/providers/apache-airflow-providers-sftp-4.2.4.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache-airflow-providers-sftp-4.2.4.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache-airflow-providers-sftp-4.2.4.tar.gz.sha512>`__)
* `The apache-airflow-providers-sftp 4.2.4 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_sftp-4.2.4-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_sftp-4.2.4-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_sftp-4.2.4-py3-none-any.whl.sha512>`__)

.. include:: ../../airflow/providers/sftp/CHANGELOG.rst
