
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

``apache-airflow-providers-presto``
===================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    PrestoTransferOperator types <operators/transfer/gcs_to_presto>

.. toctree::
    :maxdepth: 1
    :caption: Guides

    PrestoToSlackOperator types <operators/transfer/presto_to_slack>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/presto/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/providers-presto/3.0.0/tests/system/providers/presto>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-presto/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-presto
------------------------------------------------------

`Presto <https://prestodb.github.io/>`__


Release: 3.0.0

Provider package
----------------

This is a provider package for ``presto`` provider. All classes for this provider package
are in ``airflow.providers.presto`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-presto``

Requirements
------------

========================  ==================
PIP package               Version required
========================  ==================
``apache-airflow``        ``>=2.2.0``
``pandas``                ``>=0.17.1``
``presto-python-client``  ``>=0.8.2``
========================  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-presto[google]


====================================================================================================  ==========
Dependent package                                                                                     Extra
====================================================================================================  ==========
`apache-airflow-providers-google <https://airflow.apache.org/docs/apache-airflow-providers-google>`_  ``google``
`apache-airflow-providers-slack <https://airflow.apache.org/docs/apache-airflow-providers-slack>`_    ``slack``
====================================================================================================  ==========

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-presto 3.0.0 sdist package <https://downloads.apache.org/airflow/providers/apache-airflow-providers-presto-3.0.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache-airflow-providers-presto-3.0.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache-airflow-providers-presto-3.0.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-presto 3.0.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_presto-3.0.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_presto-3.0.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_presto-3.0.0-py3-none-any.whl.sha512>`__)

.. include:: ../../airflow/providers/presto/CHANGELOG.rst
