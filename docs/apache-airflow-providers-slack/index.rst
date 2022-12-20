
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

``apache-airflow-providers-slack``
==================================

Content
-------
.. toctree::
    :hidden:
    :caption: System tests

    System Tests <_api/tests/system/providers/slack/index>
.. toctree::
    :maxdepth: 1
    :caption: Guides

    How-to Guide <operators/slack_operator_howto_guide>

.. toctree::
    :maxdepth: 1
    :caption: Guides

    SqlToSlackOperator types <operators/sql_to_slack>

.. toctree::
    :maxdepth: 1
    :caption: References

    Connection Types <connections/index>
    Python API <_api/airflow/providers/slack/index>
    Example DAGs <https://github.com/apache/airflow/tree/providers-slack/|version|/tests/system/providers/slack>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-slack/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-slack
------------------------------------------------------

`Slack <https://slack.com/>`__


Release: 7.1.0

Provider package
----------------

This is a provider package for ``slack`` provider. All classes for this provider package
are in ``airflow.providers.slack`` python package.

Installation
------------

You can install this package on top of an existing Airflow 2 installation (see ``Requirements`` below)
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-slack``

Requirements
------------

=======================================  ==================
PIP package                              Version required
=======================================  ==================
``apache-airflow``                       ``>=2.3.0``
``apache-airflow-providers-common-sql``  ``>=1.3.1``
``slack_sdk``                            ``>=3.0.0``
=======================================  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-slack[common.sql]


============================================================================================================  ==============
Dependent package                                                                                             Extra
============================================================================================================  ==============
`apache-airflow-providers-common-sql <https://airflow.apache.org/docs/apache-airflow-providers-common-sql>`_  ``common.sql``
============================================================================================================  ==============

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-slack 7.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache-airflow-providers-slack-7.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache-airflow-providers-slack-7.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache-airflow-providers-slack-7.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-slack 7.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_slack-7.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_slack-7.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_slack-7.1.0-py3-none-any.whl.sha512>`__)

.. include:: ../../airflow/providers/slack/CHANGELOG.rst
