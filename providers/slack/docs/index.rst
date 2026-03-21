
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
    :caption: System tests

    System Tests <_api/tests/system/slack/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

    Connection Types <connections/index>
    Operators <operators/index>
    Slack Notifications <notifications/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/slack/index>
    Example Dags <https://github.com/apache/airflow/tree/providers-slack/|version|/providers/slack/tests/system/slack>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-slack/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-slack package
------------------------------------------------------

`Slack <https://slack.com/>`__ services integration including:

  - `Slack API <https://api.slack.com/>`__
  - `Slack Incoming Webhook <https://api.slack.com/messaging/webhooks>`__


Release: 9.8.0

Provider package
----------------

This package is for the ``slack`` provider.
All classes for this package are included in the ``airflow.providers.slack`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-slack``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==========================================  ======================================
PIP package                                 Version required
==========================================  ======================================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.10.1``
``apache-airflow-providers-common-sql``     ``>=1.27.0``
``slack-sdk``                               ``>=3.36.0``
``asgiref``                                 ``>=2.3.0; python_version < "3.14"``
``asgiref``                                 ``>=3.11.1; python_version >= "3.14"``
==========================================  ======================================
