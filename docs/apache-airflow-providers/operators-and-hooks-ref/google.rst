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

.. _Google:

Google
------

Airflow has support for the `Google service <https://developer.google.com/>`__.

All hooks are based on :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`. Some integration
also use :mod:`airflow.providers.google.common.hooks.discovery_api`.

See the :doc:`Google Cloud connection type <apache-airflow-providers-google:connections/gcp>` documentation to
configure connections to Google services.

.. _GCP:

Google Cloud
''''''''''''

Airflow has extensive support for the `Google Cloud <https://cloud.google.com/>`__.

.. note::
    You can learn how to use Google Cloud integrations by analyzing the
    `source code of the Google Cloud example DAGs
    <https://github.com/apache/airflow/tree/master/airflow/providers/google/cloud/example_dags/>`_


Service operators and hooks
"""""""""""""""""""""""""""

These integrations allow you to perform various operations within the Google Cloud.

.. operators-hooks-ref::
   :tags: gcp
   :header-separator: !


Transfer operators and hooks
""""""""""""""""""""""""""""

These integrations allow you to copy data from/to Google Cloud.

.. transfers-ref::
   :tags: gcp
   :header-separator: !


Google Marketing Platform
'''''''''''''''''''''''''

.. note::
    You can learn how to use Google Marketing Platform integrations by analyzing the
    `source code <https://github.com/apache/airflow/tree/master/airflow/providers/google/marketing_platform/example_dags/>`_
    of the example DAGs.


.. operators-hooks-ref::
   :tags: gmp
   :header-separator: !


Other Google operators and hooks
''''''''''''''''''''''''''''''''

.. operators-hooks-ref::
   :tags: google
   :header-separator: !
