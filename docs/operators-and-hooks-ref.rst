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

Operators and Hooks Reference
=============================

.. contents:: Content
  :local:
  :depth: 1

.. _fundamentals:

Fundamentals
------------

**Base:**

.. list-table::
   :header-rows: 1

   * - Module
     - Guides

   * - :mod:`airflow.hooks.base_hook`
     -

   * - :mod:`airflow.hooks.dbapi_hook`
     -

   * - :mod:`airflow.models.baseoperator`
     -

   * - :mod:`airflow.sensors.base_sensor_operator`
     -

**Operators:**

.. list-table::
   :header-rows: 1

   * - Operators
     - Guides

   * - :mod:`airflow.operators.bash`
     - :doc:`How to use <howto/operator/bash>`

   * - :mod:`airflow.operators.branch_operator`
     -

   * - :mod:`airflow.operators.dagrun_operator`
     -

   * - :mod:`airflow.operators.dummy_operator`
     -

   * - :mod:`airflow.operators.email`
     -

   * - :mod:`airflow.operators.generic_transfer`
     -

   * - :mod:`airflow.operators.latest_only`
     -

   * - :mod:`airflow.operators.python`
     - :doc:`How to use <howto/operator/python>`

   * - :mod:`airflow.operators.subdag_operator`
     -

   * - :mod:`airflow.operators.sql`
     -

**Sensors:**

.. list-table::
   :header-rows: 1

   * - Sensors
     - Guides

   * - :mod:`airflow.sensors.bash`
     -

   * - :mod:`airflow.sensors.date_time_sensor`
     -

   * - :mod:`airflow.sensors.external_task_sensor`
     - :doc:`How to use <howto/operator/external_task_sensor>`

   * - :mod:`airflow.sensors.filesystem`
     -

   * - :mod:`airflow.sensors.python`
     -

   * - :mod:`airflow.sensors.sql_sensor`
     -

   * - :mod:`airflow.sensors.time_delta_sensor`
     -

   * - :mod:`airflow.sensors.time_sensor`
     -

   * - :mod:`airflow.sensors.weekday_sensor`
     -

   * - :mod:`airflow.sensors.smart_sensor_operator`
     - :doc:`smart-sensor`

**Hooks:**

.. list-table::
   :header-rows: 1

   * - Hooks
     - Guides

   * - :mod:`airflow.hooks.filesystem`
     -


.. _Apache:

ASF: Apache Software Foundation
-------------------------------

Airflow supports various software created by `Apache Software Foundation <https://www.apache.org/foundation/>`__.

Software operators and hooks
''''''''''''''''''''''''''''
These integrations allow you to perform various operations within software developed by Apache Software
Foundation.

.. operators-hooks-ref::
   :tags: apache
   :header-separator: "


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to software developed by Apache Software
Foundation.

.. transfers-ref::
   :tags: apache
   :header-separator: "

.. _Azure:

Azure: Microsoft Azure
----------------------

Airflow has limited support for `Microsoft Azure <https://azure.microsoft.com/>`__.

Some hooks are based on :mod:`airflow.providers.microsoft.azure.hooks.base_azure`
which authenticate Azure's Python SDK Clients.

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Microsoft Azure.

.. operators-hooks-ref::
   :tags: azure
   :header-separator: "

Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to Microsoft Azure.

.. transfers-ref::
   :tags: azure
   :header-separator: "


.. _AWS:

AWS: Amazon Web Services
------------------------

Airflow has support for `Amazon Web Services <https://aws.amazon.com/>`__.

All hooks are based on :mod:`airflow.providers.amazon.aws.hooks.base_aws`.

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Amazon Web Services.

.. operators-hooks-ref::
   :tags: aws
   :header-separator: "

Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to Amazon Web Services.

.. transfers-ref::
   :tags: aws
   :header-separator: "

.. _Google:

Google
------

Airflow has support for the `Google service <https://developer.google.com/>`__.

All hooks are based on :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`. Some integration
also use :mod:`airflow.providers.google.common.hooks.discovery_api`.

See the :doc:`Google Cloud connection type <howto/connection/gcp>` documentation to
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


.. _yc_service:

Yandex.Cloud
------------

Airflow has a limited support for the `Yandex.Cloud <https://cloud.yandex.com/>`__.

See the :doc:`Yandex.Cloud connection type <howto/connection/yandexcloud>` documentation to
configure connections to Yandex.Cloud.

All hooks are based on :class:`airflow.providers.yandex.hooks.yandex.YandexCloudBaseHook`.

.. note::
    You can learn how to use Yandex.Cloud integrations by analyzing the
    `example DAG <https://github.com/apache/airflow/blob/master/airflow/providers/yandex/example_dags/example_yandexcloud_dataproc.py>`_

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Yandex.Cloud.


.. operators-hooks-ref::
   :tags: yandex
   :header-separator: "

.. _service:

Service integrations
--------------------

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within various services.

.. operators-hooks-ref::
   :tags: service
   :header-separator: "


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations within various services.

.. transfers-ref::
   :tags: service
   :header-separator: "


.. _software:

Software integrations
---------------------

Software operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations using various software.

.. operators-hooks-ref::
   :tags: software
   :header-separator: "


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data.

.. transfers-ref::
   :tags: software
   :header-separator: "


.. _protocol:

Protocol integrations
---------------------

Protocol operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations within various services using standardized
communication protocols or interface.

.. operators-hooks-ref::
   :tags: protocol
   :header-separator: "

Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data.

.. transfers-ref::
   :tags: protocol
   :header-separator: "
