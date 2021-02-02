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



Google Cloud Storage Sensors
============================

Cloud Storage allows world-wide storage and retrieval of any amount of data at any time. 
You can use Cloud Storage for a range of scenarios including serving website content, 
storing data for archival and disaster recovery, or distributing large data objects to users via direct download.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/sensors/_partials/prerequisite_tasks.rst

.. _howto/sensor:GCSObjectExistenceSensor:

GCSObjectExistenceSensor
------------------------

Use the :class:`~airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor` to wait (poll) for the existence of a file in Google Cloud Storage.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gcs_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_object_exists_task]
    :end-before: [END howto_sensor_object_exists_task]

.. _howto/sensor:GCSObjectsWtihPrefixExistenceSensor:

GCSObjectsWtihPrefixExistenceSensor
-----------------------------------

Use the :class:`~airflow.providers.google.cloud.sensors.gcs.GCSObjectsWtihPrefixExistenceSensor` to wait (poll) for the existence of a file with a specified prefix in Google Cloud Storage.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gcs_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_object_with_prefix_exists_task]
    :end-before: [END howto_sensor_object_with_prefix_exists_task]

More information
""""""""""""""""

Sensors have different modes that determine the behaviour of resources while the task is executing.
See `Airflow sensors documentation
<https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#sensors>`_ for best practices when using sensors.

Reference
---------

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/storage/index.html>`__
* `Product Documentation <https://cloud.google.com/storage/docs>`__