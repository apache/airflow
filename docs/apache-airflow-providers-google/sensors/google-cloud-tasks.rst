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

.. _google_cloud_tasks_empty_sensor:

Google Cloud Tasks
==================
Cloud Tasks is a fully managed service that allows you to manage the execution, dispatch,
and delivery of a large number of distributed tasks.
Using Cloud Tasks, you can perform work asynchronously outside of a user or service-to-service request.

For more information about the service visit
`Cloud Tasks product documentation <https://cloud.google.com/tasks/docs>`__

Google Cloud Tasks Empty Sensor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To sense Queue being empty use
:class:`~airflow.providers.google.cloud.sensor.tasks.TaskQueueEmptySensor`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_task.py
    :language: python
    :dedent: 4
    :start-after: [START cloud_tasks_empty_sensor]
    :end-before: [END cloud_tasks_empty_sensor]
