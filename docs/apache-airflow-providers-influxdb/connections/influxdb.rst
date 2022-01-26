
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

.. _howto/operator: Github Sensors:

You can build your own sensor on Repository using :class:`~airflow.providers.github.sensors.BaseGithubRepositorySensor`,
an example of this is :class:`~airflow.providers.github.sensors.GithubTagSensor`

GithubTagSensor
=================

Use the :class:`~airflow.providers.github.sensors.GithubTagSensor` to wait for creation of
a Tag in `Github <https://www.github.com/>`__.

An example of waiting for **v1.0**:

.. exampleinclude:: /../../airflow/providers/github/example_dags/example_github.py
    :language: python
    :start-after: [START howto_tag_sensor_github]
    :end-before: [END howto_tag_sensor_github]

