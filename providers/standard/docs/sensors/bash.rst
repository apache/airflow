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



.. _howto/operator:BashSensor:

BashSensor
==========

Use the :class:`~airflow.providers.standard.sensors.bash.BashSensor` to use arbitrary command for sensing. The command
should return 0 when it succeeds, any other value otherwise.

.. exampleinclude:: /../../../airflow-core/src/airflow/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_bash_sensors]
    :end-before: [END example_bash_sensors]
