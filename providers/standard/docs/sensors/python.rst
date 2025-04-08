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



.. _howto/operator:PythonSensor:

PythonSensor
============

The :class:`~airflow.providers.standard.sensors.python.PythonSensor` executes an arbitrary callable and waits for its return
value to be True.

.. tip::
    The ``@task.sensor`` decorator is recommended over the classic ``PythonSensor``
    to execute Python callables to check for True condition.

.. tab-set::

    .. tab-item:: @task.sensor
        :sync: taskflow

        .. exampleinclude:: /../../../airflow-core/src/airflow/example_dags/example_sensor_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START wait_function]
            :end-before: [END wait_function]

    .. tab-item:: PythonSensor
        :sync: operator

        .. exampleinclude:: /../../../airflow-core/src/airflow/example_dags/example_sensors.py
            :language: python
            :dedent: 4
            :start-after: [START example_python_sensors]
            :end-before: [END example_python_sensors]
