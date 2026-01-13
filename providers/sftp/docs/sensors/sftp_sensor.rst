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

SFTP Sensor
===========

Looks for either a specific file or files with a specific pattern in a server using SFTP protocol.
To get more information about this sensor visit :class:`~airflow.providers.sftp.sensors.sftp.SFTPSensor`

.. exampleinclude:: /../../sftp/tests/system/sftp/example_sftp_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sftp_sensor]
    :end-before: [END howto_operator_sftp_sensor]


We can also use TaskFlow API. It takes the same arguments as the :class:`~airflow.providers.sftp.sensors.sftp.SFTPSensor` along with -

python_callable (optional)
    A callable that will be executed after files matching the sensor criteria are found.
    This allows you to process the found files with custom logic. The callable receives:

    - Positional arguments from ``op_args``
    - Keyword arguments from ``op_kwargs``, with ``files_found`` automatically added
      (if ``op_kwargs`` is provided and not empty) containing the list of files that matched
      the sensor criteria

    The return value of the callable is stored in XCom along with the ``files_found`` list,
    accessible via ``{"files_found": [...], "decorator_return_value": <callable_return_value>}``.

op_args (optional)
    A list of positional arguments that will get unpacked when calling your callable (templated).
    Only used when ``python_callable`` is provided.

op_kwargs (optional)
    A dictionary of keyword arguments that will get unpacked in your function (templated).
    If provided and not empty, the ``files_found`` list is automatically added to this dictionary
    when the callable is invoked. Only used when ``python_callable`` is provided.

.. exampleinclude:: /../../sftp/tests/system/sftp/example_sftp_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sftp_sensor_decorator]
    :end-before: [END howto_operator_sftp_sensor_decorator]

Checks for the existence of a file on an SFTP server in the deferrable mode:

.. exampleinclude:: /../../sftp/tests/system/sftp/example_sftp_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_sftp_deferrable]
    :end-before: [END howto_sensor_sftp_deferrable]
