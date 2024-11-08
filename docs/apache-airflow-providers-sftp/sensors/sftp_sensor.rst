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

.. exampleinclude:: /../../providers/tests/system/sftp/example_sftp_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sftp_sensor]
    :end-before: [END howto_operator_sftp_sensor]


We can also use Taskflow API. It takes the same arguments as the :class:`~airflow.providers.sftp.sensors.sftp.SFTPSensor` along with -

op_args (optional)
    A list of positional arguments that will get unpacked when
    calling your callable (templated)
op_kwargs (optional)
    A dictionary of keyword arguments that will get unpacked
    in your function (templated)

Whatever returned by the python callable is put into XCom.

.. exampleinclude:: /../../providers/tests/system/sftp/example_sftp_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sftp_sensor_decorator]
    :end-before: [END howto_operator_sftp_sensor_decorator]

Checks for the existence of a file on an SFTP server in the deferrable mode:

.. exampleinclude:: /../../providers/tests/system/sftp/example_sftp_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_sftp_deferrable]
    :end-before: [END howto_sensor_sftp_deferrable]
