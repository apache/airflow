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


Microsoft Graph API Sensors
=============================

MSGraphSensor
-------------
Use the
:class:`~airflow.providers.microsoft.azure.sensors.msgraph.MSGraphSensor` to poll a Power BI API.


Below is an example of using this sensor to poll the status of a PowerBI workspace.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_powerbi.py
    :language: python
    :dedent: 0
    :start-after: [START howto_sensor_powerbi_scan_status]
    :end-before: [END howto_sensor_powerbi_scan_status]


Reference
---------

For further information, look at:

* `Using the Power BI REST APIs <https://learn.microsoft.com/en-us/rest/api/power-bi/>`__
