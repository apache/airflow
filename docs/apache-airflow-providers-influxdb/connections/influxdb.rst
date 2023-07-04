
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

.. _howto/connection:influxdb:

InfluxDB Connection
====================
The InfluxDB connection type provides connection to a InfluxDB database.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to.

Extra (required)
    Specify the extra parameters (as json dictionary) that can be used in InfluxDB
    connection.

    ``token``: (required) `Create token <https://docs.influxdata.com/influxdb/cloud/security/tokens/create-token/>`_
    using the influxdb cli or UI

    ``org_name``: (required) `Create org <https://docs.influxdata.com/influxdb/cloud/reference/cli/influx/org/create/>`_
    name using influxdb cli or UI

    Example "extras" field:

    .. code-block:: JSON

      {
        "token": "343434343423234234234343434",
        "org_name": "Test"
      }
