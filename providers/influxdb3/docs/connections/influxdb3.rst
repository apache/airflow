
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

.. _howto/connection:influxdb3:

InfluxDB 3 Connection
=====================
The InfluxDB 3 connection type provides connection to an InfluxDB 3.x database
(Core/Enterprise/Cloud Dedicated).

InfluxDB 3.x uses SQL queries and a different API compared to InfluxDB 2.x.
For InfluxDB 2.x support, use the ``apache-airflow-providers-influxdb`` provider.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to (e.g., ``https://us-east-1-1.aws.cloud2.influxdata.com``).

Extra (required)
    Specify the extra parameters (as json dictionary) that can be used in InfluxDB 3
    connection.

    Example "extras" field:

    .. code-block:: JSON

      {
        "token": "your-api-token-here",
        "database": "my_database",
        "org": "my_org"
      }

    Parameters:
        * ``token`` (required): API token for authentication
        * ``database`` (required): Database name
        * ``org`` (optional): Organization name
