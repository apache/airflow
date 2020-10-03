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

Apache Sqoop Connection
=======================

The Apache Sqoop connection type enables connection to Apache Sqoop.

Default Connection IDs
----------------------

Sqoop Hook and Sqoop Operator use parameter ``conn_id`` for Connection IDs and the value of the parameter as ``sqoop_default`` by default.

Configuring the Connection
--------------------------
Host
    The host to connect to, it can be ``local``, ``yarn`` or an URL.

Port
    Specify the port in case of host be an URL.

Extra (optional, connection parameters)
    Specify the extra parameters (as json dictionary) that can be used in sqoop connection. The following parameters out of the standard python parameters are supported:

    * ``job_tracker``: Job tracker local|jobtracker:port.
    * ``properties`` - Properties to set via the -D argument.
    * ``files``: Comma separated files to be copied to the map reduce cluster.
    * ``archives``: Comma separated archives to be unarchived on the compute machines
    * ``password_file``: Path to file containing the password.
