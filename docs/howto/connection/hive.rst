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

Apache Hive Connection
======================

The Apache Hive connection type enables connection to Apache Hive.

Default Connection IDs
----------------------

Hive CLI hook and Hive Operator use parameter ``hive_cli_conn_id`` for Connection IDs and the value of the parameter as ``hive_cli_default`` by default.

Configuring the Connection
--------------------------
Host (optional, for beeline jdbc url)
    The host to connect to, it can be ``local``, ``yarn`` or an URL.

Port (optional, for beeline jdbc url)
    Specify the port in case of host be an URL.

Extra (optional, connection parameters)
    Specify the extra parameters (as json dictionary) that can be used in hive connection. The following parameters out of the standard python parameters are supported:

    * ``auth`` - Connection parameter gets passed as in the ``jdbc`` connection string. Possible settings inlude: NONE, NOSASL, KERBEROS, PLAINSASL, LDAP.
    * ``mapred_queue`` - Queue used by the Hadoop Scheduler (Capacity or Fair)
    * ``mapred_queue_priority`` - Priority within the job queue. Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW.
    * ``mapred_job_name`` - This name will appear in the jobtracker. This can make monitoring easier.
