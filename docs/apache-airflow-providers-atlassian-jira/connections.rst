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

Jira Connection
===============

The Jira connection type enables connection to Atlassian Jira.

Default Connection IDs
----------------------

Jira Hook uses parameter ``jira_conn_id`` for Connection IDs and the value of the
parameter as ``jira_default`` by default.

Configuring the Connection
--------------------------
Host
    The Jira host (should be with scheme).

Port
    Specify the port to use for connecting to Jira.

Login
    The user that will be used for authentication against the Jira API.

Password
    The password of the user that will be used for authentication against the Jira API.

Verify SSL
    Whether to verify SSL when connecting to the Jira API (this is ``True`` by default).
