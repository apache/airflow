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

Samba Connection
=================

The Samba connection type enables connection to Samba server.

Default Connection IDs
----------------------

Samba Hook uses parameter ``samba_conn_id`` for Connection IDs and the value of the
parameter as ``samba_default`` by default.

Configuring the Connection
--------------------------
Host
    The host of the Samba server.

Port
    Specify the port to use for connecting the Samba server (default port is ``445``).

Share
    The default share that will be used in case it is not specified in the ``SambaHook``.

Login
    The user that will be used for authentication against the Samba server.

Password
    The password of the user that will be used for authentication against the Samba server.

Share Type
    The share OS type (``posix`` or ``windows``). Used to determine the formatting of file and folder paths.
