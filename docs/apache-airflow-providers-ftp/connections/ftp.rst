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



.. _howto/connection:ftp:

FTP Connection
===============

The FTP connection type enables the FTP Integrations.

Authenticating to FTP
-----------------------

Authenticate to FTP using `ftplib
<https://docs.python.org/3/library/ftplib.html>`_.
i.e. indicate ``user``, ``password``, ``host``

Default Connection IDs
----------------------

Hooks, operators, and sensors related to FTP use ``ftp_default`` by default.

Configuring the Connection
--------------------------

Login
    Specify the ftp ``user`` value.

Password
    Specify the ftp ``passwd`` value.

Host (optional)
    Specify the Hostname or IP of the remote machine.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in ftp connection.
    The following parameters are all optional:

    * ``passive``: Enable "passive" mode if val is true, otherwise disable passive mode.
      Passive mode is on by default.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

Example connection string:

.. code-block:: bash

   export AIRFLOW_CONN_FTP_DEFAULT='ftp://user:pass@localhost?passive=false'
