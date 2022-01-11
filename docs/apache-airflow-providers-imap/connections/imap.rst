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



.. _howto/connection:imap:

IMAP Connection
===============

The IMAP connection type enables integrations with the IMAP client.

Authenticating to IMAP
----------------------

Authenticate to the IMAP client with the login and password field.
Use standard `IMAP authentication
<https://docs.python.org/3/library/imaplib.html>`_

Default Connection IDs
----------------------

Hooks, operators, and sensors related to IMAP use ``imap_default`` by default.

Configuring the Connection
--------------------------

Login
    Specify the username used for the IMAP client.

Password
    Specify the password used for the IMAP client.

Host
    Specify the IMAP host url.

Port
    Specify the IMAP port to connect to. The default depends on the whether you use ssl or not.

Extra (optional)
    Specify the extra parameters (as json dictionary)

    * ``use_ssl``: If set to false, then a non-ssl connection is being used. Default is true. Also note that changing the ssl option also influences the default port being used.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_IMAP_DEFAULT='imap://username:password@myimap.com:993?use_ssl=true'

Another example for connecting via a non-SSL connection.

.. code-block:: bash

   export AIRFLOW_CONN_IMAP_NONSSL='imap://username:password@myimap.com:143?use_ssl=false'

Note that you can set the port regardless of whether you choose to use ssl or not. The above examples show default ports for SSL and Non-SSL connections.
