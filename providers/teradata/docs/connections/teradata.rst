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



.. _howto/connection:teradata:

Teradata Connection
======================
The Teradata connection type enables integrations with Teradata.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to.

Database (optional)
    Specify the name of the database to connect to.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Teradata
    connection. The following parameters out of the standard python parameters
    are supported:

    * ``tmode`` - Specifies the transaction mode.Possible values are DEFAULT (the default), ANSI, or TERA
    * ``sslmode`` - This option specifies the mode for connections to the database.
      There are six modes:
      ``disable``, ``allow``, ``prefer``, ``require``, ``verify-ca``, ``verify-full``.
    * ``sslca`` - This parameter specifies the file name of a PEM file that contains
      Certificate Authority (CA) certificates for use with sslmode values VERIFY-CA or VERIFY-FULL.
    * ``sslcapath`` - This parameter specifies the TLS cipher for HTTPS/TLS connections.
    * ``sslcipher`` - This parameter specifies the name of a file containing SSL
      certificate authority (CA) certificate(s).
    * ``sslcrc`` - This parameter controls TLS certificate revocation checking for
      HTTPS/TLS connections when sslmode is VERIFY-FULL.
    * ``sslprotocol`` - Specifies the TLS protocol for HTTPS/TLS connections.

    More details on all Teradata parameters supported can be found in
    `Teradata documentation <https://github.com/Teradata/python-driver?tab=readme-ov-file#connection-parameters>`_.

    Example "extras" field:

    .. code-block:: json

       {
          "tmode": "TERA",
          "sslmode": "verify-ca",
          "sslcert": "/tmp/client-cert.pem",
          "sslca": "/tmp/server-ca.pem",
          "sslkey": "/tmp/client-key.pem"
       }


    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

        export AIRFLOW_CONN_TERADATA_DEFAULT='teradata://teradata_user:XXXXXXXXXXXX@1.1.1.1:/teradatadb?tmode=tera&sslmode=verify-ca&sslca=%2Ftmp%2Fserver-ca.pem'

Setting QueryBand
-----------------
QueryBand can be specified using extra connection configuration parameter as below. The value specified in query_band will be set as session query band.

    .. code-block:: json

       {
          "query_band": "appname=airflow;org=test;"
       }

When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify query_band as URL-encoded as below.

For example:

    .. code-block:: bash

        export AIRFLOW_CONN_TERADATA_DEFAULT='teradata://teradata_user:XXXXXXXXXXXX@1.1.1.1:/teradatadb?query_band=appname%3Dairflow%3Borg%3Dtest%3B'
