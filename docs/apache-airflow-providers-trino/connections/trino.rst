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



Trino Connection
================
The Trino connection type provides connection to a Trino SQL engine.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to.

Port (optional)
    The port to connect to.

Username (required)
    Specify the username to connect.

Password (optional)
    Specify the password to connect.

Catalog (optional)
    Specify the Trino catalog name to be used in the database.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in trino
    connection. The following parameters out of the standard python parameters
    are supported:

    * ``kerberos__config`` - Path to ``krb5.conf`` config file.
    * ``kerberos__principal`` - Override kubernetes principle name.
    * ``kerberos__service_name`` - The service principal name of the target.
    * ``kerberos__delegate`` - Enable credential delegation, default ``False``.
    * ``kerberos__mutual_authentication`` - Enable mutual authentication, default: ``False``.
    * ``kerberos__sanitize_mutual_error_response`` - Sanitize the error response
      if response content is more important than the need for mutual auth on errors.
    * ``kerberos__force_preemptive`` - Forced to preemptively initiate the Kerberos GSS exchange
      and present a Kerberos ticket on the initial request (and all subsequent).
    * ``kerberos__hostname_override`` - Override the hostname if communicating with a host
      whose DNS name doesn't match its kerberos hostname.
    * ``kerberos__ca_bundle`` - Path to custom SSL Certificate Authority bundle file.

    More details on all Trino's Kerberos parameters supported can be found in
    `requests-kerberos <https://github.com/requests/requests-kerberos>`_.

    Example "extras" field:

    .. code-block:: json

          {
              "auth": "kerberos",
              "kerberos__config": "/etc/krb5.conf",
              "kerberos__principal": "primary/instance@REALM",
              "kerberos__delegate": false,
              "kerberos__mutual_authentication": false,
              "kerberos__force_preemptive": true,
              "kerberos__ca_bundle": "/path/to/ca-bundle.pem"
          }
