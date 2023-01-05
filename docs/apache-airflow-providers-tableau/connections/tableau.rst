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



.. _howto/connection:tableau:

Tableau Connection
==================

The Tableau connection type enables the Tableau Integrations.

Authenticating to Tableau
-------------------------

There are two ways to connect to Tableau using Airflow.

1. Use `Password and Username Authentication
   <https://tableau.github.io/server-client-python/docs/api-ref#tableauauth-class>`_
   i.e. add a ``password`` and ``login`` to the Airflow connection.
2. Use a `Token Authentication
   <https://tableau.github.io/server-client-python/docs/api-ref#personalaccesstokenauth-class>`_
   i.e. add a ``token_name`` and ``personal_access_token`` to the Airflow connection (deprecated).

Authentication by personal token was deprecated as Tableau automatically invalidates opened
personal token connection if one or more parallel connections with the same token are opened.
So, in the environments with multiple parallel tasks this authentication method can lead to numerous bugs
and all the jobs will not run as they intended. Therefore, personal token auth option
is considered harmful until the logic of Tableau server client changes.

Only one authorization method can be used at a time. If you need to manage multiple credentials or keys then you should
configure multiple connections.

Default Connection IDs
----------------------

All hooks and operators related to Tableau use ``tableau_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify the tableau username used for the initial connection. Used with password authentication.

Password (optional)
    Specify the tableau password used for the initial connection.
    Used with password authentication.

Host
    Specify the `Sever URL
    <https://tableau.github.io/server-client-python/docs/api-ref#server>`_ used for Tableau.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Azure connection.
    The following parameters are all optional:

    * ``site_id``: This corresponds to the contentUrl attribute in the Tableau REST API. The ``site_id`` is the portion of
      the URL that follows the **/site/** in the URL. For example, **MarketingTeam** is the ``site_id`` in the following URL
      **MyServer/#/site/MarketingTeam/projects**. To specify the default site on Tableau Server, you can use an empty string
      **''** (single quotes, no space). For Tableau Online, you must provide a value for the ``site_id.``
      This is used for both token and password Authentication.
    * ``token_name``: The personal access token name.
      This is used with token authentication.
    * ``personal_access_token``: The personal access token value.
      This is used with token authentication.
    * ``verify``: Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string, in which case it must be a path to a CA bundle to use. Defaults to True.
    * ``cert``: if String, path to ssl client cert file (.pem). If Tuple, ('cert', 'key') pair.


When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_TABLEAU_DEFAULT='tableau://username:password@https%3A%2F%2FMY-SERVER%2F?site_id=example-id'
