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



Neo4j Connection
================
The Neo4j connection type provides connection to a Neo4j database.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Neo4j
    connection.

    The following extras are supported:

        - Default - uses bolt scheme(bolt://)
        - neo4j_scheme - neo4j://
        - certs_self_signed - neo4j+ssc://
        - certs_trusted_ca - neo4j+s://

      * ``encrypted``: Sets encrypted=True/False for GraphDatabase.driver, Set to ``True`` for Neo4j Aura.
      * ``neo4j_scheme``: Specifies the scheme to ``neo4j://``, default is ``bolt://``
      * ``certs_self_signed``: Sets the URI scheme to support self-signed certificates(``neo4j+ssc://``)
      * ``certs_trusted_ca``: Sets the URI scheme to support only trusted CA(``neo4j+s://``)

      Example "extras" field:

      .. code-block:: json

         {
            "encrypted": true,
            "neo4j_scheme": true,
            "certs_self_signed": true,
            "certs_trusted_ca": false
         }
