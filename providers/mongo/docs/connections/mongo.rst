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



.. _howto/connection:mongo:

MongoDB Connection
==================

The MongoDB connection type enables the MongoDB Integrations.

Authenticating to MongoDB
-------------------------

Authenticate to mongo using a `mongo connection string
<https://docs.mongodb.com/manual/reference/connection-string/>`_.

Default Connection IDs
----------------------

Some hooks and sensors related to Mongo use ``mongo_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    MongoDB username that used in the connection string for the database you
    wish to connect too.

Password (optional)
    MongoDB password that used in the connection string for the database you
    wish to connect too.

Port (optional)
    MongoDB database port number used with in the connection string.

Host (optional)
    The hostname for the standalone mongodb instance used in the connection
    string.

Schema (optional)
    Any information that you would like to specify after the port number
    in the connections string. Such as the `authentication database
    <https://docs.mongodb.com/manual/reference/connection-string/#mongodb-urioption-urioption.authSource>`_.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Azure connection.
    The following parameters are all optional:

    * ``srv``: (bool) Specify if to use srv to indicate that
      the hostname corresponds to DNS SRV record. False by default.
    * ``ssl``: (bool) Specify to use SSL to connect instead of certs.
    * ``**options``: The rest of the json object can be used to specify `options
      <https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options>`_
      sent to the mongo client.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded. The URI and the mongo
connection string are not the same.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_MONGO_DEFAULT='mongo://username:password@mongodb.example.com:27317/%3FauthSource%3Dadmin'
