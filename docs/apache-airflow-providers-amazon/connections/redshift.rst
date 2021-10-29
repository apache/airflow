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

.. _howto/connection:redshift:

Amazon Redshift Connection
==========================

The Redshift connection type enables integrations with Redshift.

Authenticating to Amazon Redshift
---------------------------------

Authentication may be performed using any of the authentication methods supported by `redshift_connector <https://github.com/aws/amazon-redshift-python-driver>`_ such as via direct credentials, IAM authentication, or using an Identity Provider (IdP) plugin.

Default Connection IDs
-----------------------

The default connection ID is ``redshift_default``.

Configuring the Connection
--------------------------


User
  Specify the username to use for authentication with Amazon Redshift.

Password
  Specify the password to use for authentication with Amazon Redshift.

Host
  Specify the Amazon Redshift hostname.

Database
  Specify the Amazon Redshift database name.

Extra
    Specify the extra parameters (as json dictionary) that can be used in
    Amazon Redshift connection. For a complete list of supported parameters
    please see the `documentation <https://github.com/aws/amazon-redshift-python-driver#connection-parameters>`_
    for redshift_connector.


When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

Examples
--------
Database Authentication

.. code-block:: pycon

  >>> from airflow.models.connection import Connection
  >>> import json

  >>> c = Connection(
  ...     conn_type="redshift",
  ...     extra=json.dumps(
  ...         {
  ...             "user": "awsuser",
  ...             "password": "password",
  ...             "host": "redshift-cluster-1.123456789.us-west-1.redshift.amazonaws.com",
  ...             "port": 5439,
  ...             "database": "dev",
  ...             "ssl": True,
  ...             "sslmode": "verify-full",
  ...         }
  ...     ),
  ... )
  >>> print(c.get_uri())
  redshift://?__extra__=%7B%22user%22%3A+%22awsuser%22%2C+%22password%22%3A+%22password%22%2C+%22host%22%3A+%22redshift-cluster-1.123456789.us-west-1.redshift.amazonaws.com%22%2C+%22port%22%3A+5439%2C+%22database%22%3A+%22dev%22%2C+%22ssl%22%3A+true%2C+%22sslmode%22%3A+%22verify-full%22%7D


IAM Authentication using AWS Profile

.. code-block:: pycon

  >>> from airflow.models.connection import Connection
  >>> import json

  >>> c = Connection(
  ...     conn_type="redshift",
  ...     extra=json.dumps(
  ...         {
  ...             "iam": True,
  ...             "db_user": "awsuser",
  ...             "database": "dev",
  ...             "cluster_identifier": "redshift-cluster-1",
  ...             "profile": "default",
  ...         }
  ...     ),
  ... )
  >>> print(c.get_uri())
  redshift://?__extra__=%7B%22iam%22%3A+true%2C+%22db_user%22%3A+%22awsuser%22%2C+%22database%22%3A+%22dev%22%2C+%22cluster_identifier%22%3A+%22redshift-cluster-1%22%2C+%22profile%22%3A+%22default%22%7D

Authentication using Okta Identity Provider

.. code-block:: pycon

  >>> from airflow.models.connection import Connection
  >>> import json

  >>> c = Connection(
  ...     conn_type="redshift",
  ...     extra=json.dumps(
  ...         {
  ...             "iam": True,
  ...             "user": "developer@domain.org",
  ...             "password": "myOktaPassword",
  ...             "database": "dev",
  ...             "cluster_identifier": "redshift-cluster-1",
  ...             "credentials_provider": "OktaCredentialsProvider",
  ...             "idp_host": "my_idp_host",
  ...             "app_id": "myAppId",
  ...             "app_name": "myAppName",
  ...         }
  ...     ),
  ... )
  >>> print(c.get_uri())
  redshift://?__extra__=%7B%22iam%22%3A+true%2C+%22user%22%3A+%22developer%40domain.org%22%2C+%22password%22%3A+%22myOktaPassword%22%2C+%22database%22%3A+%22dev%22%2C+%22cluster_identifier%22%3A+%22redshift-cluster-1%22%2C+%22credentials_provider%22%3A+%22OktaCredentialsProvider%22%2C+%22idp_host%22%3A+%22my_idp_host%22%2C+%22app_id%22%3A+%22myAppId%22%2C+%22app_name%22%3A+%22myAppName%22%7D
