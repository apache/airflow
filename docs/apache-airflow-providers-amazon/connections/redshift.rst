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
----------------------

The default connection ID is ``redshift_default``.

Configuring the Connection
--------------------------

Host (optional)
  Specify the Amazon Redshift cluster endpoint.

Schema (optional)
  Specify the Amazon Redshift database name.

Login (optional)
  Specify the username to use for authentication with Amazon Redshift.

Password (optional)
  Specify the password to use for authentication with Amazon Redshift.

Port (optional)
  Specify the port to use to interact with Amazon Redshift.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in
    Amazon Redshift connection. For a complete list of supported parameters
    please see the `documentation <https://github.com/aws/amazon-redshift-python-driver#connection-parameters>`_
    for redshift_connector.

If you are configuring the connection via a URI, ensure that all components of the URI are URL-encoded.

Examples
--------

**Database Authentication**

* **Schema**: ``Dev``
* **Host**: ``redshift-cluster-1.123456789.us-west-1.redshift.amazonaws.com``
* **Login**: ``awsuser``
* **Password**: ``********``
* **Port**: ``5439``

**Credentials Authentication**

Uses the credentials in Connection to connect to Amazon Redshift. Port is required.
If none is provided, default is used (5439). This assumes all other Connection fields e.g. **Login** are empty.
In this method, **cluster_identifier** replaces **Host** and **Port** in order to uniquely identify the cluster.

**IAM Authentication**

Uses the AWS IAM profile given at hook initialization to retrieve a temporary password to connect
to Amazon Redshift. **Port** is required. If none is provided, default is used (5439). **Login**
and **Schema** are also required. This assumes all other Connection fields are empty.
In this method, if **cluster_identifier** is not set within the extras, it is automatically
inferred by the **Host** field in Connection.
`More details about AWS IAM authentication to generate database user credentials <https://docs.aws.amazon.com/redshift/latest/mgmt/generating-user-credentials.html>`_.

* **Extra**:

.. code-block:: json

    {
      "iam": true,
      "cluster_identifier": "redshift-cluster-1",
      "port": 5439,
      "region": "us-east-1",
      "db_user": "awsuser",
      "database": "dev",
      "profile": "default"
    }

If you want to use IAM with Amazon Redshift Serverless, you need to set **is_serverless** to true and provide
**serverless_work_group**. You can also set **serverless_token_duration_seconds** to specify the number of seconds
until the returned temporary password expires; the minimum is 900 seconds, the maximum is 3600 seconds and by default
it's 3600 seconds.

* **Extra**:

.. code-block:: json

    {
      "iam": true,
      "is_serverless": true,
      "serverless_work_group": "default",
      "serverless_token_duration_seconds": 3600,
      "port": 5439,
      "region": "us-east-1",
      "database": "dev",
      "profile": "default"
    }
