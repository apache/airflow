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

.. code-block:: bash

  AIRFLOW_CONN_REDSHIFT_DEFAULT=redshift://awsuser:password@redshift-cluster-1.123456789.us-west-1.redshift.amazonaws.com:5439/?database=dev&ssl=True
