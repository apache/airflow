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

.. _howto/connection:AWSHook:

Amazon Web Services Connection
==============================

The Amazon Web Services connection type enables the :ref:`AWS Integrations
<AWS>`.

Authenticating to AWS
---------------------

Authentication may be performed using any of the `boto3 options <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#configuring-credentials>`_. Alternatively, one can pass credentials in as a Connection initialisation parameter.

To use IAM instance profile, create an "empty" connection (i.e. one with no Login or Password specified).

Default Connection IDs
-----------------------

The default connection ID is ``aws_default``.

.. note:: Previously, the ``aws_default`` connection had the "extras" field set to ``{"region_name": "us-east-1"}`` 
    on install. This means that by default the ``aws_default`` connection used the ``us-east-1`` region. 
    This is no longer the case and the region needs to be set manually, either in the connection screens in Airflow, 
    or via the ``AWS_DEFAULT_REGION`` environment variable.


Configuring the Connection
==========================


Login (optional)
    Specify the AWS access key ID used for the initial connection.
    If you do an *assume_role* by specifying a ``role_arn`` in the **Extra** field,
    then temporary credentials will be used for subsequent calls to AWS.

Password (optional)
    Specify the AWS secret access key used for the initial connection.
    If you do an *assume_role* by specifying a ``role_arn`` in the **Extra** field,
    then temporary credentials will be used for subsequent calls to AWS.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in AWS
    connection. The following parameters are all optional:

    * ``aws_access_key_id``: Alternative to using the **Login** field.
    * ``aws_secret_access_key``: Alternative to using the **Password** field.
    * ``aws_session_token``: AWS session token used for the initial connection if you use external credentials. You are responsible for renewing these.

    * ``role_arn``: If specified, then an *assume_role* will be done to this role.
    * ``aws_account_id``: Used to construct ``role_arn`` if it was not specified.
    * ``aws_iam_role``: Used to construct ``role_arn`` if it was not specified.    
    * ``assume_role_kwargs``: Additional ``kwargs`` passed to *assume_role*.

    * ``host``: Endpoint URL for the connection.
    * ``region_name``: AWS region for the connection.
    * ``external_id``: AWS external ID for the connection (deprecated, rather use ``assume_role_kwargs``).

    * ``config_kwargs``: Additional ``kwargs`` used to construct a ``botocore.config.Config`` passed to *boto3.client* and *boto3.resource*.
    * ``session_kwargs``: Additional ``kwargs`` passed to *boto3.session.Session*.
    
Examples for the **Extra** field
--------------------------------

1. Using *~/.aws/credentials* and *~/.aws/config* file, with a profile.

This assumes all other Connection fields eg **Login** are empty.

.. code-block:: json

    {
      "session_kwargs": {
        "profile_name": "my_profile"
      }
    }


2. Specifying a role_arn to assume and a region_name

.. code-block:: json

    {
      "role_arn": "role_arn",
      "region_name": "ap-southeast-2"
    }


3. Configuring an outbound http proxy

.. code-block:: json

    {
      "config_kwargs": {
        "proxies": {
          "http": "http://myproxy:8080",
          "https": "http://myproxy:8080"
        }
      }
    }
