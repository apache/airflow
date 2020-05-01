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
      "role_arn": "arn:aws:iam::112223334444:role/my_role",
      "region_name": "ap-southeast-2"
    }

.. seealso::
    https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#api_assumerole


3. Configuring an outbound HTTP proxy

.. code-block:: json

    {
      "config_kwargs": {
        "proxies": {
          "http": "http://myproxy.mycompany.local:8080",
          "https": "http://myproxy.mycompany.local:8080"
        }
      }
    }

4. Using AssumeRoleWithSAML

.. code-block:: json

    {
      "region_name":"eu-west-1",
      "role_arn":"arn:aws:iam::112223334444:role/my_role",
      "assume_role_method":"assume_role_with_saml",
      "assume_role_with_saml":{
        "principal_arn":"arn:aws:iam::112223334444:saml-provider/my_saml_provider",
        "idp_url":"https://idp.mycompany.local/.../saml/clients/amazon-aws",
        "idp_auth_method":"http_spegno_auth",
        "mutual_authentication":"OPTIONAL",
        "idp_request_kwargs":{
          "headers":{"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
          "verify":false
        },
        "log_idp_response":false,
        "saml_response_xpath":"////INPUT[@NAME='SAMLResponse']/@VALUE",
      },
      "assume_role_kwargs": { "something":"something" }
    }


The following settings may be used within the ``assume_role_with_saml`` container in Extra.

    * ``principal_arn``: The ARN of the SAML provider created in IAM that describes the identity provider.
    * ``idp_url``: The URL to your IDP endpoint, which provides SAML Assertions.
    * ``idp_auth_method``: Specify "http_spegno_auth" to use the Python ``requests_gssapi`` library. This library is more up to date than ``requests_kerberos`` and is backward compatible. See ``requests_gssapi`` documentation on PyPi.
    * ``mutual_authentication``: Can be "REQUIRED", "OPTIONAL" or "DISABLED". See ``requests_gssapi`` documentation on PyPi.
    * ``idp_request_kwargs``: Additional ``kwargs`` passed to ``requests`` when requesting from the IDP (over HTTP/S).
    * ``log_idp_response``: Useful for debugging - if specified, print the IDP response content to the log. Note that a successful response will contain sensitive information!
    * ``saml_response_xpath``: How to query the IDP response using XML / HTML xpath.
    * ``assume_role_kwargs``: Additional ``kwargs`` passed to ``sts_client.assume_role_with_saml``.

.. note:: The ``requests_gssapi`` library is used to obtain a SAML response from your IDP.
    You may need to ``pip uninstall python-gssapi`` and ``pip install gssapi`` instead for this to work.
    The ``python-gssapi`` library is outdated, and conflicts with some versions of ``paramiko`` which Airflow uses elsewhere.

.. seealso::
    :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#api_assumerolewithsaml
    https://pypi.org/project/requests-gssapi/
