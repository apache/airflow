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

To use IAM instance profile, create an "empty" connection (i.e. one with no Login or Password specified, or
``aws://``).

Default Connection IDs
-----------------------

The default connection ID is ``aws_default``.

.. note:: Previously, the ``aws_default`` connection had the "extras" field set to ``{"region_name": "us-east-1"}``
    on install. This means that by default the ``aws_default`` connection used the ``us-east-1`` region.
    This is no longer the case and the region needs to be set manually, either in the connection screens in Airflow,
    or via the ``AWS_DEFAULT_REGION`` environment variable.


Configuring the Connection
--------------------------


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

If you are configuring the connection via a URI, ensure that all components of the URI are URL-encoded.

Examples
--------

**Using instance profile**:
  .. code-block:: bash

    export AIRFLOW_CONN_AWS_DEFAULT=aws://

  This will use boto's default credential look-up chain (the profile named "default" from the ~/.boto/ config files, and instance profile when running inside AWS)

**With a AWS IAM key pair**:
  .. code-block:: bash

    export AIRFLOW_CONN_AWS_DEFAULT=aws://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY@

  Note here, that the secret access key has been URL-encoded (changing ``/`` to ``%2F``), and also the
  trailing ``@`` (without which, it is treated as ``<host>:<port>`` and will not work)


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
    * ``idp_auth_method``: Specify "http_spegno_auth" to use the Python ``requests_gssapi`` library. This library is more up to date than ``requests_kerberos`` and is backward compatible. See ``requests_gssapi`` documentation on PyPI.
    * ``mutual_authentication``: Can be "REQUIRED", "OPTIONAL" or "DISABLED". See ``requests_gssapi`` documentation on PyPI.
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

.. _howto/connection:aws:gcp-federation:

Google Cloud to AWS authentication using Web Identity Federation
----------------------------------------------------------------


Thanks to `Web Identity Federation <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html>`__, you can use the credentials from the Google Cloud platform to authorize
access in the Amazon Web Service platform. If you additionally use authorizations with access token obtained
from `metadata server <https://cloud.google.com/compute/docs/storing-retrieving-metadata>`__ or
`Workload Identity <https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#gke_mds>`__,
you can improve the security of your environment by eliminating long-lived credentials.

The Google Cloud credentials is exchanged for the Amazon Web Service
`temporary credentials <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html>`__
by `AWS Security Token Service <https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html>`__.

The following diagram illustrates a typical communication flow used to obtain the AWS credentials.

.. figure::  /img/aws-web-identity-federation-gcp.png

    Communication Flow Diagram

Role setup
^^^^^^^^^^

In order for a Google identity to be recognized by AWS, you must configure roles in AWS.

You can do it by using the role wizard or by using `the Terraform <https://www.terraform.io/>`__.

Role wizard
"""""""""""

To create an IAM role for web identity federation:

1. Sign in to the AWS Management Console and open the IAM console at https://console.aws.amazon.com/iam/.
2. In the navigation pane, choose **Roles** and then choose **Create role**.
3. Choose the **Web identity** role type.
4. For Identity provider, choose the **Google**.
5. Type the service account email address (in the form ``<NAME>@<PROJECT_ID>.iam.gserviceaccount.com``) into the **Audience** box.
6. Review your web identity information and then choose **Next: Permissions**.
7. Select the policy to use for the permissions policy or choose **Create policy** to open a new browser tab and create a new policy from scratch. For more information, see `Creating IAM Policy <https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_create-console.html#access_policies_create-start>`__.
8. Choose **Next: Tags**.
9. (Optional) Add metadata to the role by attaching tags as key–value pairs. For more information about using tags in IAM, see `Tagging IAM users and roles <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_tags.html>`__.
10. Choose **Next: Review**.
11. For **Role name**, type a role name. Role names must be unique within your AWS account.
12. (Optional) For **Role description**, type a description for the new role.
13. Review the role and then choose **Create role**.

For more information, see: `Creating a role for web identity or OpenID connect federation (console) <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-idp_oidc.html>`__

Finally, you should get a role that has a similar policy to the one below:

.. code-block:: json

    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
            "Federated": "accounts.google.com"
          },
          "Action": "sts:AssumeRoleWithWebIdentity",
          "Condition": {
            "StringEquals": {
              "accounts.google.com:aud": "<NAME>@<PROJECT_ID>.iam.gserviceaccount.com"
            }
          }
        }
      ]
    }

In order to protect against the misuse of the Google OpenID token, you can also limit the scope of use by configuring
restrictions per audience. You will need to configure the same value for the connection, and then this value also included in the ID Token. AWS will test if this value matches.
For that, you can add a new condition to the policy.

.. code-block:: json

    {
      "Condition": {
        "StringEquals": {
          "accounts.google.com:aud": "<NAME>@<PROJECT_ID>.iam.gserviceaccount.com",
          "accounts.google.com:oaud": "service-amp.my-company.com"
        }
      }
    }

After creating the role, you should configure the connection in Airflow.

Terraform
"""""""""

In order to quickly configure a new role, you can use the following Terraform script, which configures
AWS roles along with the assigned policy.
Before using it, you need correct the variables in the ``locals`` section to suit your environment:

* ``google_service_account`` - The email address of the service account that will have permission to use
  this role
* ``google_openid_audience`` - Constant value that is configured in the Airflow role and connection.
  It prevents misuse of the Google ID token.
* ``aws_role_name`` - The name of the new AWS role.
* ``aws_policy_name`` - The name of the new AWS policy.


For more information on using Terraform scripts, see:
`Terraform docs - Get started - AWS <https://learn.hashicorp.com/collections/terraform/aws-get-started>`__

After executing the plan, you should configure the connection in Airflow.

.. code-block: terraform

    locals {
      google_service_account = "<NAME>@<PROJECT>.iam.gserviceaccount.com"
      google_openid_audience = "<SERVICE_NAME>.<DOMAIN>"
      aws_role_name          = "WebIdentity-Role"
      aws_policy_name        = "WebIdentity-Role"
    }

    terraform {
      required_providers {
        aws = {
          source  = "hashicorp/aws"
          version = "~> 3.0"
        }
      }
    }

    provider "aws" {
      region = "us-east-1"
    }

    data "aws_iam_policy_document" "assume_role_policy" {
      statement {
        actions = [
          "sts:AssumeRoleWithWebIdentity"
        ]
        effect = "Allow"

        condition {
          test = "StringEquals"
          variable = "accounts.google.com:aud"
          values = [local.google_service_account]
        }

        condition {
          test = "StringEquals"
          variable = "accounts.google.com:oaud"
          values = [local.google_openid_audience]
        }

        principals {
          identifiers = ["accounts.google.com"]
          type = "Federated"
        }
      }
    }

    resource "aws_iam_role" "role_web_identity" {
      name               = local.aws_role_name
      description        = "Terraform managed policy"
      path               = "/"
      assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
    }
    # terraform import aws_iam_role.role_web_identity "WebIdentity-Role"

    data "aws_iam_policy_document" "web_identity_bucket_policy_document" {
      statement {
        effect = "Allow"
        actions = [
          "s3:ListAllMyBuckets"
        ]
        resources = ["*"]
      }
    }

    resource "aws_iam_policy" "web_identity_bucket_policy" {
      name = local.aws_policy_name
      path = "/"
      description = "Terraform managed policy"
      policy = data.aws_iam_policy_document.web_identity_bucket_policy_document.json
    }
    # terraform import aws_iam_policy.web_identity_bucket_policy arn:aws:iam::240057002457:policy/WebIdentity-S3-Policy


    resource "aws_iam_role_policy_attachment" "policy-attach" {
      role       = aws_iam_role.role_web_identity.name
      policy_arn = aws_iam_policy.web_identity_bucket_policy.arn
    }
    # terraform import aws_iam_role_policy_attachment.policy-attach WebIdentity-Role/arn:aws:iam::240057002457:policy/WebIdentity-S3-Policy


Connection setup
^^^^^^^^^^^^^^^^

In order to use a Google identity, field ``"assume_role_method"`` must be ``"assume_role_with_web_identity"`` and
field ``"assume_role_with_web_identity_federation"`` must be ``"google"`` in the extra section
of the connection setup. It also requires that you set up roles in the ``"role_arn"`` field.
Optionally, you can limit the use of the Google Open ID token by configuring the
``"assume_role_with_web_identity_federation_audience"`` field. The value of these fields must match the value configured in the role.

Airflow will establish Google's credentials based on `the Application Default Credentials <https://cloud.google.com/docs/authentication/production>`__.

Below is an example connection configuration.

.. code-block:: json

  {
    "role_arn": "arn:aws:iam::240057002457:role/WebIdentity-Role",
    "assume_role_method": "assume_role_with_web_identity",
    "assume_role_with_web_identity_federation": "google",
    "assume_role_with_web_identity_federation_audience": "service_a.apache.com"
  }

You can configure connection, also using environmental variable :envvar:`AIRFLOW_CONN_{CONN_ID}`.

.. code-block:: bash

    export AIRFLOW_CONN_AWS_DEFAULT="aws://\
    ?role_arn=arn%3Aaws%3Aiam%3A%3A240057002457%3Arole%2FWebIdentity-Role&\
    assume_role_method=assume_role_with_web_identity&\
    assume_role_with_web_identity_federation=google&\
    assume_role_with_web_identity_federation_audience=aaa.polidea.com"
