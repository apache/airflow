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

The default connection ID is ``aws_default``. If the environment/machine where you are running Airflow has the
file credentials in ``/home/.aws/``, and the default connection has user and pass fields empty, it will take
automatically the credentials from there.

.. note:: Previously, the ``aws_default`` connection had the "extras" field set to ``{"region_name": "us-east-1"}``
    on install. This means that by default the ``aws_default`` connection used the ``us-east-1`` region.
    This is no longer the case and the region needs to be set manually, either in the connection screens in Airflow,
    or via the ``AWS_DEFAULT_REGION`` environment variable.


Configuring the Connection
--------------------------


Login (optional)
    Specify the AWS access key ID used for the initial connection.
    If you do an `assume role <https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html>`__
    by specifying a ``role_arn`` in the **Extra** field,
    then temporary credentials will be used for subsequent calls to AWS.

Password (optional)
    Specify the AWS secret access key used for the initial connection.
    If you do an `assume role <https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html>`__
    by specifying a ``role_arn`` in the **Extra** field,
    then temporary credentials will be used for subsequent calls to AWS.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in AWS
    connection. The following parameters are all optional:

    * ``aws_access_key_id``: AWS access key ID used for the initial connection.
    * ``aws_secret_access_key``: AWS secret access key used for the initial connection
    * ``aws_session_token``: AWS session token used for the initial connection if you use external credentials.
      You are responsible for renewing these.
    * ``region_name``: AWS Region for the connection.
    * ``session_kwargs``: Additional **kwargs** passed to
      `boto3.session.Session <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html>`__.
    * ``config_kwargs``: Additional **kwargs** used to construct a
      `botocore.config.Config <https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html>`__
      passed to `boto3.client <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client>`__
      and `boto3.resource <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.resource>`__.
    * ``role_arn``: If specified, then assume this role, obtaining a set of temporary security credentials using the ``assume_role_method``.
    * ``assume_role_method``: AWS STS client method, one of
      `assume_role <https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html>`__,
      `assume_role_with_saml <https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithSAML.html>`__ or
      `assume_role_with_web_identity <https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html>`__
      if not specified then **assume_role** is used.
    * ``assume_role_kwargs``: Additional **kwargs** passed to ``assume_role_method``.
    * ``host``: Endpoint URL for the connection.

.. warning:: Extra parameters below are deprecated and will be removed in a future version of this provider.

    * ``aws_account_id``: Used to construct ``role_arn`` if it was not specified.
    * ``aws_iam_role``: Used to construct ``role_arn`` if it was not specified.
    * ``external_id``: A unique identifier that might be required when you assume a role in another account.
      Used if ``ExternalId`` in ``assume_role_kwargs`` was not specified.
    * ``s3_config_file``: Path to local credentials file.
    * ``s3_config_format``: ``s3_config_file`` format, one of
      `aws <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-settings>`_,
      `boto <http://boto.cloudhackers.com/en/latest/boto_config_tut.html#details>`_ or
      `s3cmd <https://s3tools.org/kb/item14.htm>`_ if not specified then **boto** is used.
    * ``profile``: If you are getting your credentials from the ``s3_config_file``
      you can specify the profile with this parameter.

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
        "idp_request_retry_kwargs": {
          "total": 10,
          "backoff_factor":1,
          "status":10,
          "status_forcelist": [400, 429, 500, 502, 503, 504]
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
    * ``idp_request_retry_kwargs``: Additional ``kwargs`` to construct a
      `urllib3.util.Retry <https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry>`_
      used as a retry strategy when requesting from the IDP.
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


Avoid Throttling exceptions
---------------------------

Amazon Web Services have quota limits for simultaneous API call as result with frequent calls
``apache-airflow-providers-amazon`` components might fail during execution with a
throttling exception, e.g. *ThrottlingException*, *ProvisionedThroughputExceededException*.

``botocore.config.Config`` supports different exponential backoff modes out of the box:
``legacy``, ``standard``, ``adaptive``

By default, ``botocore.config.Config`` uses ``legacy`` mode with 5 maximum retry attempts,
which may not be enough in some cases.

If you encounter throttling exceptions, you may change the mode to ``standard`` with more retry attempts.


.. seealso::
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html#retries

Set in Connection
^^^^^^^^^^^^^^^^^

**Connection extra field**:
  .. code-block:: json

    {
      "config_kwargs": {
        "retries": {
          "mode": "standard",
          "max_attempts": 10
        }
      }
    }

Set in AWS Config File
^^^^^^^^^^^^^^^^^^^^^^

**~/.aws/config**:
  .. code-block:: ini

    [awesome_aws_profile]
    retry_mode = standard
    max_attempts = 10

**Connection extra field**:
  .. code-block:: json

    {
      "session_kwargs": {
        "profile_name": "awesome_aws_profile"
      }
    }

Set by Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  .. note:: This sets the retry mode on all connections,
    unless another retry config is explicitly set on a specific connection.

  .. code-block:: bash

    export AWS_RETRY_MODE=standard
    export AWS_MAX_ATTEMPTS=10


.. _howto/connection:aws:session-factory:

Session Factory
---------------

The default ``BaseSessionFactory`` for the connection can handle most of the authentication methods for AWS.
In the case that you would like to have full control of
`boto3 session <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html>`__ creation or
you are using custom `federation <https://aws.amazon.com/identity/federation/>`__ that requires
`external process to source the credentials <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sourcing-external.html>`__,
you can subclass :class:`~airflow.providers.amazon.aws.hooks.base_aws.BaseSessionFactory` and override ``create_session``
and/or ``_create_basic_session`` method depending on your needs.

You will also need to add configuration for ``AwsBaseHook`` to use the custom implementation by their full path.

Example
^^^^^^^

**Configuration**:
  .. code-block:: ini

    [aws]
    session_factory = my_company.aws.MyCustomSessionFactory

**Connection extra field**:
  .. code-block:: json

    {
      "federation": {
        "username": "my_username",
        "password": "my_password"
      }
    }

**Custom Session Factory**:
  .. code-block:: python

    def get_federated_aws_credentials(username: str, password: str):
        """
        Mock interaction with federation endpoint/process and returns AWS credentials.
        """
        return {
            "Version": 1,
            "AccessKeyId": "key",
            "SecretAccessKey": "secret",
            "SessionToken": "token",
            "Expiration": "2050-12-31T00:00:00.000Z",
        }


    class MyCustomSessionFactory(BaseSessionFactory):
        @property
        def federated(self):
            return "federation" in self.extra_config

        def _create_basic_session(self, session_kwargs: Dict[str, Any]) -> boto3.session.Session:
            if self.federated:
                return self._create_federated_session(session_kwargs)
            else:
                return super()._create_basic_session(session_kwargs)

        def _create_federated_session(self, session_kwargs: Dict[str, Any]) -> boto3.session.Session:
            username = self.extra_config["federation"]["username"]
            region_name = self._get_region_name()
            self.log.debug(
                f"Creating federated session with username={username} region_name={region_name} for "
                f"connection {self.conn.conn_id}"
            )
            credentials = RefreshableCredentials.create_from_metadata(
                metadata=self._refresh_federated_credentials(),
                refresh_using=self._refresh_federated_credentials,
                method="custom-federation",
            )
            session = botocore.session.get_session()
            session._credentials = credentials
            session.set_config_variable("region", region_name)
            return boto3.session.Session(botocore_session=session, **session_kwargs)

        def _refresh_federated_credentials(self) -> Dict[str, str]:
            self.log.debug("Refreshing federated AWS credentials")
            credentials = get_federated_aws_credentials(**self.extra_config["federation"])
            access_key_id = credentials["AccessKeyId"]
            expiry_time = credentials["Expiration"]
            self.log.info(
                f"New federated AWS credentials received with aws_access_key_id={access_key_id} and "
                f"expiry_time={expiry_time} for connection {self.conn.conn_id}"
            )
            return {
                "access_key": access_key_id,
                "secret_key": credentials["SecretAccessKey"],
                "token": credentials["SessionToken"],
                "expiry_time": expiry_time,
            }


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

Using IAM Roles for Service Accounts (IRSA) on EKS
----------------------------------------------------------------

If you are running Airflow on Amazon EKS, you can grant AWS related permission (such as S3 Read/Write for remote logging) to the Airflow service by granting the IAM role to it's service account.  To activate this, the following steps must be followed:

1. Create an IAM OIDC Provider on EKS cluster.
2. Create an IAM Role and Policy to attach to the Airflow service account with web identity provider created at 1.
3. Add the corresponding IAM Role to the Airflow service account as an annotation.

.. seealso::
    https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html

Then you can find ``AWS_ROLE_ARN`` and ``AWS_WEB_IDENTITY_TOKEN_FILE`` in environment variables of appropriate pods that `Amazon EKS Pod Identity Web Hook <https://github.com/aws/amazon-eks-pod-identity-webhook>`__ added. Then `boto3 <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials>`__ will configure credentials using those variables.

In order to use IRSA in Airflow, you have to create an aws connection with all fields empty. If a field such as ``role-arn`` is set, Airflow does not follow the boto3 default flow because it manually create a session using connection fields. If you did not change the default connection ID, an empty AWS connection named ``aws_default`` would be enough.
