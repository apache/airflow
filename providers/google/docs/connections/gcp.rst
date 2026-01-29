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



.. _howto/connection:gcp:

Google Cloud Connection
================================

The Google Cloud connection type enables the Google Cloud Integrations.

Authenticating to Google Cloud
------------------------------

There are three ways to connect to Google Cloud using Airflow:

1. Using a `Application Default Credentials
   <https://google-auth.readthedocs.io/en/latest/reference/google.auth.html#google.auth.default>`_,
2. Using a `service account
   <https://cloud.google.com/docs/authentication/#service_accounts>`_ by specifying a key file in JSON format.
   Key can be specified as a path to the key file (``Keyfile Path``), as a key payload (``Keyfile JSON``)
   or as secret in Secret Manager (``Keyfile secret name``). Only one way of defining the key can be used at a time.
   If you need to manage multiple keys then you should configure multiple connections.
3. Using a `credential configuration file <https://googleapis.dev/python/google-auth/2.9.0/user-guide.html#external-credentials-workload-identity-federation>`_,
   by specifying the path to or the content of a valid credential configuration file.
   A credential configuration file is a configuration file that typically contains non-sensitive metadata to instruct
   the ``google-auth`` library on how to retrieve external subject tokens and exchange them for service account access
   tokens.

   .. warning:: Additional permissions might be needed

   Connection which uses key from the Secret Manager requires that `Application Default Credentials
   <https://google-auth.readthedocs.io/en/latest/reference/google.auth.html#google.auth.default>`_ (ADC)
   have permission to access payloads of secrets.

   .. note:: Alternative way of storing connections

   Besides storing only key in Secret Manager there is an option for storing entire connection.
   For more details take a look at :ref:`Google Secret Manager Backend <google_cloud_secret_manager_backend>`.

Default Connection IDs
----------------------

All hooks and operators related to Google Cloud use ``google_cloud_default`` by default.


Note On Application Default Credentials
---------------------------------------
Application Default Credentials are inferred by the GCE metadata server when running
Airflow on Google Compute Engine or the GKE metadata server
when running on GKE which allows mapping Kubernetes Service Accounts to GCP service accounts
`Workload Identity
<https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity>`_.
This can be useful when managing minimum permissions for multiple Airflow instances on a single GKE cluster which
each have a different IAM footprint. Simply assign KSAs for your worker / webserver deployments and workload identity
will map them to separate GCP Service Accounts (rather than sharing a cluster-level GCE service account).
From a security perspective it has the benefit of not storing Google Service Account
keys  on disk nor in the Airflow database, making it impossible
to leak the sensitive long lived credential key material.

From an Airflow perspective Application Default Credentials can be used for
a connection by specifying an empty URI.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://'

.. _howto/connection:gcp:configuring_the_connection:

Configuring the Connection
--------------------------

Project Id (optional)
    The Google Cloud project ID to connect to. It is used as default project id by operators using it and
    can usually be overridden at the operator level.

Keyfile Path
    Path to a `service account
    <https://cloud.google.com/docs/authentication/#service_accounts>`_ key
    file (JSON format) on disk.

    Not required if using application default credentials.

Keyfile JSON
    Contents of a `service account
    <https://cloud.google.com/docs/authentication/#service_accounts>`_ key
    file (JSON format) on disk.

    Not required if using application default credentials.

Credential Configuration File
    Credential configuration file JSON or path to a credential configuration file on the filesystem.

    Not required if using application default credentials.

Secret name which holds Keyfile JSON
    Name of the secret in Secret Manager which contains a `service account
    <https://cloud.google.com/docs/authentication/#service_accounts>`_ key.

    Not required if using application default credentials.

Scopes (comma separated)
    A list of comma-separated `Google Cloud scopes
    <https://developers.google.com/identity/protocols/googlescopes>`_ to
    authenticate with.


Quota Project ID (optional)
---------------------------

The Google Cloud project ID to use for API quota and billing purposes. This is useful
when using a shared service account but want to attribute quota/billing to a different
project. If not specified, the default project from the connection is used. Must be a
valid GCP project ID (lowercase letters, digits, hyphens, 6–30 characters, starting
with a letter).

.. note::

    If using anonymous credentials, quota project logic is ignored.

.. warning::

    Ensure the service account has permission to use the specified quota project.
    Invalid or unauthorized quota project IDs will result in an error.

Number of Retries
    Integer, number of times to retry with randomized
    exponential backoff. If all retries fail, the :class:`googleapiclient.errors.HttpError`
    represents the last request. If zero (default), we attempt the
    request only once.

Impersonation Chain
    Optional service account to impersonate using short-term
    credentials, or chained list of accounts required to get the access_token
    of the last account in the list, which will be impersonated in all requests leveraging this connection.
    If set as a string, the account must grant the originating account
    the Service Account Token Creator IAM role.
    If set as a comma-separated list, the identities from the list must grant
    Service Account Token Creator IAM role to the directly preceding identity, with first
    account from the list granting this role to the originating account.

    When specifying the connection in environment variable you should specify
    it using URI syntax, with the following requirements:

      * scheme part should be equals ``google-cloud-platform`` (Note: look for a
        hyphen character)
      * authority (username, password, host, port), path is ignored
      * query parameters contains information specific to this type of
        connection. The following keys are accepted:

        * ``project`` - Project Id
        * ``key_path`` - Keyfile Path
        * ``keyfile_dict`` - Keyfile JSON
        * ``key_secret_name`` - Secret name which holds Keyfile JSON
        * ``key_secret_project_id`` - Project Id which holds Keyfile JSON
        * ``scope`` - Scopes
        * ``num_retries`` - Number of Retries


    Note that all components of the URI should be URL-encoded.

    For example, with URI format:

    .. code-block:: bash

       export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://?key_path=%2Fkeys%2Fkey.json&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&project=airflow&num_retries=5'

    And using JSON format:

    .. code-block:: bash

       export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='{"conn_type": "google_cloud_platform", "extra": {"key_path": "/keys/key.json", "scope": "https://www.googleapis.com/auth/cloud-platform", "project": "airflow", "num_retries": 5}}'

.. _howto/connection:gcp:impersonation:

Direct impersonation of a service account
-----------------------------------------

Google operators support `direct impersonation of a service account
<https://cloud.google.com/iam/docs/understanding-service-accounts#directly_impersonating_a_service_account>`_
via ``impersonation_chain`` argument (``google_impersonation_chain`` in case of operators
that also communicate with services of other cloud providers).
The impersonation chain can also be configured directly on the Google Cloud Connection
as described above, though the ``impersonation_chain`` passed to the operator takes precedence.

For example:

.. code-block:: python

        import os

        from airflow.providers.google.cloud.operators.bigquery import (
            BigQueryCreateEmptyDatasetOperator,
        )

        IMPERSONATION_CHAIN = "impersonated_account@your_project_id.iam.gserviceaccount.com"

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create-dataset",
            gcp_conn_id="google_cloud_default",
            dataset_id="test_dataset",
            location="southamerica-east1",
            impersonation_chain=IMPERSONATION_CHAIN,
        )

In order for this example to work, the account ``impersonated_account`` must grant the
``Service Account Token Creator`` IAM role to the service account specified in the
``google_cloud_default`` Connection. This will allow to generate ``impersonated_account``'s
access token, which will allow to act on its behalf using its permissions. ``impersonated_account``
does not even need to have a generated key.

In case of operators that connect to multiple Google services, all hooks use the same value of
``impersonation_chain`` (if applicable). You can also impersonate accounts from projects
other than the project of the originating account. In that case, the project id of the impersonated
account will be used as the default project id in operator's logic, unless you have explicitly
specified the Project Id in Connection's configuration or in operator's arguments.

Impersonation can also be used in chain: if the service account specified in Connection has
``Service Account Token Creator`` role granted on account A, and account A has this role on account
B, then we are able to impersonate account B.

For example, with the following ``terraform`` setup...

.. code-block:: terraform

        terraform {
          required_version = "> 0.11.14"
        }
        provider "google" {
        }
        variable "project_id" {
          type = "string"
        }
        resource "google_service_account" "sa_1" {
          account_id   = "impersonation-chain-1"
          project = "${var.project_id}"
        }
        resource "google_service_account" "sa_2" {
          account_id   = "impersonation-chain-2"
          project = "${var.project_id}"
        }
        resource "google_service_account" "sa_3" {
          account_id   = "impersonation-chain-3"
          project = "${var.project_id}"
        }
        resource "google_service_account" "sa_4" {
          account_id   = "impersonation-chain-4"
          project = "${var.project_id}"
        }
        resource "google_service_account_iam_member" "sa_4_member" {
          service_account_id = "${google_service_account.sa_4.name}"
          role               = "roles/iam.serviceAccountTokenCreator"
          member             = "serviceAccount:${google_service_account.sa_3.email}"
        }
        resource "google_service_account_iam_member" "sa_3_member" {
          service_account_id = "${google_service_account.sa_3.name}"
          role               = "roles/iam.serviceAccountTokenCreator"
          member             = "serviceAccount:${google_service_account.sa_2.email}"
        }
        resource "google_service_account_iam_member" "sa_2_member" {
          service_account_id = "${google_service_account.sa_2.name}"
          role               = "roles/iam.serviceAccountTokenCreator"
          member             = "serviceAccount:${google_service_account.sa_1.email}"
        }

...we should configure Airflow Connection to use ``impersonation-chain-1`` account's key and provide
following value for ``impersonation_chain`` argument...

.. code-block:: python

        PROJECT_ID = os.environ.get("TF_VAR_project_id", "your_project_id")
        IMPERSONATION_CHAIN = [
            f"impersonation-chain-2@{PROJECT_ID}.iam.gserviceaccount.com",
            f"impersonation-chain-3@{PROJECT_ID}.iam.gserviceaccount.com",
            f"impersonation-chain-4@{PROJECT_ID}.iam.gserviceaccount.com",
        ]

...then requests will be executed using ``impersonation-chain-4`` account's privileges.


Domain-wide delegation
-----------------------------------------
Some Google operators, hooks and sensors support `domain-wide delegation <https://developers.google.com/cloud-search/docs/guides/delegation>`_, in addition to direct impersonation of a service account.
Delegation allows a user or service account to grant another service account the ability to act on their behalf.
This means that the user or service account that is delegating their permissions can continue to access and manage their own resources, while the delegated service account can also access and manage those resources.

For example:

.. code-block:: python

        PROJECT_ID = os.environ.get("TF_VAR_project_id", "your_project_id")

        SPREADSHEET = {
            "properties": {"title": "Test1"},
            "sheets": [{"properties": {"title": "Sheet1"}}],
        }

        from airflow.providers.google.suite.operators.sheets import (
            GoogleSheetsCreateSpreadsheetOperator,
        )

        create_spreadsheet_operator = GoogleSheetsCreateSpreadsheetOperator(
            task_id="create-spreadsheet",
            gcp_conn_id="google_cloud_default",
            spreadsheet=SPREADSHEET,
            impersonation_chain=f"projects/-/serviceAccounts/SA@{PROJECT_ID}.iam.gserviceaccount.com",
        )

Note that as domain-wide delegation is currently supported by most of the Google operators and hooks, its usage should be limited only to Google Workspace (gsuite) and marketing platform operators and hooks or by accessing these services through the GoogleDiscoveryAPI hook. It is deprecated in the following usages:

* All of Google Cloud operators and hooks.
* Firebase hooks.
* All transfer operators that involve Google cloud in different providers, for example: :class:`airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSToS3Operator`.


Quota Project Support
---------------------

Airflow's Google Cloud providers support specifying a "quota project" (a billing project) for
API calls. That lets API usage be billed to a different Google Cloud project than the one that
owns the service account. This is useful for organizations that share service accounts but
centralize billing in specific projects.

Usage
~~~~~

There are two ways to set a quota project in Airflow:

- Via connection extras (recommended for environment-wide defaults).
- Directly on operators or hooks (recommended when a single task must bill to a different project).

Connection extras
^^^^^^^^^^^^^^^^^

Add the quota project ID to the Google Cloud connection extras. For example:

.. code-block:: json

  {
    "quota_project_id": "your-billing-project-id"
  }

You can set this via the Airflow UI, the Connections REST API, or an environment variable, for
example:

.. code-block:: bash

  export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='{
    "conn_type": "google-cloud-platform",
    "extra": {
      "quota_project_id": "your-billing-project-id"
    }
  }'

Operator / Hook parameter
^^^^^^^^^^^^^^^^^^^^^^^^^

You can also pass the quota project directly when creating an operator or hook. This takes
precedence over the connection extras:

.. code-block:: python

  from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

  task = BigQueryExecuteQueryOperator(
    task_id='execute_query',
    sql='SELECT * FROM `my_project.dataset.table`',
    quota_project_id='your-billing-project-id'
  )

  # Or when creating a hook
  from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

  hook = BigQueryHook(quota_project_id='your-billing-project-id')

Priority
~~~~~~~

If a quota project is provided both in the connection extras and as an operator/hook
parameter, the operator/hook parameter wins.

Compatibility
~~~~~~~~~~~~~

This setting works with Google Cloud services that support the quota project mechanism (the
`x-goog-user-project` header), for example:

- BigQuery
- Cloud Storage
- Dataflow
- Other Google Cloud APIs that accept quota project headers

Impact
~~~~~~

Using a quota project affects where API usage is billed, which quotas are applied, and how
usage is reported for monitoring and auditing.

Examples
~~~~~~~~

**Examples**

See the example DAG: ``airflow/providers/google/cloud/example_dags/example_quota_project.py``

