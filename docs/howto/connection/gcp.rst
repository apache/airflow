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

Google Cloud Platform Connection
================================

The Google Cloud Platform connection type enables the :ref:`GCP Integrations
<GCP>`.

Authenticating to GCP
---------------------

There are two ways to connect to GCP using Airflow.

1. Use `Application Default Credentials
   <https://google-auth.readthedocs.io/en/latest/reference/google.auth.html#google.auth.default>`_,
   such as via the metadata server when running on Google Compute Engine.
2. Use a `service account
   <https://cloud.google.com/docs/authentication/#service_accounts>`_ key
   file (JSON format) on disk.

Default Connection IDs
----------------------

All hooks and operators related to Google Cloud Platform use ``google_cloud_default`` by default.

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
    file (JSON format) on disk. It is recommended to :doc:`Secure your connections <../secure-connections>` if using this method to authenticate.

    Not required if using application default credentials.

Scopes (comma separated)
    A list of comma-separated `Google Cloud scopes
    <https://developers.google.com/identity/protocols/googlescopes>`_ to
    authenticate with.

Number of Retries
    Integer, number of times to retry with randomized
    exponential backoff. If all retries fail, the :class:`googleapiclient.errors.HttpError`
    represents the last request. If zero (default), we attempt the
    request only once.

    When specifying the connection in environment variable you should specify
    it using URI syntax, with the following requirements:

      * scheme part should be equals ``google-cloud-platform`` (Note: look for a
        hyphen character)
      * authority (username, password, host, port), path is ignored
      * query parameters contains information specific to this type of
        connection. The following keys are accepted:

        * ``extra__google_cloud_platform__project`` - Project Id
        * ``extra__google_cloud_platform__key_path`` - Keyfile Path
        * ``extra__google_cloud_platform__key_dict`` - Keyfile JSON
        * ``extra__google_cloud_platform__scope`` - Scopes
        * ``extra__google_cloud_platform__num_retries`` - Number of Retries

    Note that all components of the URI should be URL-encoded.

    For example:

    .. code-block:: bash

       export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://?extra__google_cloud_platform__key_path=%2Fkeys%2Fkey.json&extra__google_cloud_platform__scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&extra__google_cloud_platform__project=airflow&extra__google_cloud_platform__num_retries=5'
