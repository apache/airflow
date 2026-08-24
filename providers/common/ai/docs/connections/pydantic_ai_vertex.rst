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

.. _howto/connection:pydanticai-vertex:

Pydantic AI (Google Vertex AI) Connection
============================================

The ``pydanticai-vertex`` connection type configures access to
`Google Vertex AI <https://cloud.google.com/vertex-ai>`__ via the pydantic-ai
framework. It backs ``PydanticAIVertexHook``, the dedicated subclass of
``PydanticAIHook`` for Google Cloud's project/location/service-account
credential shape — none of which fit the plain ``api_key`` + ``base_url``
shape that the generic :doc:`pydantic_ai` connection assumes. All fields live
in ``extra``; the ``password`` and ``host`` fields are hidden in the connection
form.

Default Connection IDs
----------------------

The ``PydanticAIVertexHook`` uses ``pydanticai_vertex_default`` by default.

Configuring the Connection
--------------------------

All fields below are ``extra`` (JSON) fields.

Model
    Google model identifier (e.g. ``google-cloud:gemini-2.0-flash``). The
    ``google-cloud:`` prefix is required — it is what makes pydantic-ai
    instantiate the ``GoogleCloudProvider``, which is what accepts this
    hook's ``project`` / ``location`` / ``service_account_info`` fields (see
    "Credentials" below).

GCP Project
    Google Cloud project ID. Falls back to the ``GOOGLE_CLOUD_PROJECT``
    environment variable.

Location / Region
    Vertex AI region (e.g. ``us-central1``). Falls back to the
    ``GOOGLE_CLOUD_LOCATION`` environment variable.

Force Vertex AI Mode
    Legacy flag from pydantic-ai 1.x, where a single ``GoogleProvider`` took a
    ``vertexai`` argument. Not needed here: the ``google-cloud:`` model prefix
    above already makes ``GoogleCloudProvider`` hard-code ``vertexai=True``
    unconditionally when it builds its client.

    .. note::
        This field is accepted for backward compatibility but has no effect:
        it is never forwarded to the provider, and every other field on the
        connection (project, location, service account, API key) is passed
        through normally. Setting it logs a warning in the task log noting
        that the field is ignored and that Vertex AI vs. Generative Language
        API mode is selected via the model prefix (``google-cloud:`` vs.
        ``google:``) instead.

API Key
    Google API key for Vertex AI Express Mode. Falls back to the
    ``GOOGLE_API_KEY`` environment variable. Cannot be combined with
    ``project`` / ``location`` / ``service_account_info`` (those select the
    credentials/ADC path instead, which takes precedence and nulls the API
    key). For the Generative Language API
    (non-Vertex, API-key-only), use the ``google:`` prefix on the generic
    :doc:`pydantic_ai` connection instead.

Service Account Info
    Service account key as an inline JSON object (with ``type``,
    ``project_id``, ``private_key``, etc.) — not a file path.

Custom Endpoint URL
    Override the Google API base URL (optional).

Credentials
-----------

The hook passes every field you set on to ``GoogleCloudProvider`` together;
when more than one credential source is set at once, ``credentials`` /
``project`` / ``location`` take precedence over ``api_key`` (which is then
ignored):

- ``service_account_info`` — loaded into Google Cloud credentials and passed
  as ``credentials`` to the provider.
- Application Default Credentials (``GOOGLE_APPLICATION_CREDENTIALS``,
  ``gcloud auth application-default login``, Workload Identity, …) — used
  automatically once ``project`` and/or ``location`` are set without
  ``service_account_info``.
- ``api_key`` — for Vertex AI Express Mode, only used when none of the above
  are set.

Fallback Connections
    Other connection IDs to fail over to, in order, while this provider is
    unavailable. Stored in ``extra["fallback_conn_ids"]``. Entries may name any
    ``pydanticai`` connection type, so one chain can span vendors. See
    :doc:`/provider_fallback`.

Examples
--------

**Application Default Credentials (recommended)**

Leave the credential fields empty and configure
``GOOGLE_APPLICATION_CREDENTIALS`` (or another ADC source) in the worker
environment:

.. code-block:: json

    {
        "conn_type": "pydanticai-vertex",
        "extra": "{\"model\": \"google-cloud:gemini-2.0-flash\", \"project\": \"my-gcp-project\", \"location\": \"us-central1\"}"
    }

**Inline service account**

.. code-block:: json

    {
        "conn_type": "pydanticai-vertex",
        "extra": "{\"model\": \"google-cloud:gemini-2.0-flash\", \"project\": \"my-gcp-project\", \"location\": \"us-central1\", \"service_account_info\": {\"type\": \"service_account\", \"project_id\": \"my-gcp-project\", \"private_key\": \"<contents of the service account JSON key's private_key field>\", \"client_email\": \"sa@my-gcp-project.iam.gserviceaccount.com\"}}"
    }
