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

.. _howto/connection:adk:

Google ADK Connection
=====================

The `Google ADK <https://google.github.io/adk-docs/>`__ connection type
configures access to Gemini models via Google's Agent Development Kit.

Prerequisites
-------------

Install the ``google-adk`` extra:

.. code-block:: bash

    pip install 'apache-airflow-providers-common-ai[adk]'

Default Connection IDs
----------------------

The ``AdkHook`` uses ``adk_default`` by default.

Configuring the Connection
--------------------------

Model
    The model identifier for the Gemini model to use.  This field appears as
    a dedicated input in the connection form (via ``conn-fields``) and stores
    its value in ``extra["model"]``.

    Examples: ``gemini-2.5-flash``, ``gemini-2.5-pro``, ``gemini-2.0-flash``

    The model can also be overridden at the hook/operator level via the
    ``model_id`` parameter.

API Key (Password field)
    The Google API key for Gemini API access.  When provided, the hook sets
    the ``GOOGLE_API_KEY`` environment variable so the ``google-genai`` SDK
    picks it up automatically.

    Leave empty when using Application Default Credentials (ADC):

    - ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable
    - ``gcloud auth application-default login``
    - Workload Identity on GKE

Host (optional)
    Custom endpoint URL.  Only needed for proxy or custom endpoints.

Extra (JSON, optional)
    A JSON object with additional configuration.  Programmatic users can set
    the model directly in extra:

    .. code-block:: json

        {"model": "gemini-2.5-flash"}

    When using the UI, the "Model" field above writes to this same location
    automatically.

Examples
--------

**Gemini with API Key**

.. code-block:: json

    {
        "conn_type": "adk",
        "password": "AIza...",
        "extra": "{\"model\": \"gemini-2.5-flash\"}"
    }

**Gemini with Application Default Credentials**

Leave password empty and configure ADC in the environment:

.. code-block:: json

    {
        "conn_type": "adk",
        "extra": "{\"model\": \"gemini-2.5-pro\"}"
    }

Model Resolution Order
----------------------

The hook reads the model from these sources in priority order:

1. ``model_id`` parameter on the hook/operator
2. ``model`` in the connection's extra JSON (set by the "Model" conn-field in the UI)
