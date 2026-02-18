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



.. _howto/connection:pydantic-ai:

PydanticAI Connection
=====================

The PydanticAI connection type provides connection to a various LLM agent providers. refer to `<https://ai.pydantic.dev/models/overview/>`_. for all supported providers.

Configuring the connection
~~~~~~~~~~~~~~~~~~~~~~~~~~

Password (required)
    The API token to use when authenticating to the LLM agent provider.

    If using the Connection form in the Airflow UI, the token can also be stored in the "API Token" field.

Extra (optional)
    Specify extra parameters as JSON dictionary. As of now, only ``provider_module`` and ``model_settings`` is supported when wanting to connect LLM Agent providers via PydanticAI.

    ``provider_module`` should be a string eg: ``anthropic:claude-sonnet-4-6``
    ``model_settings`` should be a dictionary of settings to be passed to the model when initializing the provider. refer `<https://ai.pydantic.dev/models>`_.

    Provide the ``provider_module`` either in the extras field or pass it in the operator args.

    .. code-block:: json

        {
            "provider_module": "anthropic:claude-sonnet-4-6",
            "model_settings": {
                "temperature": 0.7,
                "max_tokens": 2048
            }
        }
