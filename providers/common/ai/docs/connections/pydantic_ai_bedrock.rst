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

.. _howto/connection:pydanticai-bedrock:

Pydantic AI (AWS Bedrock) Connection
=======================================

The ``pydanticai-bedrock`` connection type configures access to
`AWS Bedrock <https://aws.amazon.com/bedrock/>`__ via the pydantic-ai framework.
It backs ``PydanticAIBedrockHook``, the dedicated subclass of ``PydanticAIHook``
for Bedrock's AWS-style credentials — IAM keys, a bearer token, or the default
credential chain — none of which fit the plain ``api_key`` + ``base_url`` shape
that the generic :doc:`pydantic_ai` connection assumes. All fields live in
``extra``; the ``password`` and ``host`` fields are hidden in the connection form.

Default Connection IDs
----------------------

The ``PydanticAIBedrockHook`` uses ``pydanticai_bedrock_default`` by default.

Configuring the Connection
--------------------------

All fields below are ``extra`` (JSON) fields.

Model
    Bedrock model identifier (e.g. ``bedrock:us.anthropic.claude-opus-4-5``).

AWS Region
    AWS region (e.g. ``us-east-1``). Falls back to the ``AWS_DEFAULT_REGION``
    environment variable.

AWS Access Key ID
    IAM access key. Leave empty to use instance role / environment credential chain.

AWS Secret Access Key
    IAM secret key.

AWS Session Token
    Temporary session token (optional).

AWS Profile Name
    Named AWS credentials profile (optional).

Bearer Token
    AWS bearer token (alt. to IAM key/secret). Falls back to the
    ``AWS_BEARER_TOKEN_BEDROCK`` environment variable.

Custom Endpoint URL
    Override the Bedrock runtime endpoint URL (optional).

Read Timeout (s)
    boto3 read timeout in seconds (float, optional).

Connect Timeout (s)
    boto3 connect timeout in seconds (float, optional).

Credentials
-----------

The hook passes every field you set on to ``BedrockProvider`` together; when
more than one credential source is set at once, the bearer token
(``api_key``) takes precedence over IAM keys:

- A bearer token (``api_key``, mapped to ``AWS_BEARER_TOKEN_BEDROCK``) — used
  first if set.
- IAM keys (``aws_access_key_id`` + ``aws_secret_access_key``, optionally
  ``aws_session_token``) — used only when no bearer token is set.
- The environment-variable / instance-role credential chain
  (``AWS_PROFILE``, IAM role, …) when none of the fields above are set.

Examples
--------

**IAM instance role / environment credential chain (recommended)**

Leave the AWS credential fields empty and let boto3 resolve credentials from
the instance role or environment:

.. code-block:: json

    {
        "conn_type": "pydanticai-bedrock",
        "extra": "{\"model\": \"bedrock:us.anthropic.claude-opus-4-5\", \"region_name\": \"us-east-1\"}"
    }

**Explicit IAM keys**

.. code-block:: json

    {
        "conn_type": "pydanticai-bedrock",
        "extra": "{\"model\": \"bedrock:us.anthropic.claude-opus-4-5\", \"region_name\": \"us-east-1\", \"aws_access_key_id\": \"AKIA...\", \"aws_secret_access_key\": \"...\"}"
    }

**Bearer token**

.. code-block:: json

    {
        "conn_type": "pydanticai-bedrock",
        "extra": "{\"model\": \"bedrock:us.anthropic.claude-opus-4-5\", \"api_key\": \"<bearer-token>\"}"
    }
