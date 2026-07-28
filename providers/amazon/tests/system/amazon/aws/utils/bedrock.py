# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging

log = logging.getLogger(__name__)

try:
    from airflow.sdk import task
except ImportError:
    from airflow.decorators import task  # type: ignore[attr-defined, no-redef]


@task
def get_text_inference_profile_arn() -> str:
    """
    Select a valid Bedrock inference profile ARN for system tests.

    Note that for Anthropic models, first-time users may need to
    submit use case details before they can access the model.
    """
    from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook

    client = BedrockHook().conn
    profiles = client.list_inference_profiles(typeEquals="SYSTEM_DEFINED")["inferenceProfileSummaries"]
    arns = [
        profile["inferenceProfileArn"]
        for profile in profiles
        if profile.get("status") == "ACTIVE" and profile["inferenceProfileId"].startswith("global.anthropic.")
    ]
    log.info("Valid text inference profile ARNs: %s", arns)

    for arn in arns:
        # Haiku has some version dependency issues: RAG only supports 3.5 but batch only supports 4.5
        if "sonnet" in arn:
            log.info("Selected inference profile ARN: %s", arn)
            return arn
    raise RuntimeError("No valid inference profiles found")
