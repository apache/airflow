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
import re

log = logging.getLogger(__name__)

try:
    from airflow.sdk import task
except ImportError:
    from airflow.decorators import task  # type: ignore[attr-defined, no-redef]


def _foundation_model_id(model_arn: str) -> str:
    """Return the model ID part of a foundation model ARN, which is identical in every region."""
    return model_arn.rpartition("/")[2]


def _release_date(inference_profile_id: str) -> str:
    """
    Return the ``YYYYMMDD`` release date model providers embed in their version, e.g.
    ``global.anthropic.claude-sonnet-4-5-20250929-v1:0``. An ID without one sorts last, so an
    unrecognised naming scheme is treated as the newest rather than silently preferred.
    """
    match = re.search(r"\d{8}", inference_profile_id)
    return match.group() if match else "99999999"


@task
def get_text_inference_profile_arn() -> str:
    """
    Select a valid Bedrock inference profile ARN for system tests.

    Note that for Anthropic models, first-time users may need to
    submit use case details before they can access the model.
    """
    from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook

    client = BedrockHook().conn

    # Bedrock only accepts a model whose lifecycle status is ACTIVE, so requiring that status keeps this
    # working if a provider ever reports something other than today's ACTIVE/LEGACY pair. The inference
    # profile summaries do not carry the lifecycle status, only the foundation models a profile resolves to.
    active_model_ids = {
        model["modelId"]
        for model in client.list_foundation_models()["modelSummaries"]
        if model.get("modelLifecycle", {}).get("status") == "ACTIVE"
    }

    profiles = client.list_inference_profiles(typeEquals="SYSTEM_DEFINED")["inferenceProfileSummaries"]
    # Oldest release first, so a run picks a mature model rather than a fresh one that batch inference or
    # RAG may not support yet, and picks the same one on every run.
    candidates = sorted(
        (
            profile
            for profile in profiles
            if profile.get("status") == "ACTIVE"
            and profile["inferenceProfileId"].startswith("global.anthropic.")
            and all(
                _foundation_model_id(model["modelArn"]) in active_model_ids for model in profile["models"]
            )
        ),
        key=lambda profile: (_release_date(profile["inferenceProfileId"]), profile["inferenceProfileId"]),
    )
    profile_ids = [profile["inferenceProfileId"] for profile in candidates]
    log.info("Valid text inference profiles, oldest first: %s", profile_ids)

    for profile in candidates:
        # Haiku has some version dependency issues: RAG only supports 3.5 but batch only supports 4.5
        if "sonnet" in profile["inferenceProfileId"]:
            log.info("Selected inference profile ARN: %s", profile["inferenceProfileArn"])
            return profile["inferenceProfileArn"]
    raise RuntimeError(f"No valid inference profiles found. Active candidates were: {profile_ids}")
