#
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

import os

DEFAULT_GEMINI_MODEL = "gemini-3.7-flash"
SFT_ENABLED_GEMINI_MODEL = "gemini-3.5-flash"
TEXT_EMBEDDING_GEMINI_MODEL = "gemini-embedding-2"


def _get_model_env_variable(name: str, default: str) -> str:
    value = os.environ.get(name, default)
    if not value:
        raise ValueError(f"Environment variable {name} must not be empty.")
    return value


# it should be a general purpose model, multimodal & cached
def get_default_gemini_model() -> str:
    return _get_model_env_variable("DEFAULT_GEMINI_MODEL", DEFAULT_GEMINI_MODEL)


# sft enabled model
def get_sft_enabled_gemini_model() -> str:
    return _get_model_env_variable("SFT_ENABLED_GEMINI_MODEL", SFT_ENABLED_GEMINI_MODEL)


# embedding model with text support
def get_text_embedding_gemini_model() -> str:
    return _get_model_env_variable("TEXT_EMBEDDING_GEMINI_MODEL", TEXT_EMBEDDING_GEMINI_MODEL)
