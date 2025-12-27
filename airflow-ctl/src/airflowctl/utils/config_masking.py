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
"""Mask Sensitive information in Config."""
from __future__ import annotations

from typing import Any

SENSITIVE_CONFIG_OPTIONS = frozenset({
    # Database credentials
    "sql_alchemy_conn",
    "sql_alchemy_connect_args",

    # API credentials
    "api_key",
    "api_secret",
    "fernet_key",

    # Email credentials
    "smtp_password",

    # Secret keys
    "secret_key",
    "jwt_secret_key",

    # External service credentials
    "access_key",
    "access_key_id",
    "access_key_secret",
    "secret_access_key",

    # Generic sensitive patterns (suffixes)
    "_password",
    "_secret",
    "_key",
    "_token",
    "_credential",
})

replacement_value = "***"


def is_sensitive_config_key(key: str) -> bool:
    """
    Determine if a configuration key should be treated as sensitive.

    Return True if sensitive, else False
    """
    if not key:
        return False
    key_lower = key.strip().lower()

    if key_lower in SENSITIVE_CONFIG_OPTIONS:
        return True
    for suffix in ["_password", "_secret", "_key", "_token", "_credential"]:
        if key_lower.endswith(suffix):
            return True
    return False


def mask_config_value(key: str, value: Any) -> str:
    """Mask a configuration if the key is sensitive."""
    if not is_sensitive_config_key(key):
        return replacement_value

    # Handle Dictionary Values
    if isinstance(value, dict):
        return {k: mask_config_value(k, v) for k, v in value.items()}

    # List Values
    if isinstance(value, list):
        return [str(mask_config_value(str(i), i)) for i in value]

    return str(value) if value is not None else ""


def mask_config_section(section_name: str, options: list[dict]) -> list[dict]:
    """Mask all sensitive sections in configuration section."""
    return [
        {
            "key": opt["key"],
            "value": mask_config_value(opt["key"], opt.get("value"))
        }
        for opt in options
    ]


def mask_config(config: Any) -> Any:
    """Recursively Mask configuration values."""
    if isinstance(config, dict):
        return {k: mask_config_value(k, v) for k, v in config.items()}

    if hasattr(config, "sections"):
        masked_sections = []
        for section in config.sections:
            masked_options = [
                {"key": opt.key, "value": mask_config_value(opt.key, opt.value)}
                for opt in section.options
            ]
        masked_sections.append({
            "name": section.name,
            "options": masked_options
        })
        return {"sections": masked_sections}
    return config
