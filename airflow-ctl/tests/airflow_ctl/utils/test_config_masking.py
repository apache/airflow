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


import pytest
from airflowctl.api.datamodels.generated import Config, ConfigSection, ConfigOption
from airflowctl.utils.config_masking import (
    is_sensitive_config_key,
    mask_config_value,
    mask_config,
)


class TestIsSensitiveConfigKey:
    """Test sensitivity detection for config keys."""

    def test_detects_sensitive_keys(self):
        assert is_sensitive_config_key("smtp_password")
        assert is_sensitive_config_key("sql_alchemy_conn")
        assert is_sensitive_config_key("fernet_key")

    def test_ignores_non_sensitive_keys(self):
        assert not is_sensitive_config_key("parallelism")
        assert not is_sensitive_config_key("dagbag_import_timeout")


class TestMaskConfigValue:
    """Test value masking."""

    def test_masks_sensitive_values(self):
        assert mask_config_value("smtp_password", "secret123") == "***"
        assert mask_config_value("fernet_key", "my_secret_key") == "***"

    def test_preserves_non_sensitive_values(self):
        assert mask_config_value("parallelism", "32") == "32"
        assert mask_config_value("dagbag_import_timeout", "30") == "30"


class TestMaskConfig:
    """Test config object masking (entry point for API operations)."""

    def test_masks_pydantic_config_object(self):
        """Pydantic Config objects should be masked."""
        config = Config(
            sections=[
                ConfigSection(
                    name="core",
                    options=[
                        ConfigOption(key="parallelism", value="32"),
                        ConfigOption(key="sql_alchemy_conn", value="postgresql://user:pass@db"),
                    ]
                )
            ]
        )
        result = mask_config(config)

        assert isinstance(result, Config)
        assert hasattr(result, "sections")

        core_section = result.sections[0]
        opts = {opt.key: opt.value for opt in core_section.options}

        assert opts["parallelism"] == "32"
        assert opts["sql_alchemy_conn"] == "***"

