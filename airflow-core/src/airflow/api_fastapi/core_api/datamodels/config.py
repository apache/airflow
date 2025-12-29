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

import json

from pydantic import model_validator
from typing_extensions import Self

from airflow._shared.secrets_masker import redact
from airflow.api_fastapi.core_api.base import StrictBaseModel


class ConfigOption(StrictBaseModel):
    """Config option."""

    key: str
    value: str | tuple[str, str]

    @model_validator(mode="after")
    def redact_value(self) -> Self:
        if self.value is None:
            return self
        if isinstance(self.value, tuple):
            return self
        try:
            value_dict = json.loads(self.value)
            redacted_dict = redact(value_dict, max_depth=1)
            self.value = json.dumps(redacted_dict)
            return self
        except json.JSONDecodeError:
            # value is not a serialized string representation of a dict.
            self.value = str(redact(self.value, self.key))
            return self

    @property
    def text_format(self):
        if isinstance(self.value, tuple):
            return f"{self.key} = {self.value[0]} {self.value[1]}"
        return f"{self.key} = {self.value}"


class ConfigSection(StrictBaseModel):
    """Config Section Schema."""

    name: str
    options: list[ConfigOption]

    @property
    def text_format(self):
        """
        Convert the config section to text format.

        Example:
        ```
        [section_name]
        key1 = value1
        key2 = value2
        ```
        """
        return f"[{self.name}]\n" + "\n".join(option.text_format for option in self.options) + "\n"


class Config(StrictBaseModel):
    """List of config sections with their options."""

    sections: list[ConfigSection]

    @property
    def text_format(self):
        # convert all config sections to text
        return "\n".join(section.text_format for section in self.sections)
