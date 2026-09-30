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
from dataclasses import InitVar, dataclass, field

import pytest
from pydantic import BaseModel
from pydantic_ai.messages import BinaryContent, TextContent

from airflow.providers.common.ai.utils.masking import dumps_masked, mask_secrets


@pytest.mark.enable_redact
class TestMaskSecrets:
    def test_masks_inside_every_container_and_keeps_the_shape(self, registered_secret):
        value = {
            "rows": [(1, registered_secret)],
            "tags": {registered_secret},
            "note": f"key={registered_secret}",
        }

        assert mask_secrets(value) == {"rows": [(1, "***")], "tags": {"***"}, "note": "key=***"}

    @pytest.mark.parametrize("value", [42, 3.5, True, None, b"raw bytes"])
    def test_leaves_non_text_values_alone(self, registered_secret, value):
        assert mask_secrets(value) == value

    def test_keeps_values_under_keys_that_look_sensitive(self, registered_secret):
        value = {"password_policy": "rotate every 90 days", "api_key_count": 3}

        assert mask_secrets(value) == value

    def test_masks_a_secret_used_as_a_key(self, registered_secret):
        assert mask_secrets({registered_secret: 1}) == {"***": 1}

    def test_turns_a_pydantic_model_into_masked_data(self, registered_secret):
        class Setting(BaseModel):
            value: str

        assert mask_secrets([Setting(value=registered_secret)]) == [{"value": "***"}]

    def test_leaves_other_objects_such_as_images_alone(self, registered_secret):
        image = BinaryContent(data=b"\x89PNG", media_type="image/png")

        assert mask_secrets({"screenshot": image})["screenshot"] is image

    def test_masks_a_dataclass_and_keeps_its_type(self, registered_secret):
        @dataclass(frozen=True)
        class Credentials:
            user: str
            password: str

        assert mask_secrets(Credentials("svc", registered_secret)) == Credentials("svc", "***")
        assert mask_secrets(TextContent(content=f"key={registered_secret}")) == TextContent(content="key=***")

    def test_masks_a_dataclass_it_could_not_construct_again(self, registered_secret):
        """An InitVar has no value to pass back in, and an init=False field is not an argument."""

        @dataclass
        class Report:
            rows: list
            source: InitVar[str]
            label: str = field(init=False, default="")

            def __post_init__(self, source: str) -> None:
                self.label = f"from {source}"

        report = Report([registered_secret], source=registered_secret)

        masked = mask_secrets(report)

        assert type(masked) is Report
        assert masked.rows == ["***"]
        assert masked.label == "from ***"
        assert report.rows == [registered_secret]

    def test_masks_a_slotted_dataclass(self, registered_secret):
        @dataclass(slots=True)
        class Row:
            value: str

        assert mask_secrets(Row(registered_secret)) == Row("***")

    def test_masks_bytes_as_text(self, registered_secret):
        assert mask_secrets(f"key={registered_secret}".encode()) == b"key=***"

    def test_a_container_that_contains_itself_is_masked_without_recursing(self, registered_secret):
        looped: list = [registered_secret]
        looped.append(looped)

        assert mask_secrets(looped) == ["***", "<circular reference>"]


# Each changes representation inside a JSON string, so masking the dumped text would miss it.
SECRETS_JSON_ESCAPES = pytest.mark.parametrize(
    "secret",
    ['db-pa"ss-91c3', "db-pa\\ss-91c3", "db-p\u00e4ss-91c3"],
    ids=["quote", "backslash", "non-ascii"],
)


@pytest.mark.enable_redact
class TestDumpsMasked:
    @SECRETS_JSON_ESCAPES
    def test_masks_a_secret_that_json_escapes(self, register_secret, secret):
        register_secret(secret)

        dumped = dumps_masked({"password": secret, "rows": [[1, f"dsn={secret}"]]})

        assert json.loads(dumped) == {"password": "***", "rows": [[1, "dsn=***"]]}

    def test_masks_an_object_json_can_only_render_as_text(self, registered_secret):
        class Credentials:
            def __str__(self) -> str:
                return f"login with {registered_secret}"

        assert json.loads(dumps_masked({"auth": Credentials()})) == {"auth": "login with ***"}

    @SECRETS_JSON_ESCAPES
    def test_masks_a_dataclass_or_bytes_holding_a_secret_that_escapes(self, register_secret, secret):
        register_secret(secret)

        @dataclass
        class Row:
            dsn: str

        dumped = dumps_masked({"row": Row(f"pg://svc:{secret}@db"), "blob": f"key={secret}".encode()})

        assert json.loads(dumped) == {"row": {"dsn": "pg://svc:***@db"}, "blob": "key=***"}
