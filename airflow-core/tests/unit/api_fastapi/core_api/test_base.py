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

import pytest
from pydantic import Field, ValidationError, field_validator

from airflow.api_fastapi.core_api.base import StrictBaseModel, make_partial_model


class SampleModel(StrictBaseModel):
    """A sample model with required and optional fields for testing."""

    name: str = Field(max_length=50)
    age: int
    email: str | None = Field(default=None)

    @field_validator("name")
    @classmethod
    def name_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("name must not be empty")
        return v


SampleModelPartial = make_partial_model(SampleModel)


class ModelWithFieldAttributes(StrictBaseModel):
    """Model with alias, title, and description to test full attribute preservation."""

    name: str = Field(alias="user_name", title="User Name", description="The user's full name")
    age: int = Field(ge=0, le=150, title="Age", description="Age in years")


ModelWithFieldAttributesPartial = make_partial_model(ModelWithFieldAttributes)


class TestMakePartialModel:
    def test_all_fields_become_optional(self):
        instance = SampleModelPartial()
        assert instance.name is None
        assert instance.age is None
        assert instance.email is None

    def test_partial_model_accepts_subset_of_fields(self):
        instance = SampleModelPartial(name="Alice")
        assert instance.name == "Alice"
        assert instance.age is None

    def test_full_model_still_requires_fields(self):
        with pytest.raises(ValidationError):
            SampleModel(email="test@example.com")

    def test_validators_are_preserved(self):
        with pytest.raises(ValidationError, match="name must not be empty"):
            SampleModelPartial(name="   ")

    def test_field_metadata_preserved(self):
        with pytest.raises(ValidationError):
            SampleModelPartial(name="x" * 51)

    def test_extra_forbid_preserved(self):
        with pytest.raises(ValidationError):
            SampleModelPartial(unknown_field="test")

    def test_already_optional_fields_stay_optional(self):
        instance = SampleModelPartial(email="test@example.com")
        assert instance.email == "test@example.com"
        assert instance.name is None

    def test_partial_model_name(self):
        assert SampleModelPartial.__name__ == "SampleModelPartial"

    def test_field_alias_preserved(self):
        instance = ModelWithFieldAttributesPartial(user_name="Alice")
        assert instance.name == "Alice"

    def test_field_title_and_description_preserved(self):
        name_field = ModelWithFieldAttributesPartial.model_fields["name"]
        assert name_field.alias == "user_name"
        assert name_field.title == "User Name"
        assert name_field.description == "The user's full name"

        age_field = ModelWithFieldAttributesPartial.model_fields["age"]
        assert age_field.title == "Age"
        assert age_field.description == "Age in years"

    def test_field_ge_le_constraints_preserved(self):
        with pytest.raises(ValidationError):
            ModelWithFieldAttributesPartial(age=-1)
        with pytest.raises(ValidationError):
            ModelWithFieldAttributesPartial(age=200)
