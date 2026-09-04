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

import attr
import pytest

from airflow.providers.common.compat.lineage.entities import (
    Column,
    File,
    Table,
    Tag,
    User,
    default_if_none,
)


class TestFile:
    def test_accepts_positional_url_and_defaults(self):
        file = File("s3://bucket/key")

        assert file.url == "s3://bucket/key"
        assert file.type_hint is None

    def test_equality_is_value_based(self):
        assert File(url="s3://bucket/key") == File(url="s3://bucket/key")
        assert File(url="s3://bucket/key") != File(url="s3://bucket/other")


class TestKeywordOnlyEntities:
    @pytest.mark.parametrize(
        ("entity_class", "positional_args"),
        [
            (User, ("user@example.com",)),
            (Tag, ("pii",)),
            (Column, ("name", None, "VARCHAR")),
            (Table, ("db", "cluster", "name")),
        ],
    )
    def test_rejects_positional_arguments(self, entity_class, positional_args):
        with pytest.raises(TypeError):
            entity_class(*positional_args)

    @pytest.mark.parametrize(
        ("entity_class", "kwargs"),
        [
            (User, {}),
            (Column, {"name": "id"}),
            (Table, {"database": "db", "cluster": "c"}),
        ],
    )
    def test_rejects_missing_required_fields(self, entity_class, kwargs):
        with pytest.raises(TypeError):
            entity_class(**kwargs)


class TestUser:
    def test_optional_name_fields_default_to_none(self):
        user = User(email="user@example.com")

        assert user.email == "user@example.com"
        assert user.first_name is None
        assert user.last_name is None


class TestColumn:
    def test_defaults(self):
        column = Column(name="id", data_type="INTEGER")

        assert column.description is None
        assert column.tags == []


class TestTable:
    def test_defaults(self):
        table = Table(database="db", cluster="cluster", name="orders")

        assert table.tags == []
        assert table.description is None
        assert table.columns == []
        assert table.owners == []
        assert table.extra == {}
        assert table.type_hint is None

    def test_asdict_serializes_nested_entities(self):
        table = Table(
            database="db",
            cluster="cluster",
            name="orders",
            tags=[Tag(tag_name="pii")],
            columns=[Column(name="id", data_type="INTEGER", tags=[Tag(tag_name="key")])],
            owners=[User(email="owner@example.com", first_name="Ada")],
            extra={"source": "warehouse"},
        )

        serialized = attr.asdict(table)

        assert serialized["tags"] == [{"tag_name": "pii"}]
        assert serialized["columns"][0]["name"] == "id"
        assert serialized["columns"][0]["tags"] == [{"tag_name": "key"}]
        assert serialized["owners"][0]["email"] == "owner@example.com"
        assert serialized["extra"] == {"source": "warehouse"}


@pytest.mark.parametrize(
    ("entity_class", "expected"),
    [
        (File, ("url",)),
        (User, ("email", "first_name", "last_name")),
        (Tag, ("tag_name",)),
        (Column, ("name", "description", "data_type", "tags")),
        (Table, ("database", "cluster", "name", "tags", "description", "columns", "owners", "extra")),
    ],
)
def test_template_fields(entity_class, expected):
    assert entity_class.template_fields == expected


@pytest.mark.parametrize(
    ("arg", "expected"),
    [
        (None, False),
        (False, False),
        (True, True),
    ],
)
def test_default_if_none(arg, expected):
    assert default_if_none(arg) is expected
