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

from airflow.providers.common.compat.lineage.entities import File, User, Tag, Column, Table, default_if_none

def test_file_stores_url_and_type_hint():
    file = File(
        url="s3://bucket1/dir1/file1",
        type_hint="csv",
    )

    assert file.url == "s3://bucket1/dir1/file1"
    assert file.type_hint == "csv"


def test_file_template_fields():
    assert File.template_fields == ("url",)


def test_file_type_hint_defaults_to_none():
    file = File(url="s3://bucket1/dir1/file1")

    assert file.type_hint is None


def test_user_stores_email_and_names():
    user = User(
        email="mike@company.com",
        first_name="Mike",
        last_name="Smith",
    )

    assert user.email == "mike@company.com"
    assert user.first_name == "Mike"
    assert user.last_name == "Smith"


def test_user_names_default_to_none():
    user = User(email="user@example.com")

    assert user.first_name is None
    assert user.last_name is None


def test_user_template_fields():
    assert User.template_fields == ("email", "first_name", "last_name")


def test_tag_stores_tag_name():
    tag = Tag(tag_name="deadline")

    assert tag.tag_name == "deadline"


def test_tag_template_fields():
    assert Tag.template_fields == ("tag_name",)


def test_column_stores_metadata_and_tags():
    tag = Tag(tag_name="sensitive")
    column = Column(
        name="email",
        description="User email address",
        data_type="string",
        tags=[tag],
    )

    assert column.name == "email"
    assert column.description == "User email address"
    assert column.data_type == "string"
    assert column.tags == [tag]


def test_column_optional_fields_default_values():
    column = Column(name="email", data_type="string")

    assert column.description is None
    assert column.tags == []


def test_column_template_fields():
    assert Column.template_fields == ("name", "description", "data_type", "tags")


def test_table_stores_metadata_and_nested_entities():
    tag = Tag(tag_name="warehouse")
    column = Column(
        name="email",
        description="User email address",
        data_type="string",
    )
    owner = User(
        email="owner@example.com",
        first_name="Table",
        last_name="Owner",
    )

    table = Table(
        database="analytics",
        cluster="prod",
        name="events",
        tags=[tag],
        description="Event table",
        columns=[column],
        owners=[owner],
        extra={"source": "airflow"},
        type_hint="table",
    )

    assert table.database == "analytics"
    assert table.cluster == "prod"
    assert table.name == "events"
    assert table.tags == [tag]
    assert table.description == "Event table"
    assert table.columns == [column]
    assert table.owners == [owner]
    assert table.extra == {"source": "airflow"}
    assert table.type_hint == "table"


def test_table_optional_fields_default_values():
    table = Table(database="analytics", cluster="prod", name="events")

    assert table.tags == []
    assert table.description is None
    assert table.columns == []
    assert table.owners == []
    assert table.extra == {}
    assert table.type_hint is None


def test_table_template_fields():
    assert Table.template_fields == (
        "database",
        "cluster",
        "name",
        "tags",
        "description",
        "columns",
        "owners",
        "extra",
    )


@pytest.mark.parametrize(
    ("arg", "expected"),
    [
        (True, True),
        (False, False),
        (None, False),
    ],
)
def test_default_if_none(arg, expected):
    assert default_if_none(arg) is expected

