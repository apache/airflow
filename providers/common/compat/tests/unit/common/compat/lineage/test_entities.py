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

from airflow.providers.common.compat.lineage.entities import Column, Table, Tag, User


class TestMutableDefaultsAreNotShared:
    """Each instance must get its own container; a bare ``= []`` default is shared class-wide."""

    def test_column_tags_are_isolated_between_instances(self):
        first = Column(name="a", data_type="INT")
        second = Column(name="b", data_type="INT")

        first.tags.append(Tag(tag_name="pii"))

        assert first.tags == [Tag(tag_name="pii")]
        assert second.tags == []
        assert first.tags is not second.tags

    def test_table_containers_are_isolated_between_instances(self):
        first = Table(database="db", cluster="cluster", name="first")
        second = Table(database="db", cluster="cluster", name="second")

        first.tags.append(Tag(tag_name="pii"))
        first.columns.append(Column(name="id", data_type="INT"))
        first.owners.append(User(email="owner@example.com"))
        first.extra["source"] = "warehouse"

        assert second.tags == []
        assert second.columns == []
        assert second.owners == []
        assert second.extra == {}
