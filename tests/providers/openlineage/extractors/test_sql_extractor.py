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

from openlineage.common.sql import DbTableMeta

from airflow.providers.openlineage.extractors.sql import SqlExtractor


def normalize_name_lower(name: str) -> str:
    return name.lower()


def test_get_tables_hierarchy():
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Table1"), DbTableMeta("Table2")], normalize_name_lower
    ) == {None: {None: ["Table1", "Table2"]}}

    # base check with db, no cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Db.Schema1.Table1"), DbTableMeta("Db.Schema2.Table2")], normalize_name_lower
    ) == {None: {"schema1": ["Table1"], "schema2": ["Table2"]}}

    # same, with cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Db.Schema1.Table1"), DbTableMeta("Db.Schema2.Table2")],
        normalize_name_lower,
        is_cross_db=True,
    ) == {"db": {"schema1": ["Table1"], "schema2": ["Table2"]}}

    # explicit db, no cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Schema1.Table1"), DbTableMeta("Schema1.Table2")],
        normalize_name_lower,
        database="Db",
    ) == {None: {"schema1": ["Table1", "Table2"]}}

    # explicit db, with cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Schema1.Table1"), DbTableMeta("Schema1.Table2")],
        normalize_name_lower,
        database="Db",
        is_cross_db=True,
    ) == {"db": {"schema1": ["Table1", "Table2"]}}

    # mixed db, with cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Db2.Schema1.Table1"), DbTableMeta("Schema1.Table2")],
        normalize_name_lower,
        database="Db",
        is_cross_db=True,
    ) == {"db": {"schema1": ["Table2"]}, "db2": {"schema1": ["Table1"]}}


def test_get_sql_iterator():
    assert SqlExtractor._normalize_sql("select * from asdf") == "select * from asdf"

    assert (
        SqlExtractor._normalize_sql(["select * from asdf", "insert into asdf values (1,2,3)"])
        == "select * from asdf;\ninsert into asdf values (1,2,3)"
    )

    assert (
        SqlExtractor._normalize_sql("select * from asdf;insert into asdf values (1,2,3)")
        == "select * from asdf;\ninsert into asdf values (1,2,3)"
    )
