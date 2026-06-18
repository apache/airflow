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

from airflow.providers.informatica.lineage.sql_parser import parse_sql_tables


class TestParseSqlTables:
    def test_simple_select(self):
        sources, targets = parse_sql_tables("SELECT * FROM orders")
        assert len(sources) == 1
        assert sources[0].table == "orders"
        assert targets == []

    def test_select_with_join(self):
        sql = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
        sources, targets = parse_sql_tables(sql)
        table_names = {r.table for r in sources}
        assert "orders" in table_names
        assert "customers" in table_names
        assert targets == []

    def test_insert_into_select(self):
        sql = "INSERT INTO summary SELECT region, SUM(amount) FROM sales GROUP BY region"
        sources, targets = parse_sql_tables(sql)
        assert len(targets) == 1
        assert targets[0].table == "summary"
        assert any(r.table == "sales" for r in sources)

    def test_insert_into_select_multi_source(self):
        sql = "INSERT INTO dest SELECT * FROM src1 JOIN src2 ON src1.id = src2.id"
        sources, targets = parse_sql_tables(sql)
        assert targets[0].table == "dest"
        source_names = {r.table for r in sources}
        assert "src1" in source_names
        assert "src2" in source_names

    def test_insert_with_column_list_and_on_conflict(self):
        sql = """
            INSERT INTO order_summary (order_id, total_amount)
            SELECT o.order_id, o.quantity * o.unit_price
            FROM orders o
            ON CONFLICT (order_id) DO UPDATE
                SET total_amount = EXCLUDED.total_amount
            """
        sources, targets = parse_sql_tables(sql, dialect="postgres")
        target_names = {r.table for r in targets}
        source_names = {r.table for r in sources}
        assert "order_summary" in target_names
        assert "orders" in source_names
        assert "order_summary" not in source_names

    def test_create_table_as_select(self):
        sql = "CREATE TABLE report AS SELECT * FROM raw_data"
        sources, targets = parse_sql_tables(sql)
        assert targets[0].table == "report"
        assert sources[0].table == "raw_data"

    def test_schema_qualified_table(self):
        sources, _ = parse_sql_tables("SELECT * FROM public.users")
        assert sources[0].table == "users"
        assert sources[0].schema == "public"

    def test_database_qualified_table(self):
        sources, _ = parse_sql_tables("SELECT * FROM mydb.public.users", dialect="postgres")
        assert sources[0].table == "users"
        assert sources[0].schema == "public"
        assert sources[0].database == "mydb"

    def test_cte_name_excluded_from_sources(self):
        sql = """
        WITH recent AS (SELECT * FROM events WHERE ts > '2024-01-01')
        SELECT * FROM recent
        """
        sources, _ = parse_sql_tables(sql)
        table_names = {r.table for r in sources}
        assert "recent" not in table_names
        assert "events" in table_names

    def test_list_of_statements(self):
        statements = [
            "INSERT INTO a SELECT * FROM b",
            "INSERT INTO c SELECT * FROM d",
        ]
        sources, targets = parse_sql_tables(statements)
        target_names = {r.table for r in targets}
        source_names = {r.table for r in sources}
        assert "a" in target_names
        assert "c" in target_names
        assert "b" in source_names
        assert "d" in source_names

    def test_dedup_repeated_table(self):
        sql = "SELECT * FROM t JOIN t ON t.a = t.b"
        sources, _ = parse_sql_tables(sql)
        assert len(sources) == 1

    def test_empty_sql_returns_empty(self):
        sources, targets = parse_sql_tables("")
        assert sources == []
        assert targets == []

    def test_whitespace_only_sql_returns_empty(self):
        sources, targets = parse_sql_tables("   ")
        assert sources == []
        assert targets == []

    def test_jinja_template_skipped(self):
        sources, targets = parse_sql_tables("SELECT * FROM {{ params.table }}")
        assert sources == []
        assert targets == []

    def test_invalid_sql_returns_empty_not_raises(self):
        sources, targets = parse_sql_tables("THIS IS NOT SQL @@@@")
        assert isinstance(sources, list)
        assert isinstance(targets, list)

    @pytest.mark.parametrize("dialect", ["postgres", "mysql", "snowflake"])
    def test_dialect_variants_parse_without_error(self, dialect):
        sql = "SELECT id, name FROM customers WHERE active = 1"
        sources, _ = parse_sql_tables(sql, dialect=dialect)
        assert sources[0].table == "customers"

    def test_merge_into(self):
        sql = """
        MERGE INTO target t
        USING source s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.val = s.val
        """
        sources, targets = parse_sql_tables(sql, dialect="tsql")
        assert any(r.table == "target" for r in targets)
        assert any(r.table == "source" for r in sources)
