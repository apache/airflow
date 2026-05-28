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

"""IBM Db2 operator for executing SQL statements."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.providers.db2.hooks.db2 import Db2Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class Db2Operator(BaseOperator):
    """
    Execute SQL statements against an IBM Db2 database.

    This operator executes SQL statements using the Db2Hook. It supports:
    - SQL strings passed directly
    - SQL files loaded from disk
    - Jinja templating in SQL statements
    - Parameter binding for secure queries
    - Autocommit or manual transaction control

    :param sql: The SQL statement(s) to execute. Can be a string or path to a SQL file.
        Supports Jinja templating.
    :param db2_conn_id: The Airflow connection ID for the Db2 database.
    :param parameters: Parameters to bind to the SQL statement (for parameterized queries).
    :param autocommit: Whether to automatically commit after execution.
    :param split_statements: Whether to split SQL into multiple statements (for batch execution).
    :param return_last: Whether to return the result of the last statement only.

    **Example usage**:

    .. code-block:: python

        # Execute a simple query
        query_task = Db2Operator(
            task_id="query_employees",
            sql="SELECT * FROM EMPLOYEES WHERE DEPARTMENT = 'Engineering'",
            db2_conn_id="db2_default",
        )

        # Execute with parameters (prevents SQL injection)
        insert_task = Db2Operator(
            task_id="insert_employee",
            sql="INSERT INTO EMPLOYEES (ID, NAME, DEPT) VALUES (?, ?, ?)",
            parameters=(101, "John Doe", "Engineering"),
            db2_conn_id="db2_default",
        )

        # Execute SQL from a file
        batch_task = Db2Operator(
            task_id="run_batch_sql",
            sql="sql/batch_operations.sql",
            db2_conn_id="db2_default",
        )

        # Use Jinja templating
        dynamic_task = Db2Operator(
            task_id="dynamic_query",
            sql="SELECT * FROM EMPLOYEES WHERE HIRE_DATE >= '{{ ds }}'",
            db2_conn_id="db2_default",
        )
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#4682B4"

    def __init__(
        self,
        *,
        sql: str,
        db2_conn_id: str = "db2_default",
        parameters: dict[str, Any] | tuple | list | None = None,
        autocommit: bool = False,
        split_statements: bool = False,
        return_last: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.db2_conn_id = db2_conn_id
        self.parameters = parameters
        self.autocommit = autocommit
        self.split_statements = split_statements
        self.return_last = return_last
        self.hook: Db2Hook | None = None

    def execute(self, context: Context) -> Any:
        """Execute the SQL statement(s) against the Db2 database."""
        self.log.info("Executing SQL against Db2 database")
        self.log.info("SQL: %s", self.sql)

        self.hook = Db2Hook(db2_conn_id=self.db2_conn_id)
        conn = self.hook.get_conn()
        cursor = conn.cursor()

        try:
            if self.split_statements:
                statements = self._split_sql_statements(self.sql)
                results = []

                for statement in statements:
                    stripped_stmt = statement.strip()
                    if not stripped_stmt:
                        continue

                    self.log.info("Executing statement: %s", stripped_stmt[:100])

                    if self.parameters:
                        cursor.execute(stripped_stmt, self.parameters)
                    else:
                        cursor.execute(stripped_stmt)

                    if stripped_stmt.upper().strip().startswith("SELECT"):
                        result = cursor.fetchall()
                        results.append(result)
                        self.log.info("Fetched %d rows", len(result))

                    if self.autocommit:
                        conn.commit()

                if self.return_last and results:
                    return results[-1]
                return results if results else None

            if self.parameters:
                cursor.execute(self.sql, self.parameters)
            else:
                cursor.execute(self.sql)

            if self.sql.upper().strip().startswith("SELECT"):
                result = cursor.fetchall()
                self.log.info("Fetched %d rows", len(result))
                return result

            if self.autocommit:
                conn.commit()

            return None

        except Exception as e:
            self.log.error("Error executing SQL: %s", str(e))
            conn.rollback()
            raise

        finally:
            cursor.close()
            conn.close()
            self.log.info("Db2 connection closed")

    def _split_sql_statements(self, sql: str) -> list[str]:
        """Split SQL string into individual statements."""
        statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
        return statements

    def on_kill(self) -> None:
        """Clean up resources when task is killed."""
        if self.hook:
            try:
                conn = self.hook.get_conn()
                conn.rollback()
                conn.close()
                self.log.info("Rolled back transaction and closed connection")
            except Exception as e:
                self.log.warning("Error during cleanup: %s", str(e))
