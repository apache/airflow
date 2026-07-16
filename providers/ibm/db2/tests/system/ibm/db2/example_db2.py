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

"""
Example Dag demonstrating the Db2 Provider.

This Dag shows how to use the Db2 provider for common database operations:

- Using SQLExecuteQueryOperator with Db2Hook for SQL execution
- Using Db2Hook for programmatic queries
- Creating tables and inserting data
- Querying and processing results

Prerequisites:

1. Install the Db2 provider: ``pip install apache-airflow-providers-ibm-db2``
2. Configure a Db2 connection in the Airflow UI or set the environment variable
   ``AIRFLOW_CONN_DB2_DEFAULT='db2://user:password@host:port/database'``
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.ibm.db2.hooks.db2 import Db2Hook
from airflow.sdk import DAG, task

with DAG(
    dag_id="example_db2",
    description="Example Dag demonstrating Db2 Provider functionality",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "db2", "database"],
) as dag:

    @task
    def cleanup_tables() -> None:
        """Drop tables if they exist to ensure clean state."""
        hook = Db2Hook(conn_id="db2_default")
        for table in ["EMPLOYEES_BACKUP", "EMPLOYEES"]:
            try:
                hook.run(f"DROP TABLE {table}")
            except Exception as e:
                if "SQLSTATE=42704" not in str(e):
                    raise

    create_table = SQLExecuteQueryOperator(
        task_id="create_sample_table",
        conn_id="db2_default",
        sql="""
            CREATE TABLE employees (
                employee_id INTEGER NOT NULL PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                department VARCHAR(50),
                salary DECIMAL(10, 2),
                hire_date DATE,
                updated_at TIMESTAMP
            )
        """,
        autocommit=True,
        hook_params={"schema": None},
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_sample_data",
        conn_id="db2_default",
        sql="""
            INSERT INTO employees
                (employee_id, first_name, last_name, department, salary, hire_date, updated_at)
            VALUES
                (1, 'John', 'Doe', 'Engineering', 75000.00, '2023-01-15', CURRENT TIMESTAMP),
                (2, 'Jane', 'Smith', 'Marketing', 65000.00, '2023-02-20', CURRENT TIMESTAMP),
                (3, 'Bob', 'Johnson', 'Sales', 70000.00, '2023-03-10', CURRENT TIMESTAMP)
        """,
        autocommit=True,
        hook_params={"schema": None},
    )

    @task
    def query_employees() -> int:
        """Query employees and return count using Db2Hook."""
        hook = Db2Hook(conn_id="db2_default")
        sql = "SELECT employee_id, first_name, last_name, department, salary FROM employees ORDER BY employee_id"
        records = hook.get_records(sql)
        return len(records)

    update_salary = SQLExecuteQueryOperator(
        task_id="update_employee_salary",
        conn_id="db2_default",
        sql="""
            UPDATE employees
            SET salary = salary * 1.10,
                updated_at = CURRENT TIMESTAMP
            WHERE department = 'Engineering'
        """,
        autocommit=True,
        hook_params={"schema": None},
    )

    calculate_stats = SQLExecuteQueryOperator(
        task_id="calculate_department_stats",
        conn_id="db2_default",
        sql="""
            SELECT
                department,
                COUNT(*) as employee_count,
                AVG(salary) as avg_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary
            FROM employees
            GROUP BY department
            ORDER BY department
        """,
        hook_params={"schema": None},
    )

    @task
    def display_statistics() -> None:
        """Display department statistics using Db2Hook."""
        hook = Db2Hook(conn_id="db2_default")
        sql = """
            SELECT
                department,
                COUNT(*) as employee_count,
                AVG(salary) as avg_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary
            FROM employees
            GROUP BY department
            ORDER BY department
        """
        hook.get_records(sql)

    create_backup = SQLExecuteQueryOperator(
        task_id="create_backup_table",
        conn_id="db2_default",
        sql="""
            CREATE TABLE employees_backup AS
            (SELECT * FROM employees)
            WITH DATA
        """,
        autocommit=True,
        hook_params={"schema": None},
    )

    @task
    def verify_backup() -> None:
        """Verify backup table was created successfully."""
        hook = Db2Hook(conn_id="db2_default")
        original_count = hook.get_first("SELECT COUNT(*) FROM employees")[0]
        backup_count = hook.get_first("SELECT COUNT(*) FROM employees_backup")[0]
        if original_count != backup_count:
            raise ValueError(f"Backup verification failed: {original_count} != {backup_count}")

    (
        cleanup_tables()
        >> create_table
        >> insert_data
        >> query_employees()
        >> update_salary
        >> calculate_stats
        >> display_statistics()
        >> create_backup
        >> verify_backup()
    )


from tests_common.test_utils.system_tests import get_test_run

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
