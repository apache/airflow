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
Example DAG demonstrating the Db2 Provider

This DAG shows how to use the Db2 provider for common database operations:
- Using Db2Operator for SQL execution
- Using Db2Hook for programmatic queries
- Creating tables and inserting data
- Querying and processing results

Prerequisites:
1. Install the Db2 provider: pip install apache-airflow-providers-db2
2. Configure a Db2 connection in Airflow UI or via environment variable:
   AIRFLOW_CONN_DB2_DEFAULT='db2://user:password@host:port/database'
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.db2.hooks.db2 import Db2Hook
from airflow.providers.db2.operators.db2 import Db2Operator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="db2_example_dag",
    default_args=default_args,
    description="Example DAG demonstrating Db2 Provider functionality",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "db2", "database"],
)
def db2_example_dag() -> None:
    """
    Example DAG showing Db2 provider capabilities.
    """

    # Task 0: Cleanup - Drop tables if they exist (for idempotent runs)
    @task
    def cleanup_tables():
        """Drop tables if they exist to ensure clean state."""
        hook = Db2Hook(db2_conn_id="db2_default")

        for table in ["EMPLOYEES_BACKUP", "EMPLOYEES"]:
            try:
                hook.run(f"DROP TABLE {table}")
                print(f"✓ Dropped table {table}")
            except Exception as e:
                if "SQLSTATE=42704" in str(e):  # Table doesn't exist
                    print(f"ℹ Table {table} doesn't exist, skipping")
                else:
                    raise

    # Task 1: Create a sample table using Db2Operator
    create_table = Db2Operator(
        task_id="create_sample_table",
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
        autocommit=True,  # Required for DDL statements in Db2
    )

    # Task 2: Insert sample data using Db2Operator with parameters
    insert_data = Db2Operator(
        task_id="insert_sample_data",
        sql="""
            INSERT INTO employees (employee_id, first_name, last_name, department, salary, hire_date, updated_at)
            VALUES
                (1, 'John', 'Doe', 'Engineering', 75000.00, '2023-01-15', CURRENT TIMESTAMP),
                (2, 'Jane', 'Smith', 'Marketing', 65000.00, '2023-02-20', CURRENT TIMESTAMP),
                (3, 'Bob', 'Johnson', 'Sales', 70000.00, '2023-03-10', CURRENT TIMESTAMP)
        """,
        autocommit=True,  # Ensure data is committed
    )

    # Task 3: Query data using Db2Hook
    @task
    def query_employees():
        """Query employees and return results using Db2Hook."""
        hook = Db2Hook(db2_conn_id="db2_default")

        # Execute query and fetch results
        sql = "SELECT employee_id, first_name, last_name, department, salary FROM employees ORDER BY employee_id"
        records = hook.get_records(sql)

        print(f"Found {len(records)} employees:")
        for record in records:
            print(
                f"  ID: {record[0]}, Name: {record[1]} {record[2]}, Dept: {record[3]}, Salary: ${record[4]:,.2f}"
            )

        return len(records)

    # Task 4: Update employee salary using Db2Operator with parameters
    update_salary = Db2Operator(
        task_id="update_employee_salary",
        sql="""
            UPDATE employees
            SET salary = salary * 1.10,
                updated_at = CURRENT TIMESTAMP
            WHERE department = 'Engineering'
        """,
        autocommit=True,  # Ensure updates are committed
    )

    # Task 5: Calculate department statistics using Db2Operator
    calculate_stats = Db2Operator(
        task_id="calculate_department_stats",
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
    )

    # Task 6: Query and display statistics using Db2Hook
    @task
    def display_statistics():
        """Display department statistics using Db2Hook."""
        hook = Db2Hook(db2_conn_id="db2_default")

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

        stats = hook.get_records(sql)

        print("\nDepartment Statistics:")
        print("-" * 80)
        for row in stats:
            dept, count, avg_sal, min_sal, max_sal = row
            print(f"Department: {dept}")
            print(f"  Employees: {count}")
            print(f"  Avg Salary: ${avg_sal:,.2f}")
            print(f"  Min Salary: ${min_sal:,.2f}")
            print(f"  Max Salary: ${max_sal:,.2f}")
            print()

    # Task 7: Create a backup table using Db2Operator
    create_backup = Db2Operator(
        task_id="create_backup_table",
        sql="""
            CREATE TABLE employees_backup AS
            (SELECT * FROM TESTUSER1.employees)
            WITH DATA
        """,
        autocommit=True,  # Required for DDL statements in Db2
    )

    # Task 8: Verify backup using Db2Hook
    @task
    def verify_backup():
        """Verify backup table was created successfully."""
        hook = Db2Hook(db2_conn_id="db2_default")

        # Count records in both tables
        original_count = hook.get_first("SELECT COUNT(*) FROM employees")[0]
        backup_count = hook.get_first("SELECT COUNT(*) FROM employees_backup")[0]

        print(f"Original table: {original_count} records")
        print(f"Backup table: {backup_count} records")

        if original_count == backup_count:
            print("✓ Backup verified successfully!")
        else:
            raise ValueError(f"Backup verification failed: {original_count} != {backup_count}")

    # Task 9: Cleanup (optional - commented out by default)
    # cleanup = Db2Operator(
    #     task_id="cleanup_tables",
    #     sql="""
    #         DROP TABLE employees;
    #         DROP TABLE employees_backup;
    #     """,
    #     split_statements=True,
    # )

    # Define task dependencies
    cleanup_tables() >> create_table >> insert_data >> query_employees() >> update_salary
    update_salary >> calculate_stats >> display_statistics()
    display_statistics() >> create_backup >> verify_backup()


# Instantiate the DAG
dag_instance = db2_example_dag()
