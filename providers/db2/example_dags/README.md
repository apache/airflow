<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Db2 Provider Example DAG

This directory contains example DAGs demonstrating how to use the Apache Airflow Db2 Provider.

## Example DAG: `db2_example_dag.py`

A comprehensive example showing all major features of the Db2 provider.

### Features Demonstrated

1. **Db2Operator** - Executing SQL statements with the operator
2. **Db2Hook** - Programmatic database queries
3. **Table Creation** - Creating tables with various data types
4. **Data Insertion** - Inserting multiple rows
5. **Data Updates** - Updating records
6. **Query Execution** - Fetching and processing results
7. **Aggregations** - Computing statistics
8. **Backup Operations** - Creating table backups

### Prerequisites

#### 1. Install the Db2 Provider

```bash
pip install apache-airflow-providers-db2
```

Or install from the wheel file:

```bash
pip install apache_airflow_providers_db2-1.0.0-py3-none-any.whl
```

#### 2. Configure Db2 Connection

**Option A: Via Airflow UI**

1. Go to Admin → Connections
2. Click the "+" button to add a new connection
3. Fill in the details:
   - **Connection Id**: `db2_default`
   - **Connection Type**: `IBM Db2`
   - **Host**: Your Db2 server hostname
   - **Schema**: Your database name
   - **Login**: Your Db2 username
   - **Password**: Your Db2 password
   - **Port**: 50000 (or your Db2 port)
   - **Extra**: (Optional) JSON with additional parameters:

     ```json
     {
       "SECURITY": "SSL",
       "SSLServerCertificate": "/path/to/cert.arm"
     }
     ```

**Option B: Via Environment Variable**

```bash
export AIRFLOW_CONN_DB2_DEFAULT='db2://username:password@hostname:50000/database'
```

With SSL:

```bash
export AIRFLOW_CONN_DB2_DEFAULT='db2://username:password@hostname:50000/database?SECURITY=SSL&SSLServerCertificate=/path/to/cert.arm'
```

#### 3. Copy the DAG to Airflow

```bash
# Copy the example DAG to your Airflow DAGs folder
cp db2_example_dag.py $AIRFLOW_HOME/dags/
```

### Running the DAG

#### 1. Start Airflow

```bash
# Initialize the database (first time only)
airflow db init

# Create an admin user (first time only)
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start the webserver
airflow webserver --port 8080

# In another terminal, start the scheduler
airflow scheduler
```

#### 2. Access the Airflow UI

Open your browser and go to: http://localhost:8080

#### 3. Trigger the DAG

1. Log in with your admin credentials
2. Find `db2_example_dag` in the DAGs list
3. Toggle the DAG to "On" (if it's paused)
4. Click the "Play" button to trigger the DAG manually

### What You'll See in the UI

#### DAG Graph View

The DAG will show a visual flow of tasks:

```
create_sample_table (Db2Operator)
         ↓
insert_sample_data (Db2Operator)
         ↓
query_employees (Db2Hook)
         ↓
update_employee_salary (Db2Operator)
         ↓
calculate_department_stats (Db2Operator)
         ↓
display_statistics (Db2Hook)
         ↓
create_backup_table (Db2Operator)
         ↓
verify_backup (Db2Hook)
```

#### Task Logs

Each task will show detailed logs:

- **create_sample_table**: SQL execution for table creation using Db2Operator
- **insert_sample_data**: Data insertion using Db2Operator
- **query_employees**: Employee records fetched using Db2Hook
- **update_employee_salary**: Salary updates using Db2Operator
- **calculate_department_stats**: Statistics calculation using Db2Operator
- **display_statistics**: Department statistics displayed using Db2Hook
- **create_backup_table**: Backup table creation using Db2Operator
- **verify_backup**: Backup verification using Db2Hook

#### Example Output

When you view the logs for `query_employees`, you'll see:

```
Found 3 employees:
  ID: 1, Name: John Doe, Dept: Engineering, Salary: $75,000.00
  ID: 2, Name: Jane Smith, Dept: Marketing, Salary: $65,000.00
  ID: 3, Name: Bob Johnson, Dept: Sales, Salary: $70,000.00
```

After `update_employee_salary`:

```
Engineering department salaries increased by 10%
```

Department statistics from `display_statistics`:

```
Department Statistics:
--------------------------------------------------------------------------------
Department: Engineering
  Employees: 1
  Avg Salary: $82,500.00
  Min Salary: $82,500.00
  Max Salary: $82,500.00

Department: Marketing
  Employees: 1
  Avg Salary: $65,000.00
  Min Salary: $65,000.00
  Max Salary: $65,000.00

Department: Sales
  Employees: 1
  Avg Salary: $70,000.00
  Min Salary: $70,000.00
  Max Salary: $70,000.00
```

Backup verification:

```
Original table: 3 records
Backup table: 3 records
✓ Backup verified successfully!
```

### Customizing the DAG

#### Change the Connection ID

If you're using a different connection ID, update this line:

```python
db2_conn_id = "db2_default"  # Change to your connection ID
```

#### Modify the Schedule

To run the DAG on a schedule instead of manually:

```python
schedule = ("0 0 * * *",)  # Run daily at midnight
```

#### Using Db2Operator

The Db2Operator is the recommended way to execute SQL:

```python
from airflow.providers.db2.operators.db2 import Db2Operator

create_table = Db2Operator(
    task_id="create_table",
    sql="CREATE TABLE my_table (id INT, name VARCHAR(50))",
    db2_conn_id="db2_default",
)
```

#### Using Db2Hook

For programmatic queries, use Db2Hook:

```python
from airflow.providers.db2.hooks.db2 import Db2Hook


@task
def custom_db2_task():
    hook = Db2Hook(db2_conn_id="db2_default")

    # Get multiple records
    records = hook.get_records("SELECT * FROM my_table")

    # Get single value
    count = hook.get_first("SELECT COUNT(*) FROM my_table")[0]

    return count
```

### Troubleshooting

#### Connection Issues

If you see connection errors:

1. Verify your Db2 server is accessible
2. Check firewall rules allow connections on the Db2 port
3. Verify credentials are correct
4. For SSL connections, ensure the certificate path is correct

#### Import Errors

If you see "No module named 'airflow.providers.db2'":

```bash
# Reinstall the provider
pip install --force-reinstall apache-airflow-providers-db2
```

#### Task Failures

Check the task logs in the Airflow UI for detailed error messages. Common issues:

- **Table already exists**: Drop the table first or modify the SQL
- **Permission denied**: Ensure your Db2 user has necessary privileges
- **Syntax errors**: Verify SQL syntax is compatible with your Db2 version

### Additional Resources

- [Db2 Provider Documentation](../README.rst)
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [IBM Db2 Documentation](https://www.ibm.com/docs/en/db2)

### Code Examples

#### Simple Query with Db2Operator

```python
query_task = Db2Operator(
    task_id="query_data",
    sql="SELECT * FROM employees WHERE department = 'Engineering'",
)
```

#### Parameterized Query with Db2Operator

```python
insert_task = Db2Operator(
    task_id="insert_data",
    sql="INSERT INTO employees (id, name) VALUES (?, ?)",
    parameters=(1, "John Doe"),
)
```

#### Multiple Statements with Db2Operator

```python
cleanup_task = Db2Operator(
    task_id="cleanup",
    sql="""
        DROP TABLE temp_table;
        DROP TABLE backup_table;
    """,
    split_statements=True,
)
```

#### Complex Query with Db2Hook

```python
@task
def analyze_data():
    hook = Db2Hook(db2_conn_id="db2_default")

    # Execute complex query
    sql = """
        SELECT
            department,
            COUNT(*) as count,
            AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
        HAVING COUNT(*) > 5
    """

    results = hook.get_records(sql)

    # Process results
    for dept, count, avg_sal in results:
        print(f"{dept}: {count} employees, avg ${avg_sal:,.2f}")

    return results
