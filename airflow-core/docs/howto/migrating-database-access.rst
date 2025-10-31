 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Migrating Database Access from Airflow 2 to Airflow 3
=====================================================

This guide provides detailed examples for migrating database access patterns from Airflow 2 to Airflow 3.

Overview of Changes
-------------------

In Airflow 2, tasks could directly access the Airflow metadata database using database sessions. This capability has been removed in Airflow 3 for the following reasons:

- **Enhanced Security**: Prevents malicious task code from accessing or modifying the Airflow metadata database
- **Better Isolation**: Tasks run in isolated environments without direct database dependencies
- **Improved Scalability**: Reduces database connection overhead and contention
- **Stable Interface**: The Task SDK provides a forward-compatible API that doesn't depend on internal database schema changes

**What Changed in Detail:**

In Airflow 2, the following patterns were common and supported:

.. code-block:: python

    # Direct imports that worked in Airflow 2
    from airflow import settings
    from airflow.utils.session import provide_session, create_session
    from airflow.models import TaskInstance, DagRun, Variable, Connection, XCom
    from sqlalchemy.orm import sessionmaker

    # These patterns are NO LONGER SUPPORTED in Airflow 3:

    # 1. Direct settings.Session access
    session = settings.Session()


    # 2. @provide_session decorator
    @provide_session
    def my_function(session=None):
        pass


    # 3. create_session() context manager
    with create_session() as session:
        pass

    # 4. Direct model imports and queries
    from airflow.models import TaskInstance

    session.query(TaskInstance).filter(...)

**Why These Changes Were Made:**

1. **Security Vulnerabilities**: In Airflow 2, any task could potentially:
   - Read sensitive data from the metadata database
   - Modify task states, DAG runs, or other critical metadata
   - Access connection passwords and other secrets
   - Corrupt the database through malicious or buggy code

2. **Database Connection Overload**: Every task that used database sessions created additional connections, leading to:
   - Database connection pool exhaustion
   - Performance degradation under high load
   - Difficulty in scaling Airflow deployments

3. **Tight Coupling**: Direct database access created:
   - Dependencies on internal Airflow database schema
   - Brittleness when upgrading Airflow versions
   - Difficulty in maintaining backward compatibility

4. **Architectural Improvements**: Airflow 3's new architecture provides:
   - Centralized API server for all database operations
   - Better monitoring and logging of database access
   - Ability to implement fine-grained access controls
   - Easier debugging and troubleshooting

Migration Strategies
--------------------

Strategy 1: Use Task SDK API Client (Recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Task SDK provides API methods for most common database operations:

**Task Instance Operations:**

.. code-block:: python

    # Airflow 2 (deprecated) - Multiple patterns that no longer work
    from airflow.utils.session import provide_session, create_session
    from airflow.models import TaskInstance
    from airflow import settings
    from datetime import datetime, timedelta


    # Pattern 1: @provide_session decorator
    @provide_session
    def get_task_count(session=None):
        return (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id == "my_dag", TaskInstance.state == "success")
            .count()
        )


    # Pattern 2: create_session context manager
    def get_failed_tasks_last_week():
        with create_session() as session:
            week_ago = datetime.now() - timedelta(days=7)
            return (
                session.query(TaskInstance)
                .filter(TaskInstance.start_date >= week_ago, TaskInstance.state == "failed")
                .all()
            )


    # Pattern 3: Direct settings.Session usage
    def get_task_duration_stats():
        session = settings.Session()
        try:
            results = (
                session.query(TaskInstance.task_id, TaskInstance.duration)
                .filter(
                    TaskInstance.dag_id == "my_dag",
                    TaskInstance.state == "success",
                    TaskInstance.duration.isnot(None),
                )
                .all()
            )
            session.commit()
            return results
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()


    # Pattern 4: Complex queries with joins
    @provide_session
    def get_task_with_dag_info(session=None):
        from airflow.models import DagRun

        return (
            session.query(TaskInstance, DagRun)
            .join(DagRun, TaskInstance.dag_id == DagRun.dag_id)
            .filter(TaskInstance.state == "running")
            .all()
        )


    # Airflow 3 (recommended) - Using Task SDK API
    from airflow.sdk import BaseOperator
    from datetime import datetime, timedelta


    class TaskCountOperator(BaseOperator):
        """Get count of tasks matching specific criteria."""

        def __init__(self, target_dag_id: str = None, states: list = None, **kwargs):
            super().__init__(**kwargs)
            self.target_dag_id = target_dag_id
            self.states = states or ["success"]

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # Basic count operation
            result = client.task_instances.get_count(
                dag_id=self.target_dag_id or context["dag"].dag_id, states=self.states
            )

            self.log.info(f"Found {result.count} tasks in states {self.states}")
            return result.count


    class TaskStatesAnalysisOperator(BaseOperator):
        """Analyze task states across multiple criteria."""

        def __init__(self, analysis_dag_id: str, **kwargs):
            super().__init__(**kwargs)
            self.analysis_dag_id = analysis_dag_id

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # Get task states for analysis
            task_states = client.task_instances.get_task_states(
                dag_id=self.analysis_dag_id, logical_dates=[context["logical_date"]]
            )

            # Analyze the results
            analysis = {
                "total_tasks": len(task_states.task_states),
                "success_count": 0,
                "failed_count": 0,
                "running_count": 0,
                "other_count": 0,
            }

            for task_state in task_states.task_states:
                if task_state.state == "success":
                    analysis["success_count"] += 1
                elif task_state.state == "failed":
                    analysis["failed_count"] += 1
                elif task_state.state == "running":
                    analysis["running_count"] += 1
                else:
                    analysis["other_count"] += 1

            self.log.info(f"Task analysis: {analysis}")
            return analysis


    class MultiDagTaskCountOperator(BaseOperator):
        """Get task counts across multiple DAGs and states."""

        def __init__(self, dag_ids: list, **kwargs):
            super().__init__(**kwargs)
            self.dag_ids = dag_ids

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            results = {}

            for dag_id in self.dag_ids:
                # Get counts for different states
                success_count = client.task_instances.get_count(dag_id=dag_id, states=["success"]).count

                failed_count = client.task_instances.get_count(dag_id=dag_id, states=["failed"]).count

                running_count = client.task_instances.get_count(dag_id=dag_id, states=["running"]).count

                results[dag_id] = {
                    "success": success_count,
                    "failed": failed_count,
                    "running": running_count,
                    "total": success_count + failed_count + running_count,
                }

            self.log.info(f"Multi-DAG analysis: {results}")
            return results

**Key Differences and Limitations:**

1. **No Direct SQL Queries**: You cannot write custom SQL queries against the metadata database
2. **API-Based Access**: All operations go through predefined API endpoints
3. **Limited Filtering**: You're restricted to the filtering options provided by the API
4. **No Joins**: Complex queries with joins across multiple tables are not directly supported
5. **Structured Responses**: All responses are structured data objects, not raw database rows

**Advanced Task Instance Patterns:**

For more complex scenarios that were possible in Airflow 2 but need different approaches in Airflow 3:

.. code-block:: python

    # Airflow 2: Complex task analysis with custom SQL
    @provide_session
    def analyze_task_performance(session=None):
        # This type of complex analysis is not directly possible in Airflow 3
        sql = """
        SELECT
            ti.task_id,
            AVG(ti.duration) as avg_duration,
            COUNT(*) as execution_count,
            COUNT(CASE WHEN ti.state = 'success' THEN 1 END) as success_count,
            COUNT(CASE WHEN ti.state = 'failed' THEN 1 END) as failure_count
        FROM task_instance ti
        WHERE ti.dag_id = 'my_dag'
        AND ti.start_date >= NOW() - INTERVAL '30 days'
        GROUP BY ti.task_id
        ORDER BY avg_duration DESC
        """
        return session.execute(sql).fetchall()


    # Airflow 3: Alternative approaches for complex analysis
    class TaskPerformanceAnalysisOperator(BaseOperator):
        """Alternative approach using multiple API calls and local processing."""

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # Get task states for recent runs
            from datetime import datetime, timedelta

            # Note: This requires multiple API calls and local aggregation
            # which may be less efficient than direct SQL but is more secure

            recent_date = datetime.now() - timedelta(days=30)

            # Get task states (this might need to be done in batches)
            task_states = client.task_instances.get_task_states(
                dag_id=context["dag"].dag_id,
                # Note: API limitations may require multiple calls for date ranges
            )

            # Process results locally
            analysis = {}
            for task_state in task_states.task_states:
                task_id = task_state.task_id
                if task_id not in analysis:
                    analysis[task_id] = {"executions": 0, "successes": 0, "failures": 0}

                analysis[task_id]["executions"] += 1
                if task_state.state == "success":
                    analysis[task_id]["successes"] += 1
                elif task_state.state == "failed":
                    analysis[task_id]["failures"] += 1

            return analysis

**DAG Run Operations:**

.. code-block:: python

    # Airflow 2 (deprecated) - Various DAG run access patterns
    from airflow.utils.session import create_session, provide_session
    from airflow.models import DagRun
    from datetime import datetime, timedelta
    from sqlalchemy import func, and_, or_


    # Pattern 1: Simple DAG run queries
    def get_recent_dag_runs():
        with create_session() as session:
            return (
                session.query(DagRun).filter(DagRun.dag_id == "my_dag", DagRun.state == "success").limit(10).all()
            )


    # Pattern 2: Complex DAG run analysis
    @provide_session
    def analyze_dag_run_patterns(session=None):
        # Get DAG run statistics for the last month
        month_ago = datetime.now() - timedelta(days=30)

        stats = (
            session.query(
                DagRun.dag_id,
                func.count(DagRun.id).label("total_runs"),
                func.count(DagRun.id).filter(DagRun.state == "success").label("success_runs"),
                func.count(DagRun.id).filter(DagRun.state == "failed").label("failed_runs"),
                func.avg(DagRun.end_date - DagRun.start_date).label("avg_duration"),
            )
            .filter(DagRun.start_date >= month_ago)
            .group_by(DagRun.dag_id)
            .all()
        )

        return stats


    # Pattern 3: DAG run dependencies and relationships
    @provide_session
    def find_dependent_dag_runs(dag_id, run_id, session=None):
        # Find all DAG runs that might be dependent on this one
        base_run = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id).first()

        if not base_run:
            return []

        # Find runs that started after this one completed
        dependent_runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id != dag_id, DagRun.start_date >= base_run.end_date)  # Different DAG
            .all()
        )

        return dependent_runs


    # Pattern 4: Historical DAG run analysis
    @provide_session
    def get_dag_run_history(dag_id, days_back=30, session=None):
        cutoff_date = datetime.now() - timedelta(days=days_back)

        runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id, DagRun.start_date >= cutoff_date)
            .order_by(DagRun.start_date.desc())
            .all()
        )

        # Calculate success rate, average duration, etc.
        total_runs = len(runs)
        successful_runs = len([r for r in runs if r.state == "success"])
        failed_runs = len([r for r in runs if r.state == "failed"])

        durations = [r.end_date - r.start_date for r in runs if r.end_date and r.start_date]
        avg_duration = sum(durations, timedelta()) / len(durations) if durations else timedelta()

        return {
            "total_runs": total_runs,
            "success_rate": successful_runs / total_runs if total_runs > 0 else 0,
            "failed_runs": failed_runs,
            "average_duration": avg_duration,
            "runs": runs,
        }


    # Airflow 3 (recommended) - Using Task SDK API
    from airflow.sdk import BaseOperator
    from datetime import datetime, timedelta


    class DagRunAnalysisOperator(BaseOperator):
        """Detailed DAG run analysis using Task SDK API."""

        def __init__(self, target_dag_ids: list = None, analysis_days: int = 30, **kwargs):
            super().__init__(**kwargs)
            self.target_dag_ids = target_dag_ids or []
            self.analysis_days = analysis_days

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # If no target DAGs specified, analyze current DAG
            dag_ids = self.target_dag_ids or [context["dag"].dag_id]

            analysis_results = {}

            for dag_id in dag_ids:
                # Get total count of DAG runs
                total_count = client.dag_runs.get_count(dag_id=dag_id)

                # Get successful runs count
                success_count = client.dag_runs.get_count(dag_id=dag_id, states=["success"])

                # Get failed runs count
                failed_count = client.dag_runs.get_count(dag_id=dag_id, states=["failed"])

                # Get running runs count
                running_count = client.dag_runs.get_count(dag_id=dag_id, states=["running"])

                # Get previous successful run
                try:
                    prev_success = client.dag_runs.get_previous(
                        dag_id=dag_id, logical_date=context["logical_date"], state="success"
                    )
                    prev_success_info = prev_success.dag_run if prev_success.dag_run else None
                except Exception as e:
                    self.log.warning(f"Could not get previous successful run for {dag_id}: {e}")
                    prev_success_info = None

                analysis_results[dag_id] = {
                    "total_runs": total_count.count,
                    "successful_runs": success_count.count,
                    "failed_runs": failed_count.count,
                    "running_runs": running_count.count,
                    "success_rate": success_count.count / total_count.count if total_count.count > 0 else 0,
                    "previous_successful_run": prev_success_info,
                }

            self.log.info(f"DAG run analysis completed: {analysis_results}")
            return analysis_results


    class DagRunStateMonitorOperator(BaseOperator):
        """Monitor DAG run states and trigger alerts if needed."""

        def __init__(self, monitored_dag_id: str, failure_threshold: int = 3, **kwargs):
            super().__init__(**kwargs)
            self.monitored_dag_id = monitored_dag_id
            self.failure_threshold = failure_threshold

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # Check current state
            current_state = client.dag_runs.get_state(dag_id=self.monitored_dag_id, run_id=context["run_id"])

            # Get recent failure count
            failed_count = client.dag_runs.get_count(dag_id=self.monitored_dag_id, states=["failed"])

            result = {
                "current_state": current_state.state,
                "recent_failures": failed_count.count,
                "alert_triggered": failed_count.count >= self.failure_threshold,
            }

            if result["alert_triggered"]:
                self.log.error(
                    f"ALERT: DAG {self.monitored_dag_id} has {failed_count.count} failures, "
                    f"exceeding threshold of {self.failure_threshold}"
                )

            return result


    class DagRunTriggerOperator(BaseOperator):
        """Trigger DAG runs with proper error handling."""

        def __init__(self, target_dag_id: str, trigger_run_id: str = None, trigger_conf: dict = None, **kwargs):
            super().__init__(**kwargs)
            self.target_dag_id = target_dag_id
            self.trigger_run_id = trigger_run_id
            self.trigger_conf = trigger_conf or {}

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # Generate run_id if not provided
            run_id = self.trigger_run_id or f"triggered_{context['ts_nodash']}"

            # Trigger the DAG run
            result = client.dag_runs.trigger(
                dag_id=self.target_dag_id,
                run_id=run_id,
                conf=self.trigger_conf,
                logical_date=context["logical_date"],
            )

            if hasattr(result, "error"):
                self.log.error(f"Failed to trigger DAG {self.target_dag_id}: {result.error}")
                raise Exception(f"DAG trigger failed: {result.error}")

            self.log.info(f"Successfully triggered DAG {self.target_dag_id} with run_id {run_id}")
            return {
                "triggered_dag_id": self.target_dag_id,
                "triggered_run_id": run_id,
                "trigger_conf": self.trigger_conf,
            }

**Key Limitations in Airflow 3 DAG Run Operations:**

1. **No Complex Aggregations**: You cannot perform complex SQL aggregations like AVG, SUM across multiple runs
2. **Limited Historical Analysis**: Getting historical data requires multiple API calls and local processing
3. **No Cross-DAG Queries**: You cannot query relationships between different DAGs in a single operation
4. **Date Range Limitations**: API endpoints may have limitations on date range queries
5. **No Custom Sorting**: You're limited to the sorting options provided by the API endpoints

**Workarounds for Complex DAG Run Analysis:**

For scenarios that require complex analysis not directly supported by the API:

.. code-block:: python

    class AdvancedDagRunAnalysisOperator(BaseOperator):
        """Perform complex DAG run analysis using multiple API calls."""

        def __init__(self, analysis_dag_ids: list, **kwargs):
            super().__init__(**kwargs)
            self.analysis_dag_ids = analysis_dag_ids

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # Collect data from multiple API calls
            all_dag_data = {}

            for dag_id in self.analysis_dag_ids:
                # Get various counts
                total = client.dag_runs.get_count(dag_id=dag_id).count
                success = client.dag_runs.get_count(dag_id=dag_id, states=["success"]).count
                failed = client.dag_runs.get_count(dag_id=dag_id, states=["failed"]).count
                running = client.dag_runs.get_count(dag_id=dag_id, states=["running"]).count

                all_dag_data[dag_id] = {
                    "total": total,
                    "success": success,
                    "failed": failed,
                    "running": running,
                    "success_rate": success / total if total > 0 else 0,
                }

            # Perform local analysis
            overall_stats = {
                "total_dags_analyzed": len(self.analysis_dag_ids),
                "total_runs_across_all_dags": sum(data["total"] for data in all_dag_data.values()),
                "overall_success_rate": (
                    sum(data["success"] for data in all_dag_data.values())
                    / sum(data["total"] for data in all_dag_data.values())
                    if sum(data["total"] for data in all_dag_data.values()) > 0
                    else 0
                ),
                "dag_details": all_dag_data,
            }

            return overall_stats

**Variable and Connection Access:**

.. code-block:: python

    # Airflow 2 (deprecated) - Various variable and connection patterns
    from airflow.utils.session import provide_session, create_session
    from airflow.models import Variable, Connection
    from sqlalchemy import or_, and_
    import json


    # Pattern 1: Simple variable access
    @provide_session
    def get_config_data(session=None):
        var = session.query(Variable).filter(Variable.key == "my_var").first()
        conn = session.query(Connection).filter(Connection.conn_id == "my_conn").first()
        return {"variable": var.val if var else None, "connection": conn}


    # Pattern 2: Bulk variable operations
    @provide_session
    def get_all_config_variables(prefix="config_", session=None):
        variables = session.query(Variable).filter(Variable.key.like(f"{prefix}%")).all()

        return {var.key: var.val for var in variables}


    # Pattern 3: Variable manipulation and updates
    @provide_session
    def update_config_variables(updates_dict, session=None):
        for key, value in updates_dict.items():
            var = session.query(Variable).filter(Variable.key == key).first()
            if var:
                var.val = value
            else:
                new_var = Variable(key=key, val=value)
                session.add(new_var)

        session.commit()
        return len(updates_dict)


    # Pattern 4: Complex connection queries
    @provide_session
    def find_database_connections(session=None):
        # Find all database-type connections
        db_connections = (
            session.query(Connection)
            .filter(
                or_(
                    Connection.conn_type == "postgres",
                    Connection.conn_type == "mysql",
                    Connection.conn_type == "sqlite",
                )
            )
            .all()
        )

        return {
            conn.conn_id: {
                "type": conn.conn_type,
                "host": conn.host,
                "port": conn.port,
                "schema": conn.schema,
                "login": conn.login,
                # Note: password not included for security
            }
            for conn in db_connections
        }


    # Pattern 5: Variable validation and cleanup
    @provide_session
    def cleanup_old_variables(days_old=30, session=None):
        from datetime import datetime, timedelta

        # Note: This assumes variables have a created_at field (may not exist in all versions)
        cutoff_date = datetime.now() - timedelta(days=days_old)

        # Find variables that haven't been accessed recently
        # This is a simplified example - actual implementation would be more complex
        old_vars = (
            session.query(Variable).filter(Variable.key.like("temp_%")).all()  # Only cleanup temporary variables
        )

        deleted_count = 0
        for var in old_vars:
            session.delete(var)
            deleted_count += 1

        session.commit()
        return deleted_count


    # Airflow 3 (recommended) - Using Task SDK API
    from airflow.sdk import BaseOperator
    import json


    class ConfigDataOperator(BaseOperator):
        """Get configuration data from variables and connections."""

        def __init__(self, variable_keys: list = None, connection_ids: list = None, **kwargs):
            super().__init__(**kwargs)
            self.variable_keys = variable_keys or []
            self.connection_ids = connection_ids or []

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            result = {"variables": {}, "connections": {}, "errors": []}

            # Get variables
            for var_key in self.variable_keys:
                try:
                    var_response = client.variables.get(var_key)
                    if hasattr(var_response, "value"):
                        result["variables"][var_key] = var_response.value
                    else:
                        result["variables"][var_key] = None
                        result["errors"].append(f"Variable {var_key} not found")
                except Exception as e:
                    result["errors"].append(f"Error getting variable {var_key}: {str(e)}")
                    result["variables"][var_key] = None

            # Get connections
            for conn_id in self.connection_ids:
                try:
                    conn_response = client.connections.get(conn_id)
                    if hasattr(conn_response, "conn_id"):
                        # Only include safe connection info (no passwords)
                        result["connections"][conn_id] = {
                            "conn_type": getattr(conn_response, "conn_type", None),
                            "host": getattr(conn_response, "host", None),
                            "port": getattr(conn_response, "port", None),
                            "schema": getattr(conn_response, "schema", None),
                            "login": getattr(conn_response, "login", None),
                        }
                    else:
                        result["connections"][conn_id] = None
                        result["errors"].append(f"Connection {conn_id} not found")
                except Exception as e:
                    result["errors"].append(f"Error getting connection {conn_id}: {str(e)}")
                    result["connections"][conn_id] = None

            return result


    class VariableManagerOperator(BaseOperator):
        """Manage Airflow variables through the API."""

        def __init__(
            self,
            operation: str,
            variable_key: str = None,
            variable_value: str = None,
            variable_description: str = None,
            **kwargs,
        ):
            super().__init__(**kwargs)
            self.operation = operation  # 'get', 'set', 'delete'
            self.variable_key = variable_key
            self.variable_value = variable_value
            self.variable_description = variable_description

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            if self.operation == "get":
                response = client.variables.get(self.variable_key)
                if hasattr(response, "value"):
                    return {
                        "key": self.variable_key,
                        "value": response.value,
                        "description": getattr(response, "description", None),
                    }
                else:
                    return {"key": self.variable_key, "value": None, "found": False}

            elif self.operation == "set":
                response = client.variables.set(
                    key=self.variable_key, value=self.variable_value, description=self.variable_description
                )
                return {
                    "operation": "set",
                    "key": self.variable_key,
                    "success": hasattr(response, "ok") and response.ok,
                }

            elif self.operation == "delete":
                response = client.variables.delete(key=self.variable_key)
                return {
                    "operation": "delete",
                    "key": self.variable_key,
                    "success": hasattr(response, "ok") and response.ok,
                }

            else:
                raise ValueError(f"Unsupported operation: {self.operation}")


    class ConfigurationValidatorOperator(BaseOperator):
        """Validate that required configuration is present."""

        def __init__(self, required_variables: list = None, required_connections: list = None, **kwargs):
            super().__init__(**kwargs)
            self.required_variables = required_variables or []
            self.required_connections = required_connections or []

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            validation_result = {"valid": True, "missing_variables": [], "missing_connections": [], "errors": []}

            # Check required variables
            for var_key in self.required_variables:
                try:
                    var_response = client.variables.get(var_key)
                    if not hasattr(var_response, "value") or var_response.value is None:
                        validation_result["missing_variables"].append(var_key)
                        validation_result["valid"] = False
                except Exception as e:
                    validation_result["missing_variables"].append(var_key)
                    validation_result["errors"].append(f"Error checking variable {var_key}: {str(e)}")
                    validation_result["valid"] = False

            # Check required connections
            for conn_id in self.required_connections:
                try:
                    conn_response = client.connections.get(conn_id)
                    if not hasattr(conn_response, "conn_id"):
                        validation_result["missing_connections"].append(conn_id)
                        validation_result["valid"] = False
                except Exception as e:
                    validation_result["missing_connections"].append(conn_id)
                    validation_result["errors"].append(f"Error checking connection {conn_id}: {str(e)}")
                    validation_result["valid"] = False

            if not validation_result["valid"]:
                error_msg = f"Configuration validation failed. Missing variables: {validation_result['missing_variables']}, Missing connections: {validation_result['missing_connections']}"
                self.log.error(error_msg)
                raise Exception(error_msg)

            self.log.info("Configuration validation passed")
            return validation_result

**Key Differences in Variable/Connection Access:**

1. **No Bulk Operations**: You cannot query multiple variables or connections in a single API call
2. **No Pattern Matching**: You cannot search for variables by pattern (like 'config_%')
3. **Individual API Calls**: Each variable or connection requires a separate API call
4. **Limited Metadata**: You may not have access to creation dates, modification history, etc.
5. **No Direct Database Manipulation**: You cannot perform bulk updates or complex queries
6. **Error Handling**: Each API call can fail independently, requiring robust error handling

**Advanced Variable/Connection Patterns:**

For scenarios requiring bulk operations or complex logic:

.. code-block:: python

    class BulkVariableOperator(BaseOperator):
        """Handle multiple variables efficiently."""

        def __init__(self, variable_operations: list, **kwargs):
            super().__init__(**kwargs)
            # variable_operations: [{'operation': 'get'|'set'|'delete', 'key': 'var_key', 'value': 'var_value'}]
            self.variable_operations = variable_operations

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            results = []
            errors = []

            for op in self.variable_operations:
                try:
                    if op["operation"] == "get":
                        response = client.variables.get(op["key"])
                        results.append(
                            {
                                "operation": "get",
                                "key": op["key"],
                                "value": getattr(response, "value", None),
                                "success": hasattr(response, "value"),
                            }
                        )

                    elif op["operation"] == "set":
                        response = client.variables.set(
                            key=op["key"], value=op.get("value"), description=op.get("description")
                        )
                        results.append(
                            {
                                "operation": "set",
                                "key": op["key"],
                                "success": hasattr(response, "ok") and response.ok,
                            }
                        )

                    elif op["operation"] == "delete":
                        response = client.variables.delete(key=op["key"])
                        results.append(
                            {
                                "operation": "delete",
                                "key": op["key"],
                                "success": hasattr(response, "ok") and response.ok,
                            }
                        )

                except Exception as e:
                    error_msg = f"Error in {op['operation']} operation for key {op['key']}: {str(e)}"
                    errors.append(error_msg)
                    self.log.error(error_msg)
                    results.append(
                        {"operation": op["operation"], "key": op["key"], "success": False, "error": str(e)}
                    )

            return {
                "results": results,
                "errors": errors,
                "total_operations": len(self.variable_operations),
                "successful_operations": len([r for r in results if r.get("success", False)]),
            }

**XCom Operations:**

.. code-block:: python

    # Airflow 2 (deprecated) - Various XCom access patterns
    from airflow.utils.session import provide_session, create_session
    from airflow.models import XCom
    from datetime import datetime, timedelta
    from sqlalchemy import func, and_, or_
    import json


    # Pattern 1: Simple XCom retrieval
    @provide_session
    def get_xcom_data(session=None):
        return (
            session.query(XCom)
            .filter(XCom.dag_id == "my_dag", XCom.task_id == "my_task", XCom.key == "my_key")
            .first()
        )


    # Pattern 2: Bulk XCom operations
    @provide_session
    def get_all_xcoms_for_run(dag_id, run_id, session=None):
        xcoms = session.query(XCom).filter(XCom.dag_id == dag_id, XCom.run_id == run_id).all()

        return {f"{xcom.task_id}_{xcom.key}": xcom.value for xcom in xcoms}


    # Pattern 3: XCom analysis and aggregation
    @provide_session
    def analyze_xcom_usage(dag_id, days_back=7, session=None):
        cutoff_date = datetime.now() - timedelta(days=days_back)

        # Get XCom statistics
        stats = (
            session.query(
                XCom.task_id,
                XCom.key,
                func.count(XCom.id).label("usage_count"),
                func.avg(func.length(XCom.value)).label("avg_size"),
            )
            .filter(XCom.dag_id == dag_id, XCom.timestamp >= cutoff_date)
            .group_by(XCom.task_id, XCom.key)
            .all()
        )

        return {
            f"{stat.task_id}_{stat.key}": {"usage_count": stat.usage_count, "avg_size": stat.avg_size}
            for stat in stats
        }


    # Pattern 4: Cross-task XCom dependencies
    @provide_session
    def find_xcom_dependencies(dag_id, session=None):
        # Find which tasks produce XComs that other tasks consume
        producers = (
            session.query(XCom.task_id.label("producer_task"), XCom.key.label("xcom_key"))
            .filter(XCom.dag_id == dag_id)
            .distinct()
            .all()
        )

        # This is a simplified example - actual dependency tracking would be more complex
        return {
            f"{prod.producer_task}_{prod.xcom_key}": {"producer": prod.producer_task, "key": prod.xcom_key}
            for prod in producers
        }


    # Pattern 5: XCom cleanup and maintenance
    @provide_session
    def cleanup_old_xcoms(dag_id, days_old=30, session=None):
        cutoff_date = datetime.now() - timedelta(days=days_old)

        deleted_count = session.query(XCom).filter(XCom.dag_id == dag_id, XCom.timestamp < cutoff_date).delete()

        session.commit()
        return deleted_count


    # Pattern 6: Large XCom handling
    @provide_session
    def get_large_xcoms(size_threshold_mb=10, session=None):
        size_threshold_bytes = size_threshold_mb * 1024 * 1024

        large_xcoms = session.query(XCom).filter(func.length(XCom.value) > size_threshold_bytes).all()

        return [
            {
                "dag_id": xcom.dag_id,
                "task_id": xcom.task_id,
                "key": xcom.key,
                "size_mb": len(xcom.value) / (1024 * 1024) if xcom.value else 0,
            }
            for xcom in large_xcoms
        ]


    # Airflow 3 (recommended) - Using Task SDK API
    from airflow.sdk import BaseOperator
    import json


    class XComDataOperator(BaseOperator):
        """Handle XCom operations using Task SDK API."""

        def __init__(
            self,
            operation: str,
            xcom_dag_id: str = None,
            xcom_task_id: str = None,
            xcom_key: str = None,
            xcom_value=None,
            **kwargs,
        ):
            super().__init__(**kwargs)
            self.operation = operation  # 'get', 'set', 'delete'
            self.xcom_dag_id = xcom_dag_id
            self.xcom_task_id = xcom_task_id
            self.xcom_key = xcom_key
            self.xcom_value = xcom_value

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # Use current context if not specified
            dag_id = self.xcom_dag_id or context["dag"].dag_id
            run_id = context["run_id"]
            task_id = self.xcom_task_id or context["task"].task_id
            key = self.xcom_key or "return_value"

            if self.operation == "get":
                xcom_response = client.xcoms.get(dag_id=dag_id, run_id=run_id, task_id=task_id, key=key)

                result = {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "task_id": task_id,
                    "key": key,
                    "value": getattr(xcom_response, "value", None),
                    "found": hasattr(xcom_response, "value") and xcom_response.value is not None,
                }

                return result

            elif self.operation == "set":
                response = client.xcoms.set(
                    dag_id=dag_id, run_id=run_id, task_id=task_id, key=key, value=self.xcom_value
                )

                return {
                    "operation": "set",
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "task_id": task_id,
                    "key": key,
                    "success": hasattr(response, "ok") and response.ok,
                }

            elif self.operation == "delete":
                response = client.xcoms.delete(dag_id=dag_id, run_id=run_id, task_id=task_id, key=key)

                return {
                    "operation": "delete",
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "task_id": task_id,
                    "key": key,
                    "success": hasattr(response, "ok") and response.ok,
                }

            else:
                raise ValueError(f"Unsupported operation: {self.operation}")


    class XComSequenceOperator(BaseOperator):
        """Handle XCom sequence operations (for mapped tasks)."""

        def __init__(self, source_dag_id: str, source_task_id: str, source_key: str = "return_value", **kwargs):
            super().__init__(**kwargs)
            self.source_dag_id = source_dag_id
            self.source_task_id = source_task_id
            self.source_key = source_key

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # Get the count of XCom values (for mapped tasks)
            try:
                count_response = client.xcoms.head(
                    dag_id=self.source_dag_id,
                    run_id=context["run_id"],
                    task_id=self.source_task_id,
                    key=self.source_key,
                )

                total_items = count_response.len
                self.log.info(f"Found {total_items} XCom items")

                # Get a slice of the sequence
                slice_response = client.xcoms.get_sequence_slice(
                    dag_id=self.source_dag_id,
                    run_id=context["run_id"],
                    task_id=self.source_task_id,
                    key=self.source_key,
                    start=0,
                    stop=min(10, total_items),  # Get first 10 items
                )

                return {
                    "total_items": total_items,
                    "sample_items": slice_response.values,
                    "source_task": self.source_task_id,
                }

            except Exception as e:
                self.log.error(f"Error accessing XCom sequence: {e}")
                return {"total_items": 0, "sample_items": [], "error": str(e)}


    class XComAnalysisOperator(BaseOperator):
        """Analyze XCom usage patterns (limited compared to Airflow 2)."""

        def __init__(self, analysis_tasks: list, **kwargs):
            super().__init__(**kwargs)
            self.analysis_tasks = analysis_tasks  # List of task_ids to analyze

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            analysis_results = {}

            for task_id in self.analysis_tasks:
                try:
                    # Try to get the default return value
                    xcom_response = client.xcoms.get(
                        dag_id=context["dag"].dag_id,
                        run_id=context["run_id"],
                        task_id=task_id,
                        key="return_value",
                    )

                    if hasattr(xcom_response, "value") and xcom_response.value is not None:
                        value = xcom_response.value
                        analysis_results[task_id] = {
                            "has_xcom": True,
                            "value_type": type(value).__name__,
                            "value_size": len(str(value)) if value is not None else 0,
                            "is_json": self._is_json(value),
                        }
                    else:
                        analysis_results[task_id] = {
                            "has_xcom": False,
                            "value_type": None,
                            "value_size": 0,
                            "is_json": False,
                        }

                except Exception as e:
                    analysis_results[task_id] = {"has_xcom": False, "error": str(e)}

            return analysis_results

        def _is_json(self, value):
            """Check if a value is JSON serializable."""
            try:
                json.dumps(value)
                return True
            except (TypeError, ValueError):
                return False

**Key Limitations in Airflow 3 XCom Operations:**

1. **No Bulk Retrieval**: You cannot get all XComs for a DAG run in a single API call
2. **No Cross-Run Analysis**: You cannot easily analyze XCom patterns across multiple DAG runs
3. **Limited Metadata**: You cannot access XCom timestamps, sizes, or other metadata directly
4. **No Pattern Matching**: You cannot search for XComs by key patterns
5. **Individual API Calls**: Each XCom requires a separate API call, which can be inefficient for bulk operations
6. **No Aggregation**: You cannot perform SQL-like aggregations on XCom data

**Workarounds for Complex XCom Analysis:**

.. code-block:: python

    class BulkXComAnalysisOperator(BaseOperator):
        """Perform bulk XCom analysis using multiple API calls."""

        def __init__(self, target_tasks: list, analysis_keys: list = None, **kwargs):
            super().__init__(**kwargs)
            self.target_tasks = target_tasks
            self.analysis_keys = analysis_keys or ["return_value"]

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            bulk_results = {}

            for task_id in self.target_tasks:
                task_results = {}

                for key in self.analysis_keys:
                    try:
                        xcom_response = client.xcoms.get(
                            dag_id=context["dag"].dag_id, run_id=context["run_id"], task_id=task_id, key=key
                        )

                        if hasattr(xcom_response, "value"):
                            task_results[key] = {
                                "found": True,
                                "value": xcom_response.value,
                                "type": type(xcom_response.value).__name__,
                            }
                        else:
                            task_results[key] = {"found": False}

                    except Exception as e:
                        task_results[key] = {"found": False, "error": str(e)}

                bulk_results[task_id] = task_results

            # Perform local analysis
            summary = {
                "total_tasks_analyzed": len(self.target_tasks),
                "tasks_with_xcoms": len(
                    [t for t, r in bulk_results.items() if any(k.get("found", False) for k in r.values())]
                ),
                "total_xcoms_found": sum(
                    len([k for k in r.values() if k.get("found", False)]) for r in bulk_results.values()
                ),
                "detailed_results": bulk_results,
            }

            return summary

Strategy 2: Explicit Database Session (Advanced Use Cases)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. danger::
   **CRITICAL WARNING**: This approach completely bypasses Airflow 3's security model and architectural improvements.

   **Use this approach ONLY when:**

   - The Task SDK API absolutely cannot provide the required functionality
   - You have thoroughly evaluated all alternatives
   - You understand and accept the security risks
   - You have proper database permissions and monitoring in place
   - You are prepared to maintain this code as Airflow evolves

   **Risks include:**

   - **Security vulnerabilities**: Direct database access can expose sensitive data
   - **Data corruption**: Incorrect queries can corrupt the Airflow metadata
   - **Performance impact**: Unoptimized queries can degrade Airflow performance
   - **Upgrade compatibility**: Internal schema changes may break your code
   - **Monitoring blind spots**: Database operations won't appear in Airflow's audit logs

**When You Might Need This Approach:**

1. **Complex Analytics**: Multi-table joins and aggregations not supported by the API
2. **Data Migration**: Moving data between Airflow instances
3. **Custom Reporting**: Generating reports that require complex SQL
4. **Performance Optimization**: Bulk operations that would be too slow via API
5. **Legacy Integration**: Existing systems that depend on direct database access

**Detailed Implementation Examples:**

.. code-block:: python

    from airflow.sdk import BaseOperator
    from airflow.configuration import conf
    from sqlalchemy import create_engine, text, MetaData, Table
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.exc import SQLAlchemyError
    import logging
    from datetime import datetime, timedelta
    from typing import List, Dict, Any, Optional


    class SecureAdvancedDatabaseOperator(BaseOperator):
        """
        DANGER: This bypasses Airflow 3's security model.

        This implementation includes additional safety measures:
        - Read-only by default
        - Query validation
        - Connection pooling
        - Detailed error handling
        - Audit logging
        """

        def __init__(
            self,
            query: str,
            read_only: bool = True,
            query_timeout: int = 300,
            max_rows: int = 10000,
            validate_query: bool = True,
            **kwargs,
        ):
            super().__init__(**kwargs)
            self.query = query.strip()
            self.read_only = read_only
            self.query_timeout = query_timeout
            self.max_rows = max_rows
            self.validate_query = validate_query

            # Validate query safety
            if self.validate_query:
                self._validate_query_safety()

        def _validate_query_safety(self):
            """Validate that the query is safe to execute."""
            query_upper = self.query.upper().strip()

            # Check for dangerous operations
            dangerous_keywords = [
                "DROP",
                "DELETE",
                "TRUNCATE",
                "ALTER",
                "CREATE",
                "INSERT",
                "UPDATE",
                "GRANT",
                "REVOKE",
            ]

            if self.read_only:
                for keyword in dangerous_keywords:
                    if keyword in query_upper:
                        raise ValueError(f"Dangerous keyword '{keyword}' found in read-only query: {self.query}")

            # Check for suspicious patterns
            suspicious_patterns = ["--", "/*", "*/", ";"]
            for pattern in suspicious_patterns:
                if pattern in self.query:
                    self.log.warning(f"Suspicious pattern '{pattern}' found in query")

        def _create_secure_engine(self):
            """Create a database engine with security-focused settings."""
            sql_alchemy_conn = conf.get("database", "sql_alchemy_conn")

            # Security-focused engine configuration
            engine_kwargs = {
                "pool_pre_ping": True,
                "pool_recycle": 3600,
                "pool_size": 1,  # Limit connection pool size
                "max_overflow": 0,  # No overflow connections
                "echo": False,  # Don't log SQL (security)
                "connect_args": {"connect_timeout": 30, "application_name": f"airflow_task_{self.task_id}"},
            }

            # Add read-only settings for PostgreSQL
            if sql_alchemy_conn.startswith("postgresql"):
                engine_kwargs["connect_args"]["options"] = "-c default_transaction_read_only=on"

            return create_engine(sql_alchemy_conn, **engine_kwargs)

        def execute(self, context):
            # Audit logging
            self.log.warning(
                f"SECURITY AUDIT: Direct database access initiated by task {self.task_id} "
                f"in DAG {context['dag'].dag_id}. Query: {self.query[:100]}..."
            )

            engine = None
            session = None

            try:
                # Create secure engine
                engine = self._create_secure_engine()
                Session = sessionmaker(bind=engine)
                session = Session()

                # Set query timeout
                if hasattr(session.bind.dialect, "name"):
                    if session.bind.dialect.name == "postgresql":
                        session.execute(text(f"SET statement_timeout = {self.query_timeout * 1000}"))
                    elif session.bind.dialect.name == "mysql":
                        session.execute(text(f"SET SESSION max_execution_time = {self.query_timeout * 1000}"))

                # Execute query with safety measures
                if self.read_only:
                    result = session.execute(text(self.query))

                    # Limit result size
                    rows = result.fetchmany(self.max_rows)
                    if len(rows) == self.max_rows:
                        self.log.warning(f"Query result truncated to {self.max_rows} rows")

                    # Convert to dictionaries
                    data = [dict(row._mapping) for row in rows]

                    self.log.info(f"Query returned {len(data)} rows")
                    return data

                else:
                    # Write operations (EXTREMELY DANGEROUS)
                    self.log.error(
                        f"CRITICAL: Write operation attempted by task {self.task_id}. " f"Query: {self.query}"
                    )

                    if not self.read_only:
                        # Additional confirmation required for write operations
                        confirmation_var = f"ALLOW_WRITE_{self.task_id.upper()}"

                        # This would need to be set as an Airflow variable
                        # client = context["task_instance"].task_sdk_client
                        # confirmation = client.variables.get(confirmation_var)

                        # For now, we'll require explicit override
                        raise RuntimeError(
                            f"Write operations require explicit confirmation. "
                            f"Set Airflow variable '{confirmation_var}' to 'CONFIRMED' to proceed."
                        )

                    result = session.execute(text(self.query))
                    session.commit()

                    affected_rows = result.rowcount
                    self.log.warning(f"Write operation affected {affected_rows} rows")
                    return affected_rows

            except SQLAlchemyError as e:
                if session:
                    session.rollback()
                self.log.error(f"Database operation failed: {e}")
                raise
            except Exception as e:
                if session:
                    session.rollback()
                self.log.error(f"Unexpected error in database operation: {e}")
                raise
            finally:
                if session:
                    session.close()
                if engine:
                    engine.dispose()


    # Specific use case examples
    class ComplexAnalyticsOperator(SecureAdvancedDatabaseOperator):
        """Perform complex analytics that require multi-table joins."""

        def __init__(self, analysis_type: str = "performance", days_back: int = 7, **kwargs):
            self.analysis_type = analysis_type
            self.days_back = days_back

            # Generate query based on analysis type
            if analysis_type == "performance":
                query = f"""
                SELECT
                    dr.dag_id,
                    COUNT(DISTINCT dr.id) as total_runs,
                    COUNT(DISTINCT CASE WHEN dr.state = 'success' THEN dr.id END) as successful_runs,
                    COUNT(DISTINCT CASE WHEN dr.state = 'failed' THEN dr.id END) as failed_runs,
                    AVG(EXTRACT(EPOCH FROM (dr.end_date - dr.start_date))) as avg_duration_seconds,
                    COUNT(DISTINCT ti.id) as total_task_instances,
                    COUNT(DISTINCT CASE WHEN ti.state = 'success' THEN ti.id END) as successful_tasks,
                    COUNT(DISTINCT CASE WHEN ti.state = 'failed' THEN ti.id END) as failed_tasks
                FROM dag_run dr
                LEFT JOIN task_instance ti ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
                WHERE dr.start_date >= NOW() - INTERVAL '{days_back} days'
                GROUP BY dr.dag_id
                ORDER BY total_runs DESC
                """

            elif analysis_type == "resource_usage":
                query = f"""
                SELECT
                    ti.dag_id,
                    ti.task_id,
                    COUNT(*) as execution_count,
                    AVG(EXTRACT(EPOCH FROM (ti.end_date - ti.start_date))) as avg_duration,
                    MAX(EXTRACT(EPOCH FROM (ti.end_date - ti.start_date))) as max_duration,
                    COUNT(CASE WHEN ti.state = 'failed' THEN 1 END) as failure_count
                FROM task_instance ti
                WHERE ti.start_date >= NOW() - INTERVAL '{days_back} days'
                AND ti.end_date IS NOT NULL
                GROUP BY ti.dag_id, ti.task_id
                HAVING COUNT(*) > 1
                ORDER BY avg_duration DESC
                """

            elif analysis_type == "xcom_usage":
                query = f"""
                SELECT
                    x.dag_id,
                    x.task_id,
                    x.key,
                    COUNT(*) as usage_count,
                    AVG(LENGTH(x.value)) as avg_size_bytes,
                    MAX(LENGTH(x.value)) as max_size_bytes
                FROM xcom x
                WHERE x.timestamp >= NOW() - INTERVAL '{days_back} days'
                GROUP BY x.dag_id, x.task_id, x.key
                ORDER BY avg_size_bytes DESC
                """

            else:
                raise ValueError(f"Unsupported analysis type: {analysis_type}")

            super().__init__(query=query, read_only=True, **kwargs)


    class DataMigrationOperator(SecureAdvancedDatabaseOperator):
        """Migrate data between Airflow instances (read-only source operations)."""

        def __init__(self, migration_type: str, filter_conditions: Dict[str, Any] = None, **kwargs):
            self.migration_type = migration_type
            self.filter_conditions = filter_conditions or {}

            # Build query based on migration type
            if migration_type == "dag_runs":
                base_query = """
                SELECT
                    dag_id, run_id, state, execution_date, start_date, end_date,
                    external_trigger, run_type, conf, data_interval_start, data_interval_end
                FROM dag_run
                """

            elif migration_type == "task_instances":
                base_query = """
                SELECT
                    dag_id, task_id, run_id, map_index, start_date, end_date,
                    duration, state, try_number, max_tries, hostname, unixname,
                    job_id, pool, pool_slots, queue, priority_weight, operator,
                    queued_dttm, pid, executor_config
                FROM task_instance
                """

            elif migration_type == "variables":
                base_query = """
                SELECT key, val, description
                FROM variable
                """

            elif migration_type == "connections":
                base_query = """
                SELECT conn_id, conn_type, description, host, schema, login, port, extra
                FROM connection
                """

            else:
                raise ValueError(f"Unsupported migration type: {migration_type}")

            # Add filter conditions
            where_clauses = []
            for field, value in self.filter_conditions.items():
                if isinstance(value, list):
                    placeholders = ",".join([f"'{v}'" for v in value])
                    where_clauses.append(f"{field} IN ({placeholders})")
                else:
                    where_clauses.append(f"{field} = '{value}'")

            if where_clauses:
                query = base_query + " WHERE " + " AND ".join(where_clauses)
            else:
                query = base_query

            super().__init__(query=query, read_only=True, **kwargs)


    class DatabaseMaintenanceOperator(SecureAdvancedDatabaseOperator):
        """Perform database maintenance operations (READ-ONLY analysis)."""

        def __init__(self, maintenance_type: str = "analyze_size", **kwargs):
            if maintenance_type == "analyze_size":
                query = """
                SELECT
                    schemaname,
                    tablename,
                    attname,
                    n_distinct,
                    correlation
                FROM pg_stats
                WHERE schemaname = 'public'
                ORDER BY tablename, attname
                """

            elif maintenance_type == "check_indexes":
                query = """
                SELECT
                    schemaname,
                    tablename,
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE schemaname = 'public'
                ORDER BY tablename, indexname
                """

            elif maintenance_type == "analyze_performance":
                query = """
                SELECT
                    query,
                    calls,
                    total_time,
                    mean_time,
                    rows
                FROM pg_stat_statements
                WHERE query LIKE '%task_instance%' OR query LIKE '%dag_run%'
                ORDER BY total_time DESC
                LIMIT 20
                """

            else:
                raise ValueError(f"Unsupported maintenance type: {maintenance_type}")

            super().__init__(query=query, read_only=True, **kwargs)

**Critical Security Considerations:**

1. **Database Permissions**: Ensure the Airflow database user has minimal required permissions
2. **Query Validation**: Always validate queries before execution
3. **Audit Logging**: Log all direct database access for security audits
4. **Connection Limits**: Limit the number of database connections
5. **Query Timeouts**: Set appropriate timeouts to prevent long-running queries
6. **Result Size Limits**: Limit the size of query results to prevent memory issues
7. **Read-Only by Default**: Default to read-only operations unless absolutely necessary
8. **Monitoring**: Monitor database performance impact of direct queries

**Alternative: Using Database Views for Safe Access**

A safer alternative to direct table access is creating database views:

.. code-block:: sql

    -- Create read-only views for common analytics needs
    CREATE VIEW dag_run_summary AS
    SELECT
        dag_id,
        DATE(start_date) as run_date,
        COUNT(*) as total_runs,
        COUNT(CASE WHEN state = 'success' THEN 1 END) as successful_runs,
        COUNT(CASE WHEN state = 'failed' THEN 1 END) as failed_runs
    FROM dag_run
    GROUP BY dag_id, DATE(start_date);

    CREATE VIEW task_performance_summary AS
    SELECT
        dag_id,
        task_id,
        COUNT(*) as execution_count,
        AVG(duration) as avg_duration,
        COUNT(CASE WHEN state = 'failed' THEN 1 END) as failure_count
    FROM task_instance
    WHERE start_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY dag_id, task_id;

.. code-block:: python

    class DatabaseViewOperator(SecureAdvancedDatabaseOperator):
        """Access data through pre-created database views for safety."""

        def __init__(self, view_name: str, filters: Dict[str, Any] = None, **kwargs):
            # Only allow access to predefined safe views
            allowed_views = ["dag_run_summary", "task_performance_summary", "xcom_usage_summary"]

            if view_name not in allowed_views:
                raise ValueError(f"View {view_name} not in allowed list: {allowed_views}")

            query = f"SELECT * FROM {view_name}"

            if filters:
                where_clauses = []
                for field, value in filters.items():
                    where_clauses.append(f"{field} = '{value}'")
                query += " WHERE " + " AND ".join(where_clauses)

            super().__init__(query=query, read_only=True, **kwargs)

Strategy 3: External Database for Custom Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For storing and accessing custom application data, consider using a separate database:

.. code-block:: python

    from airflow.sdk import BaseOperator
    from airflow.sdk import Connection
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker


    class ExternalDatabaseOperator(BaseOperator):
        def __init__(self, conn_id: str, **kwargs):
            super().__init__(**kwargs)
            self.conn_id = conn_id

        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            # Get connection details
            conn_response = client.connections.get(self.conn_id)
            if not hasattr(conn_response, "conn_id"):
                raise ValueError(f"Connection {self.conn_id} not found")

            # Build connection string
            conn_uri = f"{conn_response.conn_type}://{conn_response.login}:{conn_response.password}@{conn_response.host}:{conn_response.port}/{conn_response.schema}"

            # Use external database
            engine = create_engine(conn_uri)
            Session = sessionmaker(bind=engine)

            with Session() as session:
                # Your custom database operations
                result = session.execute("SELECT * FROM my_custom_table")
                return result.fetchall()

Migration Patterns
------------------

Here are all the major migration patterns organized by category:

**Session Management Patterns:**

.. list-table::
   :header-rows: 1
   :widths: 50, 50

   * - **Airflow 2 Pattern**
     - **Airflow 3 Migration**
   * - ``@provide_session`` decorator
     - Use ``context["task_instance"].task_sdk_client``
   * - ``create_session()`` context manager
     - Use Task SDK API methods
   * - ``settings.Session()`` direct access
     - Use Task SDK API methods or explicit session (advanced)
   * - ``from airflow.utils.session import NEW_SESSION``
     - Remove import, use Task SDK client

**Task Instance Patterns:**

.. list-table::
   :header-rows: 1
   :widths: 50, 50

   * - **Airflow 2 Pattern**
     - **Airflow 3 Migration**
   * - ``session.query(TaskInstance).count()``
     - ``client.task_instances.get_count(dag_id, states=[...])``
   * - ``session.query(TaskInstance).filter(...)``
     - ``client.task_instances.get_task_states(dag_id, ...)``
   * - ``TaskInstance.dag_id == 'my_dag'``
     - ``dag_id='my_dag'`` parameter in API calls
   * - ``TaskInstance.state == 'success'``
     - ``states=['success']`` parameter in API calls
   * - ``TaskInstance.start_date >= date``
     - ``logical_dates=[date]`` parameter in API calls
   * - ``session.query(TaskInstance, DagRun).join(...)``
     - Multiple API calls + local processing

**DAG Run Patterns:**

.. list-table::
   :header-rows: 1
   :widths: 50, 50

   * - **Airflow 2 Pattern**
     - **Airflow 3 Migration**
   * - ``session.query(DagRun).count()``
     - ``client.dag_runs.get_count(dag_id, states=[...])``
   * - ``session.query(DagRun).filter(...)``
     - ``client.dag_runs.get_count()`` with parameters
   * - ``DagRun.state == 'success'``
     - ``states=['success']`` parameter
   * - ``DagRun.start_date >= date``
     - ``logical_dates=[date]`` parameter
   * - ``session.query(DagRun).order_by(...)``
     - Use ``client.dag_runs.get_previous()`` for chronological access
   * - ``DagRun.execution_date``
     - ``logical_date`` in API calls

**Variable Patterns:**

.. list-table::
   :header-rows: 1
   :widths: 50, 50

   * - **Airflow 2 Pattern**
     - **Airflow 3 Migration**
   * - ``session.query(Variable).filter(Variable.key == 'x')``
     - ``client.variables.get('x')``
   * - ``Variable.set(key, value)``
     - ``client.variables.set(key, value)``
   * - ``Variable.get(key)``
     - ``client.variables.get(key)``
   * - ``Variable.delete(key)``
     - ``client.variables.delete(key)``
   * - ``session.query(Variable).filter(Variable.key.like('prefix%'))``
     - Multiple ``client.variables.get()`` calls
   * - ``session.add(Variable(key=k, val=v))``
     - ``client.variables.set(key=k, value=v)``

**Connection Patterns:**

.. list-table::
   :header-rows: 1
   :widths: 50, 50

   * - **Airflow 2 Pattern**
     - **Airflow 3 Migration**
   * - ``session.query(Connection).filter(Connection.conn_id == 'x')``
     - ``client.connections.get('x')``
   * - ``Connection.get_connection_from_secrets(conn_id)``
     - ``client.connections.get(conn_id)``
   * - ``session.query(Connection).filter(Connection.conn_type == 'postgres')``
     - Multiple ``client.connections.get()`` calls
   * - ``connection.host``, ``connection.port``
     - Access via ``conn_response.host``, ``conn_response.port``

**XCom Patterns:**

.. list-table::
   :header-rows: 1
   :widths: 50, 50

   * - **Airflow 2 Pattern**
     - **Airflow 3 Migration**
   * - ``session.query(XCom).filter(...)``
     - ``client.xcoms.get(dag_id, run_id, task_id, key)``
   * - ``XCom.set(key, value, task_id, dag_id)``
     - ``client.xcoms.set(dag_id, run_id, task_id, key, value)``
   * - ``XCom.get_one(key, task_id, dag_id)``
     - ``client.xcoms.get(dag_id, run_id, task_id, key)``
   * - ``XCom.delete(key, task_id, dag_id)``
     - ``client.xcoms.delete(dag_id, run_id, task_id, key)``
   * - ``session.query(XCom).filter(XCom.dag_id == 'x').all()``
     - Multiple ``client.xcoms.get()`` calls
   * - ``XCom.get_many(dag_id, run_id)``
     - Multiple ``client.xcoms.get()`` calls for different keys

**Import Statement Patterns:**

.. list-table::
   :header-rows: 1
   :widths: 50, 50

   * - **Airflow 2 Pattern**
     - **Airflow 3 Migration**
   * - ``from airflow.models import TaskInstance``
     - Remove import, use API client
   * - ``from airflow.models import DagRun``
     - Remove import, use API client
   * - ``from airflow.models import Variable``
     - Remove import, use API client
   * - ``from airflow.models import Connection``
     - Remove import, use API client
   * - ``from airflow.models import XCom``
     - Remove import, use API client
   * - ``from airflow import settings``
     - Remove import, use API client
   * - ``from airflow.utils.session import provide_session``
     - Remove import, use API client
   * - ``from airflow.utils.session import create_session``
     - Remove import, use API client

**Complex Query Patterns:**

.. list-table::
   :header-rows: 1
   :widths: 50, 50

   * - **Airflow 2 Pattern**
     - **Airflow 3 Migration**
   * - ``session.query().join().filter()``
     - Multiple API calls + local joins
   * - ``session.query().group_by().having()``
     - API calls + local aggregation
   * - ``session.query().order_by().limit()``
     - API calls + local sorting/limiting
   * - ``func.count()``, ``func.avg()``, ``func.sum()``
     - Local calculation after API calls
   * - ``session.execute(text('SELECT ...'))``
     - Explicit database session (advanced) or redesign
   * - ``session.bulk_insert_mappings()``
     - Multiple individual API calls

**Error Handling Patterns:**

.. list-table::
   :header-rows: 1
   :widths: 50, 50

   * - **Airflow 2 Pattern**
     - **Airflow 3 Migration**
   * - ``session.rollback()``
     - Handle API errors individually
   * - ``session.commit()``
     - API calls auto-commit
   * - ``try/except SQLAlchemyError``
     - ``try/except`` API client exceptions
   * - ``session.close()``
     - Not needed with API client
   * - ``engine.dispose()``
     - Not needed with API client

Best Practices
--------------

1. **Prefer Task SDK API**: Always try to use the Task SDK API methods first
2. **Minimize Database Access**: Review if database access is actually necessary
3. **Use Read-Only Operations**: When using explicit sessions, prefer read-only operations
4. **Handle Errors Gracefully**: Always include proper error handling and session cleanup
5. **Test Thoroughly**: Test all database access code in development environments
6. **Document Deviations**: Clearly document any use of explicit database sessions
7. **Plan for Future Changes**: Be prepared to update code as the Task SDK API evolves

Error Handling
--------------

When migrating, ensure proper error handling:

.. code-block:: python

    from airflow.sdk import BaseOperator
    from airflow.sdk.exceptions import AirflowException


    class RobustDatabaseOperator(BaseOperator):
        def execute(self, context):
            client = context["task_instance"].task_sdk_client

            try:
                # Task SDK API call
                result = client.task_instances.get_count(dag_id=self.dag_id, states=["success"])
                return result.count

            except Exception as e:
                self.log.error(f"Failed to get task count: {e}")
                # Decide whether to fail the task or return a default value
                if self.fail_on_error:
                    raise AirflowException(f"Database operation failed: {e}")
                else:
                    return 0  # Default value

Testing Your Migration
----------------------

Test your migrated code thoroughly:

.. code-block:: python

    import pytest
    from unittest.mock import Mock, patch
    from airflow.sdk.api.client import Client


    def test_migrated_operator():
        # Mock the Task SDK client
        mock_client = Mock(spec=Client)
        mock_client.task_instances.get_count.return_value = Mock(count=5)

        # Mock context
        mock_context = {
            "task_instance": Mock(task_sdk_client=mock_client),
            "logical_date": "2023-01-01T00:00:00Z",
        }

        # Test your operator
        operator = TaskCountOperator(task_id="test")
        result = operator.execute(mock_context)

        assert result == 5
        mock_client.task_instances.get_count.assert_called_once()

Troubleshooting
---------------

**Common Issues and Detailed Solutions:**

1. **"task_sdk_client not found in context"**

   **Symptoms:**

   .. code-block:: text

       KeyError: 'task_instance' not found in context
       # or
       AttributeError: 'TaskInstance' object has no attribute 'task_sdk_client'

   **Root Causes:**

   - Running on Airflow 2.x instead of Airflow 3.x
   - Using legacy task execution methods
   - Incorrect context access patterns
   - Task not executed through proper Airflow 3 mechanisms

   **Solutions:**

   .. code-block:: python

       # Check Airflow version first
       import airflow

       print(f"Airflow version: {airflow.__version__}")


       # Correct way to access the client
       def execute(self, context):
           # Verify context structure
           self.log.info(f"Available context keys: {list(context.keys())}")

           # Safe access pattern
           task_instance = context.get("task_instance")
           if not task_instance:
               raise ValueError("task_instance not found in context")

           client = getattr(task_instance, "task_sdk_client", None)
           if not client:
               raise ValueError("task_sdk_client not available on task_instance")

           return client

   **Verification Steps:**

   - Confirm Airflow 3.x installation: ``airflow version``
   - Check task execution logs for Airflow 3 specific messages
   - Verify DAG is using ``airflow.sdk`` imports
   - Ensure proper task execution environment

2. **"API endpoint not found" or "404 Not Found"**

   **Symptoms:**

   .. code-block:: text

       httpx.HTTPStatusError: Client error '404 Not Found' for url 'http://localhost:8080/api/v2/task-instances/count'

   **Root Causes:**

   - Incorrect API endpoint URLs
   - API server not running or misconfigured
   - Version mismatch between client and server
   - Wrong API version being used

   **Solutions:**

   .. code-block:: python

       # Debug API connectivity
       class APIDebugOperator(BaseOperator):
           def execute(self, context):
               client = context["task_instance"].task_sdk_client

               # Check client configuration
               self.log.info(f"Client base URL: {client.base_url}")
               self.log.info(f"Client headers: {client.headers}")

               # Test basic connectivity
               try:
                   # Try a simple API call
                   result = client.task_instances.get_count(dag_id=context["dag"].dag_id)
                   self.log.info(f"API test successful: {result}")
               except Exception as e:
                   self.log.error(f"API test failed: {e}")
                   # Log detailed error information
                   if hasattr(e, "response"):
                       self.log.error(f"Response status: {e.response.status_code}")
                       self.log.error(f"Response headers: {e.response.headers}")
                       self.log.error(f"Response content: {e.response.text}")
                   raise

   **Verification Steps:**

   - Check API server status: ``curl http://localhost:8080/health``
   - Verify API server configuration in ``airflow.cfg``
   - Check firewall and network connectivity
   - Confirm API version compatibility

3. **"Connection refused to API server"**

   **Symptoms:**

   .. code-block:: text

       httpx.ConnectError: [Errno 111] Connection refused

   **Root Causes:**

   - API server not running
   - Wrong host/port configuration
   - Network connectivity issues
   - Firewall blocking connections

   **Solutions:**

   .. code-block:: bash

       # Check if API server is running
       ps aux | grep "airflow api-server"

       # Check port availability
       netstat -tlnp | grep :8080

       # Test connectivity
       telnet localhost 8080

       # Check Airflow configuration
       airflow config get-value webserver web_server_host
       airflow config get-value webserver web_server_port

   **Configuration Check:**

   .. code-block:: python

       # Check API configuration in your DAG
       class ConnectivityTestOperator(BaseOperator):
           def execute(self, context):
               from airflow.configuration import conf

               # Log configuration details
               api_host = conf.get("webserver", "web_server_host", fallback="localhost")
               api_port = conf.get("webserver", "web_server_port", fallback="8080")

               self.log.info(f"Expected API server: {api_host}:{api_port}")

               # Test network connectivity
               import socket

               try:
                   sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                   sock.settimeout(5)
                   result = sock.connect_ex((api_host, int(api_port)))
                   sock.close()

                   if result == 0:
                       self.log.info("Network connectivity OK")
                   else:
                       self.log.error(f"Cannot connect to {api_host}:{api_port}")
               except Exception as e:
                   self.log.error(f"Network test failed: {e}")

4. **"Permission denied for database access"**

   **Symptoms:**

   .. code-block:: text

       sqlalchemy.exc.ProgrammingError: (psycopg2.errors.InsufficientPrivilege) permission denied for table task_instance

   **Root Causes:**

   - Database user lacks required permissions
   - Read-only database configuration
   - Security policies blocking access
   - Incorrect connection string

   **Solutions:**

   .. code-block:: python

       # Check database permissions
       class DatabasePermissionTestOperator(BaseOperator):
           def execute(self, context):
               from airflow.configuration import conf
               from sqlalchemy import create_engine, text

               sql_alchemy_conn = conf.get("database", "sql_alchemy_conn")
               engine = create_engine(sql_alchemy_conn)

               try:
                   with engine.connect() as conn:
                       # Test basic connectivity
                       result = conn.execute(text("SELECT 1"))
                       self.log.info("Basic database connectivity OK")

                       # Test table access
                       try:
                           result = conn.execute(text("SELECT COUNT(*) FROM dag_run LIMIT 1"))
                           self.log.info("Read access to dag_run table OK")
                       except Exception as e:
                           self.log.error(f"Cannot read from dag_run table: {e}")

                       # Check current user and permissions
                       try:
                           result = conn.execute(text("SELECT current_user, current_database()"))
                           user_info = result.fetchone()
                           self.log.info(f"Database user: {user_info[0]}, Database: {user_info[1]}")
                       except Exception as e:
                           self.log.warning(f"Cannot get user info: {e}")

               except Exception as e:
                   self.log.error(f"Database connection failed: {e}")
                   raise

5. **"Task SDK API method not available"**

   **Symptoms:**

   .. code-block:: text

       AttributeError: 'TaskInstanceOperations' object has no attribute 'get_detailed_info'

   **Root Causes:**

   - Using non-existent API methods
   - Version mismatch between documentation and implementation
   - Typos in method names

   **Solutions:**

   .. code-block:: python

       # Discover available API methods
       class APIDiscoveryOperator(BaseOperator):
           def execute(self, context):
               client = context["task_instance"].task_sdk_client

               # List available operations
               operations = {
                   "task_instances": dir(client.task_instances),
                   "dag_runs": dir(client.dag_runs),
                   "variables": dir(client.variables),
                   "connections": dir(client.connections),
                   "xcoms": dir(client.xcoms),
               }

               for op_name, methods in operations.items():
                   # Filter out private methods
                   public_methods = [m for m in methods if not m.startswith("_")]
                   self.log.info(f"{op_name} available methods: {public_methods}")

               return operations

6. **"XCom value too large" or "Serialization errors"**

   **Symptoms:**

   .. code-block:: text

       ValueError: XCom value is too large (>1MB)
       # or
       TypeError: Object of type 'datetime' is not JSON serializable

   **Solutions:**

   .. code-block:: python

       class SafeXComOperator(BaseOperator):
           def execute(self, context):
               client = context["task_instance"].task_sdk_client

               # Handle large data
               large_data = self.get_large_data()

               if len(str(large_data)) > 1000000:  # 1MB limit
                   # Store in external system instead
                   storage_key = self.store_in_external_system(large_data)
                   xcom_value = {"storage_key": storage_key, "type": "external_reference"}
               else:
                   xcom_value = large_data

               # Handle serialization
               try:
                   import json

                   json.dumps(xcom_value)  # Test serialization
               except TypeError as e:
                   self.log.warning(f"Serialization issue: {e}")
                   # Convert to serializable format
                   xcom_value = str(xcom_value)

               # Set XCom safely
               client.xcoms.set(
                   dag_id=context["dag"].dag_id,
                   run_id=context["run_id"],
                   task_id=context["task"].task_id,
                   key="result",
                   value=xcom_value,
               )

7. **"Performance issues with multiple API calls"**

   **Symptoms:**

   - Slow task execution
   - High network latency
   - API rate limiting errors

   **Solutions:**

   .. code-block:: python

       class OptimizedBulkOperator(BaseOperator):
           """Optimize multiple API calls for better performance."""

           def execute(self, context):
               client = context["task_instance"].task_sdk_client

               # Batch operations where possible
               results = {}

               # Use concurrent execution for independent calls
               import concurrent.futures
               import time

               def get_dag_count(dag_id):
                   return dag_id, client.dag_runs.get_count(dag_id=dag_id).count

               dag_ids = ["dag1", "dag2", "dag3"]

               # Sequential approach (slow)
               start_time = time.time()
               sequential_results = {}
               for dag_id in dag_ids:
                   sequential_results[dag_id] = client.dag_runs.get_count(dag_id=dag_id).count
               sequential_time = time.time() - start_time

               # Concurrent approach (faster)
               start_time = time.time()
               with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                   future_to_dag = {executor.submit(get_dag_count, dag_id): dag_id for dag_id in dag_ids}
                   concurrent_results = {}

                   for future in concurrent.futures.as_completed(future_to_dag):
                       dag_id, count = future.result()
                       concurrent_results[dag_id] = count

               concurrent_time = time.time() - start_time

               self.log.info(f"Sequential time: {sequential_time:.2f}s")
               self.log.info(f"Concurrent time: {concurrent_time:.2f}s")

               return {
                   "sequential_results": sequential_results,
                   "concurrent_results": concurrent_results,
                   "performance_improvement": f"{(sequential_time/concurrent_time):.2f}x faster",
               }

**Advanced Debugging Techniques:**

.. code-block:: python

    class DetailedDebugOperator(BaseOperator):
        """Detailed debugging for migration issues."""

        def execute(self, context):
            debug_info = {}

            # 1. Environment information
            import airflow
            import sys
            import os

            debug_info["environment"] = {
                "airflow_version": airflow.__version__,
                "python_version": sys.version,
                "platform": sys.platform,
                "working_directory": os.getcwd(),
                "environment_variables": {k: v for k, v in os.environ.items() if "AIRFLOW" in k},
            }

            # 2. Context analysis
            debug_info["context"] = {
                "available_keys": list(context.keys()),
                "dag_id": context.get("dag", {}).dag_id if context.get("dag") else None,
                "task_id": context.get("task", {}).task_id if context.get("task") else None,
                "run_id": context.get("run_id"),
                "logical_date": str(context.get("logical_date")),
            }

            # 3. Task instance analysis
            task_instance = context.get("task_instance")
            if task_instance:
                debug_info["task_instance"] = {
                    "has_task_sdk_client": hasattr(task_instance, "task_sdk_client"),
                    "task_instance_type": type(task_instance).__name__,
                    "task_instance_attributes": [attr for attr in dir(task_instance) if not attr.startswith("_")],
                }

                # 4. Client analysis
                if hasattr(task_instance, "task_sdk_client"):
                    client = task_instance.task_sdk_client
                    debug_info["client"] = {
                        "client_type": type(client).__name__,
                        "base_url": getattr(client, "base_url", "Not available"),
                        "available_operations": {
                            "task_instances": [m for m in dir(client.task_instances) if not m.startswith("_")],
                            "dag_runs": [m for m in dir(client.dag_runs) if not m.startswith("_")],
                            "variables": [m for m in dir(client.variables) if not m.startswith("_")],
                            "connections": [m for m in dir(client.connections) if not m.startswith("_")],
                            "xcoms": [m for m in dir(client.xcoms) if not m.startswith("_")],
                        },
                    }

            # 5. Configuration analysis
            from airflow.configuration import conf

            debug_info["configuration"] = {
                "database_url": conf.get("database", "sql_alchemy_conn", fallback="Not configured")[:50] + "...",
                "webserver_host": conf.get("webserver", "web_server_host", fallback="Not configured"),
                "webserver_port": conf.get("webserver", "web_server_port", fallback="Not configured"),
            }

            # Log all debug information
            for section, info in debug_info.items():
                self.log.info(f"=== {section.upper()} ===")
                self.log.info(f"{info}")

            return debug_info

**Getting Help:**

1. **Community Resources:**

   - `Airflow Slack <https://s.apache.org/airflow-slack>`_ - #user-troubleshooting channel
   - `GitHub Discussions <https://github.com/apache/airflow/discussions>`_ - For detailed questions
   - `Stack Overflow <https://stackoverflow.com/questions/tagged/apache-airflow>`_ - airflow tag

2. **Documentation:**

   - `Official Airflow 3 Documentation <https://airflow.apache.org/docs/apache-airflow/stable/>`_
   - `Task SDK Documentation <https://airflow.apache.org/docs/apache-airflow/stable/task-sdk/>`_
   - `API Reference <https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html>`_

3. **When Reporting Issues:**

   - Include Airflow version: ``airflow version``
   - Include Python version: ``python --version``
   - Include relevant configuration (sanitized)
   - Include complete error messages and stack traces
   - Include minimal reproducible example
   - Use the DetailedDebugOperator output for context
