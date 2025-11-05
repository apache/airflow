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

Audit Logs in Airflow
=====================

Understanding Audit Logs
------------------------

Audit logs serve as the historical record of an Airflow system, documenting who performed what actions and when they occurred. These logs are essential for maintaining system integrity, meeting compliance requirements, and conducting forensic analysis when issues arise.

In essence, audit logs answer three fundamental questions:

- **Who**: Which user or system component initiated an action
- **What**: The specific operation that was performed
- **When**: The precise timestamp of the event

The primary purposes of audit logs include:

- **Regulatory Compliance**: Meeting requirements for data governance and audit trails
- **Security Monitoring**: Detecting unauthorized access or suspicious activities
- **Operational Troubleshooting**: Understanding the sequence of events leading to system issues
- **Change Management**: Tracking modifications to critical system components

.. note::
   Access to audit logs requires the ``Audit Logs.can_read`` permission. Users with this permission can view all audit entries regardless of their DAG-specific access rights.


Understanding Event Logs
------------------------

Event logs represent the operational heartbeat of an Airflow system. Unlike audit logs, which focus on accountability and compliance, event logs capture the technical details of system behavior, application performance, and operational metrics.

Event logs serve several critical functions:

- **Debugging and Troubleshooting**: Providing detailed error messages and stack traces
- **Performance Monitoring**: Recording execution times, resource usage, and system metrics
- **Operational Insights**: Tracking system health, component interactions, and workflow execution
- **Development Support**: Offering detailed information for code debugging and optimization

Event logs are typically stored in log files or external logging systems and include information such as:

- Task execution details and output
- System errors and warnings
- Performance metrics and timing information
- Component startup and shutdown events
- Resource utilization data

Audit Logs vs Event Logs
------------------------

While both logging systems are crucial for system management, they serve distinct purposes and audiences:

.. list-table::
   :header-rows: 1
   :widths: 25 37 38

   * - Characteristic
     - Audit Logs
     - Event Logs
   * - **Primary Purpose**
     - Accountability and compliance tracking
     - Operational monitoring and system debugging
   * - **Target Audience**
     - Security teams, auditors, compliance officers
     - Developers, system administrators, operations teams
   * - **Content Focus**
     - User actions and administrative changes
     - System behavior, errors, and performance data
   * - **Storage Location**
     - Structured database table (``log``)
     - Log files, external logging systems
   * - **Retention Requirements**
     - Long-term (months to years for compliance), if not purged from database
     - Short to medium-term (days to weeks)
   * - **Query Patterns**
     - "Who cleared the task instance for re-execution?"
     - No query made except is a log aggregation framework is used. Usually logs are read on a per task execution basis and will describe: "Why did this task execution fail?"


Accessing Audit Logs
--------------------

Airflow provides multiple interfaces for accessing audit log data, each suited to different use cases and technical requirements:

**Web User Interface**
   The Airflow web interface provides the most accessible method for viewing audit logs. Navigate to **Browse → Audit Logs** to access an interface with built-in filtering, sorting, and search capabilities. This interface is ideal for ad-hoc investigations and routine monitoring.

**REST API Integration**
   For programmatic access and system integration, use the ``/eventLogs`` REST API endpoint. This approach enables automated monitoring, integration with external security tools, and custom reporting applications.



Scope of Audit Logging
----------------------

Airflow's audit logging system captures events across three distinct operational domains:

**User-Initiated Actions**
   These events occur when users interact with Airflow through any interface (web UI, REST API, or command-line tools). Examples include:

   - Manual DAG run triggers and modifications
   - Configuration changes to variables, connections, and pools
   - Task instance state modifications (clearing, marking as success/failed)
   - Administrative operations and user management activities

**System-Generated Events**
   These events are automatically created by Airflow's internal processes during normal operation:

   - Task lifecycle state transitions (queued, running, success, failed)
   - System monitoring events (heartbeat timeouts, external state changes)
   - Automatic recovery operations (task rescheduling, retry attempts)
   - Resource management activities

**Command-Line Interface Operations**
   These events capture activities performed through Airflow's CLI tools:

   - Direct task execution commands
   - DAG management operations
   - System administration and maintenance tasks
   - Automated script executions


Common Audit Log Scenarios
--------------------------

To facilitate audit log analysis, here are some frequently encountered scenarios and their corresponding queries:

**"Who triggered this DAG?"**

.. code-block:: sql

   SELECT dttm, owner, extra
   FROM log
   WHERE event = 'trigger_dag_run' AND dag_id = 'example_dag'
   ORDER BY dttm DESC;

**"What happened to this failed task?"**

.. code-block:: sql

   SELECT dttm, event, owner, extra
   FROM log
   WHERE dag_id = 'example_dag' AND task_id = 'example_task'
   ORDER BY dttm DESC;

**"Who changed variables recently?"**

.. code-block:: sql

   SELECT dttm, event, owner, extra
   FROM log
   WHERE event LIKE '%variable%'
   ORDER BY dttm DESC LIMIT 20;

Event Catalog
-------------

The following section provides a complete reference of all events tracked by Airflow's audit logging system. Understanding these event types will help interpret audit logs and construct effective queries for specific use cases.

Task Instance Events
~~~~~~~~~~~~~~~~~~~~

**System-generated task events**:

- ``running``: Task instance started execution
- ``success``: Task instance completed successfully
- ``failed``: Task instance failed during execution
- ``skipped``: Task instance was skipped
- ``upstream_failed``: Task instance failed due to upstream failure
- ``up_for_retry``: Task instance is scheduled for retry
- ``up_for_reschedule``: Task instance is rescheduled
- ``queued``: Task instance is queued for execution
- ``scheduled``: Task instance is scheduled
- ``deferred``: Task instance is deferred (waiting for trigger)
- ``restarting``: Task instance is restarting
- ``removed``: Task instance was removed

**System monitoring events**:

- ``heartbeat timeout``: Task instance stopped sending heartbeats and will be terminated
- ``state mismatch``: Task instance state changed externally (outside of Airflow)
- ``stuck in queued reschedule``: Task instance was stuck in queued state and rescheduled
- ``stuck in queued tries exceeded``: Task instance exceeded maximum requeue attempts

**User-initiated task events**:

- ``fail task``: User manually marked task as failed
- ``skip task``: User manually marked task as skipped
- ``action_set_failed``: User set task instance as failed through UI/API
- ``action_set_success``: User set task instance as successful through UI/API
- ``action_set_retry``: User set task instance to retry
- ``action_set_skipped``: User set task instance as skipped
- ``action_set_running``: User set task instance as running
- ``action_clear``: User cleared task instance state

User Action Events
~~~~~~~~~~~~~~~~~~

**DAG operations**:

- ``trigger_dag_run``: User triggered a DAG run
- ``delete_dag_run``: User deleted a DAG run
- ``patch_dag_run``: User modified a DAG run
- ``clear_dag_run``: User cleared a DAG run
- ``get_dag_run``: User retrieved DAG run information
- ``get_dag_runs_batch``: User retrieved multiple DAG runs
- ``post_dag_run``: User created a DAG run
- ``patch_dag``: User modified DAG configuration
- ``get_dag``: User retrieved DAG information
- ``get_dags``: User retrieved multiple DAGs
- ``delete_dag``: User deleted a DAG

**Task instance operations**:

- ``post_clear_task_instances``: User cleared task instances
- ``patch_task_instance``: User modified a task instance
- ``get_task_instances_batch``: User retrieved task instance information
- ``delete_task_instance``: User deleted a task instance
- ``get_task_instance``: User retrieved single task instance information
- ``get_task_instance_tries``: User retrieved task instance retry information
- ``patch_task_instances_batch``: User modified multiple task instances

**Variable operations**:

- ``delete_variable``: User deleted a variable
- ``patch_variable``: User modified a variable
- ``post_variable``: User created a variable
- ``bulk_variables``: User performed bulk variable operations

**Connection operations**:

- ``delete_connection``: User deleted a connection
- ``post_connection``: User created a connection
- ``patch_connection``: User modified a connection
- ``bulk_connections``: User performed bulk connection operations
- ``create_default_connections``: User created default connections

**Pool operations**:

- ``get_pool``: User retrieved pool information
- ``get_pools``: User retrieved multiple pools
- ``post_pool``: User created a pool
- ``patch_pool``: User modified a pool
- ``delete_pool``: User deleted a pool
- ``bulk_pools``: User performed bulk pool operations

**Asset operations**:

- ``get_asset``: User retrieved asset information
- ``get_assets``: User retrieved multiple assets
- ``get_asset_alias``: User retrieved asset alias information
- ``get_asset_aliases``: User retrieved multiple asset aliases
- ``post_asset_events``: User created asset events
- ``get_asset_events``: User retrieved asset events
- ``materialize_asset``: User triggered asset materialization
- ``get_asset_queued_events``: User retrieved queued asset events
- ``delete_asset_queued_events``: User deleted queued asset events
- ``get_dag_asset_queued_events``: User retrieved DAG asset queued events
- ``delete_dag_asset_queued_events``: User deleted DAG asset queued events
- ``get_dag_asset_queued_event``: User retrieved specific DAG asset queued event
- ``delete_dag_asset_queued_event``: User deleted specific DAG asset queued event

**Backfill operations**:

- ``get_backfill``: User retrieved backfill information
- ``get_backfills``: User retrieved multiple backfills
- ``post_backfill``: User created a backfill
- ``pause_backfill``: User paused a backfill
- ``unpause_backfill``: User unpaused a backfill
- ``cancel_backfill``: User cancelled a backfill
- ``create_backfill_dry_run``: User performed backfill dry run

**User and Role Management**:

- ``get_user``: User retrieved user information
- ``get_users``: User retrieved multiple users
- ``post_user``: User created a user account
- ``patch_user``: User modified a user account
- ``delete_user``: User deleted a user account
- ``get_role``: User retrieved role information
- ``get_roles``: User retrieved multiple roles
- ``post_role``: User created a role
- ``patch_role``: User modified a role
- ``delete_role``: User deleted a role

CLI Events
~~~~~~~~~~

**DAG Management Commands**:

- ``cli_dags_list``: List all DAGs in the system
- ``cli_dags_show``: Display DAG information and structure
- ``cli_dags_state``: Check the state of a DAG run
- ``cli_dags_next_execution``: Show next execution time for a DAG
- ``cli_dags_trigger``: Trigger a DAG run from command line
- ``cli_dags_delete``: Delete a DAG and its metadata
- ``cli_dags_pause``: Pause a DAG
- ``cli_dags_unpause``: Unpause a DAG
- ``cli_dags_backfill``: Backfill DAG runs for a date range
- ``cli_dags_test``: Test a DAG without affecting the database

**Task Management Commands**:

- ``cli_tasks_list``: List tasks for a specific DAG
- ``cli_tasks_run``: Execute a specific task instance
- ``cli_tasks_test``: Test a task without affecting the database
- ``cli_tasks_state``: Check the state of a task instance
- ``cli_tasks_failed_deps``: Show failed dependencies for a task
- ``cli_tasks_render``: Render task templates
- ``cli_tasks_clear``: Clear task instance state

**Database and System Commands**:

- ``cli_db_init``: Initialize the Airflow database
- ``cli_db_upgrade``: Upgrade the database schema
- ``cli_db_reset``: Reset the database (dangerous operation)
- ``cli_db_shell``: Open database shell
- ``cli_db_check``: Check database connectivity and schema
- ``cli_db_migrate``: Migrate database schema (legacy command)
- ``cli_migratedb``: Legacy database migration command
- ``cli_initdb``: Legacy database initialization command
- ``cli_resetdb``: Legacy database reset command
- ``cli_upgradedb``: Legacy database upgrade command

**User and Security Commands**:

- ``cli_users_create``: Create a new user account
- ``cli_users_delete``: Delete a user account
- ``cli_users_list``: List all users in the system
- ``cli_users_add_role``: Add role to a user
- ``cli_users_remove_role``: Remove role from a user

**Configuration and Variable Commands**:

- ``cli_variables_get``: Retrieve variable value
- ``cli_variables_set``: Set variable value
- ``cli_variables_delete``: Delete a variable
- ``cli_variables_list``: List all variables
- ``cli_variables_import``: Import variables from file
- ``cli_variables_export``: Export variables to file

**Connection Management Commands**:

- ``cli_connections_get``: Retrieve connection details
- ``cli_connections_add``: Add a new connection
- ``cli_connections_delete``: Delete a connection
- ``cli_connections_list``: List all connections
- ``cli_connections_import``: Import connections from file
- ``cli_connections_export``: Export connections to file

**Pool Management Commands**:

- ``cli_pools_get``: Get pool information
- ``cli_pools_set``: Create or update a pool
- ``cli_pools_delete``: Delete a pool
- ``cli_pools_list``: List all pools
- ``cli_pools_import``: Import pools from file
- ``cli_pools_export``: Export pools to file

**Service and Process Commands**:

- ``cli_webserver``: Start the Airflow webserver
- ``cli_scheduler``: Start the Airflow scheduler
- ``cli_worker``: Start a Celery worker
- ``cli_flower``: Start Flower monitoring tool
- ``cli_triggerer``: Start the triggerer process
- ``cli_standalone``: Start Airflow in standalone mode
- ``cli_api_server``: Start the Airflow API server
- ``cli_dag_processor``: Start the DAG processor service
- ``cli_celery_worker``: Start Celery worker (alternative command)
- ``cli_celery_flower``: Start Celery Flower (alternative command)

**Maintenance and Utility Commands**:

- ``cli_cheat_sheet``: Display CLI command reference
- ``cli_version``: Show Airflow version information
- ``cli_info``: Display system information
- ``cli_config_get_value``: Get configuration value
- ``cli_config_list``: List configuration options
- ``cli_plugins``: List installed plugins
- ``cli_rotate_fernet_key``: Rotate Fernet encryption key
- ``cli_sync_perm``: Synchronize permissions
- ``cli_shell``: Start interactive Python shell
- ``cli_kerberos``: Start Kerberos ticket renewer

**Testing and Development Commands**:

- ``cli_test``: Run tests
- ``cli_render``: Render templates
- ``cli_dag_deps``: Show DAG dependencies
- ``cli_task_deps``: Show task dependencies

**Legacy Commands**:

- ``cli_run``: Legacy task run command
- ``cli_backfill``: Legacy backfill command
- ``cli_clear``: Legacy clear command
- ``cli_list_dags``: Legacy DAG list command
- ``cli_list_tasks``: Legacy task list command
- ``cli_pause``: Legacy pause command
- ``cli_unpause``: Legacy unpause command
- ``cli_trigger_dag``: Legacy DAG trigger command

Each CLI command audit log entry includes:

- **User identification**: Who executed the command
- **Command details**: Full command with arguments
- **Execution context**: Working directory, environment variables
- **Timestamp**: When the command was executed
- **Exit status**: Success or failure indication

Custom Events
~~~~~~~~~~~~~

Airflow allows creating custom audit log entries programmatically:

.. code-block:: python

    from airflow.models.log import Log
    from airflow.utils.session import provide_session


    @provide_session
    def log_custom_event(session=None):
        log_entry = Log(event="custom_event", owner="username", extra="Additional context information")
        session.add(log_entry)
        session.commit()


Anatomy of an Audit Log Entry
-----------------------------

Each audit log record contains structured information that provides a complete picture of the logged event. Understanding these fields is essential for effective log analysis:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Field Name
     - Description and Usage
   * - ``dttm``
     - Timestamp indicating when the event occurred (UTC timezone)
   * - ``event``
     - Descriptive name of the action or event (e.g., ``trigger_dag_run``, ``failed``)
   * - ``owner``
     - Identity of the actor: username for user actions, "airflow" for system events
   * - ``dag_id``
     - Identifier of the affected DAG (when applicable)
   * - ``task_id``
     - Identifier of the affected task (when applicable)
   * - ``run_id``
     - Specific DAG run identifier for tracking execution instances
   * - ``try_number``
     - Attempt number for task retries and re-executions
   * - ``map_index``
     - Index for dynamically mapped tasks
   * - ``logical_date``
     - Logical execution date of the DAG run
   * - ``extra``
     - JSON-formatted additional context (parameters, error details, etc.)


Audit Log Query Methods
-----------------------

Effective audit log analysis requires understanding the various methods available for querying and retrieving log data. Each method has its strengths and is suited to different scenarios:

**REST API Examples**:

.. code-block:: bash

    # Get all audit logs
    curl -X GET "http://localhost:8080/api/v1/eventLogs"

    # Filter by event type
    curl -X GET "http://localhost:8080/api/v1/eventLogs?event=trigger_dag_run"

    # Filter by DAG
    curl -X GET "http://localhost:8080/api/v1/eventLogs?dag_id=example_dag"

    # Filter by date range
    curl -X GET "http://localhost:8080/api/v1/eventLogs?after=2024-01-01T00:00:00Z&before=2024-12-31T23:59:59Z"

**Database Query Examples**:

.. code-block:: sql

    -- Get recent user actions
    SELECT dttm, event, owner, dag_id, task_id, extra
    FROM log
    WHERE owner IS NOT NULL
    ORDER BY dttm DESC
    LIMIT 100;

    -- Get task failure events
    SELECT dttm, dag_id, task_id, run_id, extra
    FROM log
    WHERE event = 'failed'
    ORDER BY dttm DESC;

    -- Get user actions on specific DAG
    SELECT dttm, event, owner, extra
    FROM log
    WHERE dag_id = 'example_dag' AND owner IS NOT NULL
    ORDER BY dttm DESC;


Querying Event Logs
-------------------

Event logs (operational logs) are typically accessed through different methods depending on the logging configuration:

**Log Files**:

.. code-block:: bash

    # View scheduler logs
    tail -f $AIRFLOW_HOME/logs/scheduler/latest/*.log

    # View webserver logs
    tail -f $AIRFLOW_HOME/logs/webserver/webserver.log

    # View task logs for specific DAG run
    cat $AIRFLOW_HOME/logs/dag_id/task_id/2024-01-01T00:00:00+00:00/1.log

**REST API for Task Logs**:

.. code-block:: bash

    # Get task instance logs
    curl -X GET "http://localhost:8080/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"

    # Get task logs with metadata
    curl -X GET "http://localhost:8080/api/v1/dags/example_dag/dagRuns/2024-01-01T00:00:00+00:00/taskInstances/example_task/logs/1?full_content=true"

**Python Logging Integration**:

.. code-block:: python

    import logging
    from airflow.utils.log.logging_mixin import LoggingMixin


    class MyOperator(BaseOperator, LoggingMixin):
        def execute(self, context):
            # These will appear in event logs
            self.log.info("Task started")
            self.log.warning("Warning message")
            self.log.error("Error occurred")

**External Logging Systems**:

When using external logging systems (e.g., ELK stack, Splunk, CloudWatch):

.. code-block:: bash

    # Example Elasticsearch query
    curl -X GET "elasticsearch:9200/airflow-*/_search" -H 'Content-Type: application/json' -d'
    {
      "query": {
        "bool": {
          "must": [
            {"match": {"dag_id": "example_dag"}},
            {"range": {"@timestamp": {"gte": "2024-01-01", "lte": "2024-01-31"}}}
          ]
        }
      }
    }'


Practical Query Examples
------------------------

The following examples demonstrate practical applications of audit log queries for common operational and security scenarios. These queries serve as templates that can be adapted for specific requirements:

**Security Investigation**

.. code-block:: sql

   -- Find all actions by a specific user in the last 24 hours
   SELECT dttm, event, dag_id, task_id, extra
   FROM log
   WHERE owner = 'suspicious_user'
     AND dttm > NOW() - INTERVAL '24 hours'
   ORDER BY dttm DESC;

**Compliance Reporting**

.. code-block:: sql

   -- Get all variable and connection changes for audit report
   SELECT dttm, event, owner, extra
   FROM log
   WHERE event IN ('post_variable', 'patch_variable', 'delete_variable',
                   'post_connection', 'patch_connection', 'delete_connection')
     AND dttm BETWEEN '2024-01-01' AND '2024-01-31'
   ORDER BY dttm;

**Troubleshooting DAG Issues**

.. code-block:: sql

   -- See all events for a problematic DAG run
   SELECT dttm, event, task_id, owner, extra
   FROM log
   WHERE dag_id = 'example_dag'
     AND run_id = '2024-01-15T10:00:00+00:00'
   ORDER BY dttm;
