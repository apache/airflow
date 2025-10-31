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

.. _troubleshooting:

How to Debug Your Airflow Deployment
=====================================

This guide provides a systematic approach to debugging Airflow deployments based on proven debugging principles. It's designed to help you think like an Airflow debugger and develop effective troubleshooting strategies for your specific deployment.

.. contents:: Table of Contents
   :local:
   :depth: 2

What You Should Expect from This Guide
---------------------------------------

**What this guide provides:**

- A systematic debugging methodology for Airflow deployments
- Real-world examples with symptoms, diagnosis, and suggestions
- Guidelines for building your own debugging practices
- Links to relevant documentation and community resources

**What this guide does NOT provide:**

- Comprehensive solutions for every possible Airflow issue
- End-to-end recipes for all deployment scenarios
- Guaranteed fixes for complex infrastructure problems

**Your responsibility:**

- Adapt these guidelines to your specific deployment environment
- Build deployment-specific debugging procedures
- Maintain logs and monitoring appropriate for your setup
- Understand your infrastructure and dependencies

The 9 Rules of Airflow Debugging
---------------------------------

Based on Agans' debugging principles, here's how to approach Airflow issues systematically:

Rule 1: Understand the System
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before debugging, understand your Airflow architecture and components.

**Key Airflow Components to Understand:**

- **Scheduler**: Orchestrates DAG execution and task scheduling
- **Executor**: Manages task execution (LocalExecutor, CeleryExecutor, KubernetesExecutor)
- **Webserver**: Provides the UI and API
- **Workers**: Execute tasks (in distributed setups)
- **Database**: Stores metadata and state
- **Message Broker**: Queues tasks (Redis/RabbitMQ for Celery)

**Example: Task Stuck in Queued State**

*Symptoms:*
- Tasks remain in "queued" state indefinitely
- No error messages in task logs
- DAG appears to run normally

*Diagnosis:*
Understanding the system helps identify that queued tasks indicate an executor issue - tasks are scheduled but not picked up for execution.

*Suggestions:*
- Check executor configuration and worker availability
- Verify message broker connectivity (for CeleryExecutor)
- Review resource limits and worker capacity
- Check ``airflow celery worker`` logs for distributed setups

Rule 2: Make It Fail
^^^^^^^^^^^^^^^^^^^^^

Reproduce issues consistently to understand their patterns.

**Airflow-Specific Reproduction Strategies:**

- Test with minimal DAGs to isolate issues
- Use ``airflow tasks test`` for individual task debugging
- Trigger DAG runs manually to control timing
- Modify task configurations incrementally

**Example: Intermittent Task Failures**

*Symptoms:*
- Tasks fail randomly with connection timeouts
- Same task succeeds on retry
- No clear pattern in failure timing

*Diagnosis:*
Intermittent failures often indicate resource contention, network issues, or race conditions.

*Suggestions:*
- Run the task multiple times: ``airflow tasks test dag_id task_id execution_date``
- Monitor system resources during task execution
- Check connection pool settings and limits
- Review retry configuration and implement exponential backoff

Rule 3: Quit Thinking and Look
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Examine actual logs and data rather than making assumptions.

**Essential Airflow Log Locations:**

- Task logs: ``$AIRFLOW_HOME/logs/dag_id/task_id/execution_date/``
- Scheduler logs: ``$AIRFLOW_HOME/logs/scheduler/``
- Webserver logs: Check your webserver configuration
- Worker logs: ``airflow celery worker`` output

**Example: "Task Not Found" Error**

*Symptoms:*
- Error: "Task 'task_id' not found in DAG 'dag_id'"
- Task exists in DAG file
- Other tasks in same DAG work fine

*Diagnosis:*
Instead of assuming the task definition is correct, examine the actual parsed DAG.

*Suggestions:*
- Check DAG parsing: ``airflow dags show dag_id``
- Verify task_id spelling and case sensitivity
- Look for conditional task creation logic
- Check if task is dynamically generated and conditions are met

Rule 4: Divide and Conquer
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Isolate problems by testing components separately.

**Airflow Isolation Strategies:**

- Test individual tasks: ``airflow tasks test``
- Test connections: ``airflow connections test``
- Test DAG parsing: ``airflow dags list-import-errors``
- Test database connectivity: ``airflow db check``

**Example: DAG Import Failures**

*Symptoms:*
- DAGs not appearing in UI
- Import errors in scheduler logs
- Some DAGs work, others don't

*Diagnosis:*
Isolate the problematic DAG from working ones to identify the specific issue.

*Suggestions:*
- Test DAG parsing individually: ``python /path/to/dag.py``
- Check import errors: ``airflow dags list-import-errors``
- Move problematic DAG to separate directory for testing
- Verify all imports and dependencies are available

Rule 5: Change One Thing at a Time
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Make incremental changes to avoid introducing new problems.

**Airflow Change Management:**

- Modify one configuration parameter at a time
- Test single task changes before applying to entire DAG
- Update one dependency version at a time
- Change one environment variable per test cycle

**Example: Performance Optimization**

*Symptoms:*
- Slow DAG execution
- Tasks taking longer than expected
- Resource utilization issues

*Diagnosis:*
Multiple performance factors could be involved.

*Suggestions:*
- Change one setting at a time: parallelism, pool slots, or worker count
- Test each change with consistent workload
- Monitor metrics after each modification
- Document performance impact of each change

Rule 6: Keep an Audit Trail
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Document your debugging process and changes made.

**Airflow Debugging Documentation:**

- Log configuration changes with timestamps
- Record error messages and their contexts
- Document successful and failed solutions
- Track performance metrics before/after changes

**Example: Connection Configuration Issues**

*Symptoms:*
- Tasks fail with authentication errors
- Connection tests pass in UI
- Intermittent connection failures

*Diagnosis:*
Connection configuration might have subtle issues not apparent in simple tests.

*Suggestions:*
- Document exact connection parameters tested
- Record which authentication methods were tried
- Log environment variables and their values
- Keep track of successful connection configurations

Rule 7: Check the Plug
^^^^^^^^^^^^^^^^^^^^^^^

Verify basic assumptions and simple causes first.

**Airflow "Plug" Checks:**

- Is Airflow running? ``airflow version``
- Are services accessible? ``airflow db check``
- Are DAGs in the correct directory? ``echo $AIRFLOW_HOME/dags``
- Are permissions correct? ``ls -la $AIRFLOW_HOME/dags``

**Example: DAGs Not Loading**

*Symptoms:*
- No DAGs visible in UI
- Scheduler appears to be running
- No obvious error messages

*Diagnosis:*
Before investigating complex issues, check basic requirements.

*Suggestions:*
- Verify DAG directory path: ``airflow config get-value core dags_folder``
- Check file permissions and ownership
- Confirm DAG files have ``.py`` extension
- Test with a simple example DAG

Rule 8: Get a Fresh View
^^^^^^^^^^^^^^^^^^^^^^^^^

Seek external perspectives when stuck.

**Airflow Community Resources:**

- `Apache Airflow Slack <https://s.apache.org/airflow-slack>`_
- `GitHub Issues <https://github.com/apache/airflow/issues>`_
- `Stack Overflow <https://stackoverflow.com/questions/tagged/airflow>`_
- `Airflow Documentation <https://airflow.apache.org/docs/>`_

**Example: Complex DAG Dependencies**

*Symptoms:*
- DAG runs but tasks execute in wrong order
- Dependencies seem correct in code
- Graph view shows unexpected relationships

*Diagnosis:*
Complex dependency logic might have subtle issues not obvious to the original author.

*Suggestions:*
- Ask colleague to review DAG structure
- Post dependency graph on community forums
- Compare with similar working DAGs
- Use ``airflow tasks list dag_id --tree`` to visualize dependencies

Rule 9: If You Didn't Fix It, It Ain't Fixed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Verify that your solution actually resolves the problem.

**Airflow Solution Verification:**

- Run full DAG cycles after fixes
- Monitor for several execution periods
- Test edge cases and failure scenarios
- Verify fix doesn't introduce new issues

**Example: Memory Leak Resolution**

*Symptoms:*
- Workers consuming increasing memory
- Tasks failing with OOM errors
- System performance degrading over time

*Diagnosis:*
Memory issues might appear resolved but return under load.

*Suggestions:*
- Monitor memory usage over extended periods
- Test with production-like workloads
- Verify fix works across different task types
- Implement monitoring to catch regressions early

Common Debugging Scenarios
---------------------------

Task Execution Issues
^^^^^^^^^^^^^^^^^^^^^

**Scenario: Tasks Failing with Import Errors**

*Symptoms:*
- ``ModuleNotFoundError`` in task logs
- Tasks work in development but fail in production
- Inconsistent failures across workers

*Diagnosis:*
Environment differences between development and production, or missing dependencies on workers.

*Suggestions:*
- Compare Python environments: ``pip list`` on all workers
- Check ``PYTHONPATH`` configuration
- Verify custom modules are accessible to workers
- Use virtual environments consistently across environments

**Scenario: Tasks Timing Out**

*Symptoms:*
- Tasks marked as failed after timeout period
- No error in task logic
- Timeout occurs at predictable intervals

*Diagnosis:*
Task timeout configuration or resource constraints.

*Suggestions:*
- Check task timeout settings: ``task_timeout`` parameter
- Monitor resource usage during task execution
- Review database connection timeouts
- Consider increasing timeout or optimizing task logic

Scheduler Issues
^^^^^^^^^^^^^^^^

**Scenario: Scheduler Not Picking Up New DAGs**

*Symptoms:*
- New DAG files not appearing in UI
- Scheduler logs show no errors
- Existing DAGs continue to work

*Diagnosis:*
DAG parsing issues or scheduler configuration problems.

*Suggestions:*
- Check DAG parsing interval: ``scheduler.dag_dir_list_interval``
- Verify DAG file syntax: ``python /path/to/dag.py``
- Review scheduler logs for parsing errors
- Restart scheduler if configuration changed

Database and Connectivity
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Scenario: Database Connection Pool Exhaustion**

*Symptoms:*
- Tasks fail with database connection errors
- Errors occur during high concurrency periods
- Connection errors are temporary

*Diagnosis:*
Database connection pool too small for workload.

*Suggestions:*
- Increase connection pool size: ``sql_alchemy_pool_size``
- Monitor database connection usage
- Review task concurrency settings
- Consider connection pooling at database level

Performance Issues
^^^^^^^^^^^^^^^^^^^

**Scenario: Slow DAG Loading**

*Symptoms:*
- UI takes long time to load DAG views
- Scheduler performance degraded
- High CPU usage during DAG parsing

*Diagnosis:*
Complex DAG logic or too many DAG files.

*Suggestions:*
- Profile DAG parsing time: ``airflow dags list-import-errors``
- Optimize DAG generation logic
- Reduce number of DAG files if possible
- Consider DAG serialization: ``store_serialized_dags = True``

Building Your Debugging Toolkit
--------------------------------

Essential Commands
^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   # Test individual tasks
   airflow tasks test dag_id task_id execution_date

   # Check DAG structure
   airflow dags show dag_id

   # List import errors
   airflow dags list-import-errors

   # Test connections
   airflow connections test connection_id

   # Check database connectivity
   airflow db check

   # View configuration
   airflow config list

Monitoring and Logging
^^^^^^^^^^^^^^^^^^^^^^^

Set up comprehensive monitoring for:

- Task success/failure rates
- Execution times and resource usage
- Queue lengths and worker utilization
- Database performance metrics
- System resource consumption

**Log Analysis Tips:**

- Use structured logging for easier parsing
- Implement log aggregation for distributed setups
- Set appropriate log levels for different components
- Regularly rotate and archive logs

Creating Deployment-Specific Procedures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Develop standardized procedures for your environment:

1. **Incident Response Checklist**
   - Steps to take when critical DAGs fail
   - Escalation procedures and contact information
   - Rollback procedures for configuration changes

2. **Regular Health Checks**
   - Automated monitoring of key metrics
   - Periodic validation of critical DAGs
   - Database and infrastructure health checks

3. **Change Management Process**
   - Testing procedures for DAG changes
   - Configuration change approval process
   - Deployment and rollback procedures

Remember: This guide provides a framework for debugging Airflow deployments. Your specific environment will require customized approaches and procedures. Use these principles as a starting point to develop your own debugging expertise and deployment-specific practices.
