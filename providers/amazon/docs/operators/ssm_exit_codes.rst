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

===========================================
SSM Exit Code Handling and Workflow Routing
===========================================

.. contents:: Table of Contents
   :depth: 3
   :local:

Overview
========

Apache Airflow's Amazon SSM integration provides enhanced exit code handling capabilities that enable
workflow routing patterns. The ``fail_on_nonzero_exit`` parameter allows tasks to complete
successfully regardless of command exit codes, enabling you to capture exit codes and route workflows
based on different exit conditions.

This feature is particularly useful when:

* You need to handle multiple exit codes from commands and route workflows accordingly
* Commands may exit with non-zero codes that represent valid business logic states (not errors)
* You want to implement retry logic with different parameters based on exit codes
* You need to trigger different downstream processing paths based on command results
* You're migrating from traditional schedulers that support exit code routing

Operational Modes
=================

Traditional Mode (Default)
--------------------------

In traditional mode (``fail_on_nonzero_exit=True`` or parameter not specified), the SSM operators
and sensors behave as they always have:

* Non-zero exit codes cause task failures
* Failed command status causes sensor failures
* Workflow stops on command failures unless error handling is explicitly configured

This mode maintains full backward compatibility with existing DAGs.

Enhanced Mode
-------------

In enhanced mode (``fail_on_nonzero_exit=False``), the SSM operators and sensors provide graceful
exit code handling:

* Tasks complete successfully regardless of command exit codes
* Command-level failures (non-zero exit codes) are tolerated
* AWS-level failures (TimedOut, Cancelled) still raise exceptions
* Exit codes can be retrieved and used for workflow routing decisions

**Important**: Enhanced mode distinguishes between:

* **Command-level failures**: Commands that execute but return non-zero exit codes (tolerated in enhanced mode)
* **AWS-level failures**: Service-level issues like timeouts or cancellations (always raise exceptions)

Usage Patterns
==============

Pattern 1: Enhanced Async Pattern
---------------------------------

This pattern provides non-blocking execution with full async benefits while handling any exit code gracefully.
It's ideal for long-running commands where you want to free up worker slots.

**Use case**: Long-running commands where you want async execution and need to handle non-zero exit codes.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ssm_exit_codes.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ssm_enhanced_async]
    :end-before: [END howto_operator_ssm_enhanced_async]

**Key points**:

* Set ``wait_for_completion=False`` on the operator to return immediately
* Set ``fail_on_nonzero_exit=False`` on both operator and sensor
* Use :class:`~airflow.providers.amazon.aws.sensors.ssm.SsmRunCommandCompletedSensor` to wait for completion
* Retrieve output with :class:`~airflow.providers.amazon.aws.operators.ssm.SsmGetCommandInvocationOperator`

Pattern 2: Enhanced Sync Pattern
--------------------------------

This pattern provides synchronous execution with simplified code while handling any exit code gracefully.
It's ideal for short-running commands where blocking the worker is acceptable.

**Use case**: Short-running commands where you want simple synchronous execution and need to handle non-zero exit codes.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ssm_exit_codes.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ssm_enhanced_sync]
    :end-before: [END howto_operator_ssm_enhanced_sync]

**Key points**:

* Set ``wait_for_completion=True`` on the operator to wait for completion
* Set ``fail_on_nonzero_exit=False`` to tolerate non-zero exit codes
* No sensor needed - operator waits for completion
* Simpler code but blocks worker during execution

Pattern 3: Exit Code Routing
----------------------------

This pattern demonstrates how to route workflows based on command exit codes.

**Use case**: Complex workflows where different exit codes trigger different downstream processing paths.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ssm_exit_codes.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ssm_exit_code_routing]
    :end-before: [END howto_operator_ssm_exit_code_routing]

**Key points**:

* Use :class:`~airflow.providers.amazon.aws.operators.ssm.SsmGetCommandInvocationOperator` to retrieve exit codes
* Access ``response_code`` field for the numeric exit code
* Use branching operators or custom logic to route based on exit codes
* Different exit codes can trigger different downstream tasks

Pattern 4: Traditional Pattern
------------------------------

This pattern shows the traditional behavior for comparison. This is the default when ``fail_on_nonzero_exit``
is not specified or set to ``True``.

**Use case**: When you want tasks to fail on non-zero exit codes (default behavior).

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ssm_exit_codes.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ssm_traditional]
    :end-before: [END howto_operator_ssm_traditional]

**Key points**:

* Default behavior - no parameter changes needed
* Non-zero exit codes cause task failures
* Failed command status causes sensor failures
* Maintains backward compatibility with existing DAGs

Parameter Reference
===================

fail_on_nonzero_exit Parameter
------------------------------

The ``fail_on_nonzero_exit`` parameter is available on:

* :class:`~airflow.providers.amazon.aws.operators.ssm.SsmRunCommandOperator`
* :class:`~airflow.providers.amazon.aws.sensors.ssm.SsmRunCommandCompletedSensor`

**Type**: ``bool``

**Default**: ``True`` (maintains backward compatibility)

**Behavior**:

.. list-table::
   :header-rows: 1
   :widths: 15 25 30 30

   * - Value
     - Command Exit Code
     - Operator/Sensor Behavior
     - Use Case
   * - ``True`` (default)
     - 0 (success)
     - Task succeeds
     - Traditional mode
   * - ``True`` (default)
     - Non-zero (failure)
     - Task fails with exception
     - Traditional mode
   * - ``False``
     - 0 (success)
     - Task succeeds
     - Enhanced mode
   * - ``False``
     - Non-zero (failure)
     - Task succeeds, exit code available for routing
     - Enhanced mode
   * - ``True`` or ``False``
     - AWS-level failure (TimedOut, Cancelled)
     - Task always fails with exception
     - Both modes

**Example**:

.. code-block:: python

    # Enhanced mode - tolerate non-zero exit codes
    operator = SsmRunCommandOperator(
        task_id="run_command",
        document_name="AWS-RunShellScript",
        run_command_kwargs={...},
        fail_on_nonzero_exit=False,  # Enable enhanced mode
    )

    # Traditional mode - fail on non-zero exit codes (default)
    operator = SsmRunCommandOperator(
        task_id="run_command",
        document_name="AWS-RunShellScript",
        run_command_kwargs={...},
        # fail_on_nonzero_exit=True (default)
    )

Migration Guide
===============

Migrating from Manual Polling
-----------------------------

If you're currently using manual polling workarounds to handle non-zero exit codes, you can simplify
your DAGs significantly with the enhanced mode.

**Before (Manual Polling Anti-Pattern)**:

This approach requires complex custom logic to poll for command completion and handle failures:

.. code-block:: python

    # DON'T DO THIS - Complex manual polling workaround
    @task
    def manual_wait_for_completion(**context):
        """Custom polling logic - complex and error-prone"""
        from airflow.providers.amazon.aws.hooks.ssm import SsmHook

        command_id = context["ti"].xcom_pull(task_ids="run_command")
        hook = SsmHook(aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME)

        # Manual polling loop with error handling
        max_attempts = 18
        attempt = 0

        while attempt < max_attempts:
            try:
                response = hook.list_command_invocations(command_id)
                invocations = response.get("CommandInvocations", [])

                if not invocations:
                    attempt += 1
                    continue

                all_completed = True
                for invocation in invocations:
                    status = invocation["Status"]
                    if status in ["Pending", "InProgress"]:
                        all_completed = False
                        break

                if all_completed:
                    return {"command_id": command_id, "completed": True}

            except Exception as e:
                print(f"Error: {e}")

            import time

            time.sleep(10)
            attempt += 1

        return {"command_id": command_id, "timeout": True}


    run_command = SsmRunCommandOperator(task_id="run_command", wait_for_completion=False, ...)

    run_command >> manual_wait_for_completion() >> get_output

**After (Enhanced Mode - Recommended)**:

The enhanced mode eliminates the need for manual polling:

.. code-block:: python

    # DO THIS - Simple and clean with enhanced mode
    run_command = SsmRunCommandOperator(
        task_id="run_command",
        document_name="AWS-RunShellScript",
        run_command_kwargs={...},
        wait_for_completion=False,
        fail_on_nonzero_exit=False,  # Enable enhanced mode
    )

    wait_command = SsmRunCommandCompletedSensor(
        task_id="wait_command",
        command_id="{{ ti.xcom_pull(task_ids='run_command') }}",
        fail_on_nonzero_exit=False,  # Enable enhanced mode
    )

    get_output = SsmGetCommandInvocationOperator(
        task_id="get_output",
        command_id="{{ ti.xcom_pull(task_ids='run_command') }}",
    )

    run_command >> wait_command >> get_output

**Benefits of migration**:

* **Simpler code**: No custom polling logic needed
* **More reliable**: Uses AWS waiters instead of custom polling
* **Better resource usage**: Sensor properly releases worker slots
* **Easier maintenance**: Less code to maintain and debug
* **Consistent behavior**: Follows Airflow patterns and conventions

Migrating from Traditional Mode
-------------------------------

If you have existing DAGs using traditional mode and want to add exit code routing:

**Step 1**: Add ``fail_on_nonzero_exit=False`` to operators and sensors:

.. code-block:: python

    # Before
    run_command = SsmRunCommandOperator(
        task_id="run_command",
        document_name="AWS-RunShellScript",
        run_command_kwargs={...},
    )

    # After
    run_command = SsmRunCommandOperator(
        task_id="run_command",
        document_name="AWS-RunShellScript",
        run_command_kwargs={...},
        fail_on_nonzero_exit=False,  # Add this parameter
    )

**Step 2**: Add exit code retrieval and routing logic:

.. code-block:: python

    get_output = SsmGetCommandInvocationOperator(
        task_id="get_output",
        command_id="{{ ti.xcom_pull(task_ids='run_command') }}",
    )


    @task
    def route_based_on_exit_code(**context):
        output = context["ti"].xcom_pull(task_ids="get_output")
        exit_code = output.get("response_code")

        if exit_code == 0:
            return "success_path"
        elif exit_code == 1:
            return "retry_path"
        else:
            return "error_path"


    run_command >> get_output >> route_based_on_exit_code()

Best Practices
==============

Choosing the Right Pattern
--------------------------

**Use Enhanced Async Pattern when**:

* Commands take more than a few seconds to complete
* You want to maximize worker slot efficiency
* You need full async benefits with deferrable operators
* You're running many concurrent SSM commands

**Use Enhanced Sync Pattern when**:

* Commands complete quickly (under 30 seconds)
* Simplicity is more important than worker efficiency
* You don't need async execution
* You're running few concurrent SSM commands

**Use Traditional Mode when**:

* You want tasks to fail on non-zero exit codes
* You don't need exit code routing
* You're maintaining existing DAGs with traditional behavior

Error Handling Recommendations
------------------------------

**Always handle AWS-level failures**: Even in enhanced mode, AWS-level failures (TimedOut, Cancelled)
will raise exceptions. Ensure your DAG has appropriate error handling:

.. code-block:: python

    run_command = SsmRunCommandOperator(
        task_id="run_command",
        fail_on_nonzero_exit=False,
        retries=2,  # Retry on AWS-level failures
        retry_delay=timedelta(minutes=1),
    )

**Log exit codes for debugging**: Always log exit codes in your routing logic:

.. code-block:: python

    @task
    def route_based_on_exit_code(**context):
        output = context["ti"].xcom_pull(task_ids="get_output")
        exit_code = output.get("response_code")

        log.info("Command exit code: %s", exit_code)  # Always log

        # Route based on exit code
        if exit_code == 0:
            return "success_path"
        # ...

**Document exit code meanings**: Add comments explaining what different exit codes mean:

.. code-block:: python

    @task
    def route_based_on_exit_code(**context):
        """
        Route workflow based on command exit code.

        Exit codes:
        - 0: Success - continue normal processing
        - 1: Recoverable error - retry with backoff
        - 2: Configuration error - alert operations team
        - 3: Data validation error - skip and continue
        """
        output = context["ti"].xcom_pull(task_ids="get_output")
        exit_code = output.get("response_code")
        # ...

Performance Considerations
--------------------------

**Sensor poke interval**: Adjust the poke interval based on expected command duration:

.. code-block:: python

    # For quick commands (< 1 minute)
    wait_command = SsmRunCommandCompletedSensor(
        task_id="wait_command",
        command_id="{{ ti.xcom_pull(task_ids='run_command') }}",
        fail_on_nonzero_exit=False,
        poke_interval=5,  # Check every 5 seconds
    )

    # For long commands (> 5 minutes)
    wait_command = SsmRunCommandCompletedSensor(
        task_id="wait_command",
        command_id="{{ ti.xcom_pull(task_ids='run_command') }}",
        fail_on_nonzero_exit=False,
        poke_interval=30,  # Check every 30 seconds
    )

**Use deferrable mode for long-running commands**: For commands that take several minutes or more,
use deferrable mode to free up worker slots:

.. code-block:: python

    run_command = SsmRunCommandOperator(
        task_id="run_command",
        document_name="AWS-RunShellScript",
        run_command_kwargs={...},
        deferrable=True,  # Use deferrable mode
        fail_on_nonzero_exit=False,
    )

Common Use Cases
================

Retry with Different Parameters
--------------------------------

Handle recoverable errors by retrying with different parameters:

.. code-block:: python

    @task
    def route_for_retry(**context):
        output = context["ti"].xcom_pull(task_ids="get_output")
        exit_code = output.get("response_code")

        if exit_code == 0:
            return "success_task"
        elif exit_code == 1:
            return "retry_with_more_memory"
        elif exit_code == 2:
            return "retry_with_different_config"
        else:
            return "alert_operations"

Multi-Exit Code Workflows
--------------------------

Handle multiple exit codes with different downstream paths:

.. code-block:: python

    @task
    def route_multi_exit(**context):
        output = context["ti"].xcom_pull(task_ids="get_output")
        exit_code = output.get("response_code")

        routing_map = {
            0: "process_success",
            1: "handle_warning",
            2: "handle_error",
            3: "skip_processing",
            4: "alert_and_retry",
        }

        return routing_map.get(exit_code, "handle_unknown_exit_code")

Conditional Alerting
--------------------

Send alerts only for specific exit codes:

.. code-block:: python

    @task
    def conditional_alert(**context):
        output = context["ti"].xcom_pull(task_ids="get_output")
        exit_code = output.get("response_code")

        # Only alert on critical errors (exit codes 10-19)
        if 10 <= exit_code <= 19:
            send_alert(f"Critical error: exit code {exit_code}")
            return "critical_error_path"
        elif exit_code != 0:
            log.warning("Non-critical error: exit code %s", exit_code)
            return "warning_path"
        else:
            return "success_path"

Troubleshooting
===============

Task Still Fails on Non-Zero Exit Code
---------------------------------------

**Problem**: Task fails even with ``fail_on_nonzero_exit=False``

**Solution**: Ensure the parameter is set on both the operator and sensor:

.. code-block:: python

    # Set on operator
    run_command = SsmRunCommandOperator(
        task_id="run_command", fail_on_nonzero_exit=False, ...  # Must be set here
    )

    # Set on sensor
    wait_command = SsmRunCommandCompletedSensor(
        task_id="wait_command", fail_on_nonzero_exit=False, ...  # Must also be set here
    )

Task Fails with TimedOut or Cancelled
--------------------------------------

**Problem**: Task fails even in enhanced mode with TimedOut or Cancelled status

**Solution**: This is expected behavior. AWS-level failures always raise exceptions, even in enhanced mode.
These represent service-level issues, not command-level failures. To handle these:

.. code-block:: python

    run_command = SsmRunCommandOperator(
        task_id="run_command",
        fail_on_nonzero_exit=False,
        retries=2,  # Retry on AWS-level failures
        retry_delay=timedelta(minutes=1),
    )

Cannot Retrieve Exit Code
--------------------------

**Problem**: ``response_code`` field is missing or None

**Solution**: Ensure you're using :class:`~airflow.providers.amazon.aws.operators.ssm.SsmGetCommandInvocationOperator`
to retrieve the output:

.. code-block:: python

    get_output = SsmGetCommandInvocationOperator(
        task_id="get_output",
        command_id="{{ ti.xcom_pull(task_ids='run_command') }}",
        instance_id=instance_id,  # Specify instance_id if needed
    )


    @task
    def check_exit_code(**context):
        output = context["ti"].xcom_pull(task_ids="get_output")

        # Check if output exists
        if not output:
            log.error("No output retrieved")
            return

        # Access response_code
        exit_code = output.get("response_code")
        log.info("Exit code: %s", exit_code)

Reference
=========

* :doc:`ssm` - Main SSM operators and sensors documentation
* `AWS SSM Run Command Documentation <https://docs.aws.amazon.com/systems-manager/latest/userguide/run-command.html>`__
* `AWS boto3 library documentation for Amazon SSM <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html>`__
