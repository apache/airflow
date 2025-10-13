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

import json
import time
from collections import deque
from collections.abc import Sequence
from typing import TYPE_CHECKING

from boto3.session import NoCredentialsError
from botocore.utils import ClientError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.aws_lambda.utils import (
    CONFIG_GROUP_NAME,
    INVALID_CREDENTIALS_EXCEPTIONS,
    AllLambdaConfigKeys,
    CommandType,
    LambdaQueuedTask,
)
from airflow.providers.amazon.aws.executors.utils.exponential_backoff_retry import (
    calculate_next_attempt_delay,
    exponential_backoff_retry,
)
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.hooks.sqs import SqsHook

try:
    from airflow._shared.observability.stats import Stats
    from airflow.sdk import timezone
except ImportError:
    from airflow.stats import Stats  # type: ignore[attr-defined,no-redef]
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.executors import workloads
    from airflow.models.taskinstance import TaskInstance


class AwsLambdaExecutor(BaseExecutor):
    """
    An Airflow Executor that submits tasks to AWS Lambda asynchronously.

    When execute_async() is called, the executor invokes a specified AWS Lambda function (asynchronously)
    with a payload that includes the task command and a unique task key.

    The Lambda function writes its result directly to an SQS queue, which is then polled by this executor
    to update task state in Airflow.
    """

    if TYPE_CHECKING and AIRFLOW_V_3_0_PLUS:
        # In the v3 path, we store workloads, not commands as strings.
        # TODO: TaskSDK: move this type change into BaseExecutor
        queued_tasks: dict[TaskInstanceKey, workloads.All]  # type: ignore[assignment]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pending_tasks: deque = deque()
        self.running_tasks: dict[str, TaskInstanceKey] = {}
        self.lambda_function_name = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.FUNCTION_NAME)
        self.sqs_queue_url = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.QUEUE_URL)
        self.dlq_url = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.DLQ_URL)
        self.qualifier = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.QUALIFIER, fallback=None)
        # Maximum number of retries to invoke Lambda.
        self.max_invoke_attempts = conf.get(
            CONFIG_GROUP_NAME,
            AllLambdaConfigKeys.MAX_INVOKE_ATTEMPTS,
        )

        self.attempts_since_last_successful_connection = 0
        self.IS_BOTO_CONNECTION_HEALTHY = False
        self.load_connections(check_connection=False)

    def start(self):
        """Call this when the Executor is run for the first time by the scheduler."""
        check_health = conf.getboolean(CONFIG_GROUP_NAME, AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP)

        if not check_health:
            return

        self.log.info("Starting Lambda Executor and determining health...")
        try:
            self.check_health()
        except AirflowException:
            self.log.error("Stopping the Airflow Scheduler from starting until the issue is resolved.")
            raise

    def check_health(self):
        """
        Check the health of the Lambda and SQS connections.

        For lambda: Use get_function to test if the lambda connection works and the function can be
        described.
        For SQS: Use get_queue_attributes is used as a close analog to describe to test if the SQS
        connection is working.
        """
        self.IS_BOTO_CONNECTION_HEALTHY = False

        def _check_queue(queue_url):
            sqs_get_queue_attrs_response = self.sqs_client.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
            )
            approx_num_msgs = sqs_get_queue_attrs_response.get("Attributes").get(
                "ApproximateNumberOfMessages"
            )
            self.log.info(
                "SQS connection is healthy and queue %s is present with %s messages.",
                queue_url,
                approx_num_msgs,
            )

        self.log.info("Checking Lambda and SQS connections")
        try:
            # Check Lambda health
            lambda_get_response = self.lambda_client.get_function(FunctionName=self.lambda_function_name)
            if self.lambda_function_name not in lambda_get_response["Configuration"]["FunctionName"]:
                raise AirflowException("Lambda function %s not found.", self.lambda_function_name)
            self.log.info(
                "Lambda connection is healthy and function %s is present.", self.lambda_function_name
            )

            # Check SQS results queue
            _check_queue(self.sqs_queue_url)
            # Check SQS dead letter queue
            _check_queue(self.dlq_url)

            # If we reach this point, both connections are healthy and all resources are present
            self.IS_BOTO_CONNECTION_HEALTHY = True
        except Exception:
            self.log.exception("Lambda Executor health check failed")
            raise AirflowException(
                "The Lambda executor will not be able to run Airflow tasks until the issue is addressed."
            )

    def load_connections(self, check_connection: bool = True):
        """
        Retrieve the AWS connection via Hooks to leverage the Airflow connection system.

        :param check_connection: If True, check the health of the connection after loading it.
        """
        self.log.info("Loading Connections")
        aws_conn_id = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.AWS_CONN_ID)
        region_name = conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.REGION_NAME, fallback=None)
        self.sqs_client = SqsHook(aws_conn_id=aws_conn_id, region_name=region_name).conn
        self.lambda_client = LambdaHook(aws_conn_id=aws_conn_id, region_name=region_name).conn

        self.attempts_since_last_successful_connection += 1
        self.last_connection_reload = timezone.utcnow()

        if check_connection:
            self.check_health()
            self.attempts_since_last_successful_connection = 0

    def sync(self):
        """
        Sync the executor with the current state of tasks.

        Check in on currently running tasks and attempt to run any new tasks that have been queued.
        """
        if not self.IS_BOTO_CONNECTION_HEALTHY:
            exponential_backoff_retry(
                self.last_connection_reload,
                self.attempts_since_last_successful_connection,
                self.load_connections,
            )
            if not self.IS_BOTO_CONNECTION_HEALTHY:
                return
        try:
            self.sync_running_tasks()
            self.attempt_task_runs()
        except (ClientError, NoCredentialsError) as error:
            error_code = error.response["Error"]["Code"]
            if error_code in INVALID_CREDENTIALS_EXCEPTIONS:
                self.IS_BOTO_CONNECTION_HEALTHY = False
                self.log.warning(
                    "AWS credentials are either missing or expired: %s.\nRetrying connection", error
                )
        except Exception:
            self.log.exception("An error occurred while syncing tasks")

    def queue_workload(self, workload: workloads.All, session: Session | None) -> None:
        from airflow.executors import workloads

        if not isinstance(workload, workloads.ExecuteTask):
            raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(workload)}")
        ti = workload.ti
        self.queued_tasks[ti.key] = workload

    def _process_workloads(self, workloads: Sequence[workloads.All]) -> None:
        from airflow.executors.workloads import ExecuteTask

        for w in workloads:
            if not isinstance(w, ExecuteTask):
                raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(w)}")

            command = [w]
            key = w.ti.key
            queue = w.ti.queue
            executor_config = w.ti.executor_config or {}

            del self.queued_tasks[key]
            self.execute_async(key=key, command=command, queue=queue, executor_config=executor_config)  # type: ignore[arg-type]
            self.running.add(key)

    def execute_async(self, key: TaskInstanceKey, command: CommandType, queue=None, executor_config=None):
        """
        Save the task to be executed in the next sync by inserting the commands into a queue.

        :param key: A unique task key (typically a tuple identifying the task instance).
        :param command: The shell command string to execute.
        :param executor_config:  (Unused) to keep the same signature as the base.
        :param queue: (Unused) to keep the same signature as the base.
        """
        if len(command) == 1:
            from airflow.executors.workloads import ExecuteTask

            if isinstance(command[0], ExecuteTask):
                workload = command[0]
                ser_input = workload.model_dump_json()
                command = [
                    "python",
                    "-m",
                    "airflow.sdk.execution_time.execute_workload",
                    "--json-string",
                    ser_input,
                ]
            else:
                raise RuntimeError(
                    f"LambdaExecutor doesn't know how to handle workload of type: {type(command[0])}"
                )

        self.pending_tasks.append(
            LambdaQueuedTask(
                key, command, queue if queue else "", executor_config or {}, 1, timezone.utcnow()
            )
        )

    def attempt_task_runs(self):
        """
        Attempt to run tasks that are queued in the pending_tasks.

        Each task is submitted to AWS Lambda with a payload containing the task key and command.
        The task key is used to track the task's state in Airflow.
        """
        queue_len = len(self.pending_tasks)
        for _ in range(queue_len):
            task_to_run = self.pending_tasks.popleft()
            task_key = task_to_run.key
            cmd = task_to_run.command
            attempt_number = task_to_run.attempt_number
            failure_reasons = []
            ser_task_key = json.dumps(task_key._asdict())
            payload = {
                "task_key": ser_task_key,
                "command": cmd,
                "executor_config": task_to_run.executor_config,
            }
            if timezone.utcnow() < task_to_run.next_attempt_time:
                self.pending_tasks.append(task_to_run)
                continue

            self.log.info("Submitting task %s to Lambda function %s", task_key, self.lambda_function_name)

            try:
                invoke_kwargs = {
                    "FunctionName": self.lambda_function_name,
                    "InvocationType": "Event",
                    "Payload": json.dumps(payload),
                }
                if self.qualifier:
                    invoke_kwargs["Qualifier"] = self.qualifier
                response = self.lambda_client.invoke(**invoke_kwargs)
            except NoCredentialsError:
                self.pending_tasks.append(task_to_run)
                raise
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code in INVALID_CREDENTIALS_EXCEPTIONS:
                    self.pending_tasks.append(task_to_run)
                    raise
                failure_reasons.append(str(e))
            except Exception as e:
                # Failed to even get a response back from the Boto3 API or something else went
                # wrong.  For any possible failure we want to add the exception reasons to the
                # failure list so that it is logged to the user and most importantly the task is
                # added back to the pending list to be retried later.
                failure_reasons.append(str(e))

            if failure_reasons:
                # Make sure the number of attempts does not exceed max invoke attempts
                if int(attempt_number) < int(self.max_invoke_attempts):
                    task_to_run.attempt_number += 1
                    task_to_run.next_attempt_time = timezone.utcnow() + calculate_next_attempt_delay(
                        attempt_number
                    )
                    self.pending_tasks.append(task_to_run)
                else:
                    reasons_str = ", ".join(failure_reasons)
                    self.log.error(
                        "Lambda invoke %s has failed a maximum of %s times. Marking as failed. Reasons: %s",
                        task_key,
                        attempt_number,
                        reasons_str,
                    )
                    self.log_task_event(
                        event="lambda invoke failure",
                        ti_key=task_key,
                        extra=(
                            f"Task could not be queued after {attempt_number} attempts. "
                            f"Marking as failed. Reasons: {reasons_str}"
                        ),
                    )
                    self.fail(task_key)
            else:
                status_code = response.get("StatusCode")
                self.log.info("Invoked Lambda for task %s with status %s", task_key, status_code)
                self.running_tasks[ser_task_key] = task_key
                # Add the serialized task key as the info, this will be assigned on the ti as the external_executor_id
                self.running_state(task_key, ser_task_key)

    def sync_running_tasks(self):
        """
        Poll the SQS queue for messages indicating task completion.

        Each message is expected to contain a JSON payload with 'task_key' and 'return_code'.
        Based on the return code, update the task state accordingly.
        """
        if not len(self.running_tasks):
            self.log.debug("No running tasks to process.")
            return

        self.process_queue(self.sqs_queue_url)
        if self.dlq_url and self.running_tasks:
            self.process_queue(self.dlq_url)

    def process_queue(self, queue_url: str):
        """
        Poll the SQS queue for messages indicating task completion.

        Each message is expected to contain a JSON payload with 'task_key' and 'return_code'.

        Based on the return code, update the task state accordingly.
        """
        response = self.sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
        )

        # Pagination? Maybe we don't need it. But we don't always delete messages after viewing them so we
        # could possibly accumulate a lot of messages in the queue and get stuck if we don't read bigger
        # chunks and paginate.
        messages = response.get("Messages", [])
        # The keys that we validate in the messages below will be different depending on whether or not
        # the message is from the dead letter queue or the main results queue.
        message_keys = ("return_code", "task_key")
        if messages and queue_url == self.dlq_url:
            self.log.warning("%d messages received from the dead letter queue", len(messages))
            message_keys = ("command", "task_key")

        for message in messages:
            delete_message = False
            receipt_handle = message["ReceiptHandle"]
            try:
                body = json.loads(message["Body"])
            except json.JSONDecodeError:
                self.log.warning(
                    "Received a message from the queue that could not be parsed as JSON: %s",
                    message["Body"],
                )
                delete_message = True
            # If the message is not already marked for deletion, check if it has the required keys.
            if not delete_message and not all(key in body for key in message_keys):
                self.log.warning(
                    "Message is not formatted correctly, %s and/or %s are missing: %s", *message_keys, body
                )
                delete_message = True
            if delete_message:
                self.log.warning("Deleting the message to avoid processing it again.")
                self.sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                continue
            return_code = body.get("return_code")
            ser_task_key = body.get("task_key")
            # Fetch the real task key from the running_tasks dict, using the serialized task key.
            try:
                task_key = self.running_tasks[ser_task_key]
            except KeyError:
                self.log.debug(
                    "Received task %s from the queue which is not found in running tasks, it is likely "
                    "from another Lambda Executor sharing this queue or might be a stale message that needs "
                    "deleting manually. Marking the message as visible again.",
                    ser_task_key,
                )
                # Mark task as visible again in SQS so that another executor can pick it up.
                self.sqs_client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=0,
                )
                continue

            if task_key:
                if return_code == 0:
                    self.success(task_key)
                    self.log.info(
                        "Successful Lambda invocation for task %s received from SQS queue.", task_key
                    )
                else:
                    self.fail(task_key)
                    if queue_url == self.dlq_url and return_code is None:
                        # DLQ failure: AWS Lambda service could not complete the invocation after retries.
                        # This indicates a Lambda-level failure (timeout, memory limit, crash, etc.)
                        # where the function was unable to successfully execute to return a result.
                        self.log.error(
                            "DLQ message received: Lambda invocation for task: %s was unable to successfully execute. This likely indicates a Lambda-level failure (timeout, memory limit, crash, etc.).",
                            task_key,
                        )
                    else:
                        # In this case the Lambda likely started but failed at run time since we got a non-zero
                        # return code. We could consider retrying these tasks within the executor, because this _likely_
                        # means the Airflow task did not run to completion, however we can't be sure (maybe the
                        # lambda runtime code has a bug and is returning a non-zero when it actually passed?). So
                        # perhaps not retrying is the safest option.
                        self.log.debug(
                            "Lambda invocation for task: %s completed but the underlying Airflow task has returned a non-zero exit code %s",
                            task_key,
                            return_code,
                        )
                # Remove the task from the tracking mapping.
                self.running_tasks.pop(ser_task_key)

            # Delete the message from the queue.
            self.sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        """
        Adopt task instances which have an external_executor_id (the serialized task key).

        Anything that is not adopted will be cleared by the scheduler and becomes eligible for re-scheduling.

        :param tis: The task instances to adopt.
        """
        with Stats.timer("lambda_executor.adopt_task_instances.duration"):
            adopted_tis: list[TaskInstance] = []

            if serialized_task_keys := [
                (ti, ti.external_executor_id) for ti in tis if ti.external_executor_id
            ]:
                for ti, ser_task_key in serialized_task_keys:
                    try:
                        task_key = TaskInstanceKey.from_dict(json.loads(ser_task_key))
                    except Exception:
                        # If that task fails to deserialize, we should just skip it.
                        self.log.exception(
                            "Task failed to be adopted because the key could not be deserialized"
                        )
                        continue
                    self.running_tasks[ser_task_key] = task_key
                    adopted_tis.append(ti)

            if adopted_tis:
                tasks = [f"{task} in state {task.state}" for task in adopted_tis]
                task_instance_str = "\n\t".join(tasks)
                self.log.info(
                    "Adopted the following %d tasks from a dead executor:\n\t%s",
                    len(adopted_tis),
                    task_instance_str,
                )

            not_adopted_tis = [ti for ti in tis if ti not in adopted_tis]
            return not_adopted_tis

    def end(self, heartbeat_interval=10):
        """
        End execution. Poll until all outstanding tasks are marked as completed.

        This is a blocking call and async Lambda tasks can not be cancelled, so this will wait until
        all tasks are either completed or the timeout is reached.

        :param heartbeat_interval: The interval in seconds to wait between checks for task completion.
        """
        self.log.info("Received signal to end, waiting for outstanding tasks to finish.")
        time_to_wait = int(conf.get(CONFIG_GROUP_NAME, AllLambdaConfigKeys.END_WAIT_TIMEOUT))
        start_time = timezone.utcnow()
        while True:
            if time_to_wait:
                current_time = timezone.utcnow()
                elapsed_time = (current_time - start_time).total_seconds()
                if elapsed_time > time_to_wait:
                    self.log.warning(
                        "Timed out waiting for tasks to finish. Some tasks may not be handled gracefully"
                        " as the executor is force ending due to timeout."
                    )
                    break
            self.sync()
            if not self.running_tasks:
                self.log.info("All tasks completed; executor ending.")
                break
            self.log.info("Waiting for %d task(s) to complete.", len(self.running_tasks))
            time.sleep(heartbeat_interval)

    def terminate(self):
        """Get called when the daemon receives a SIGTERM."""
        self.log.warning("Terminating Lambda executor. In-flight tasks cannot be stopped.")
