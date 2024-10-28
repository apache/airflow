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

import inspect
import json
import logging
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

if TYPE_CHECKING:
    from botocore.client import BaseClient

ENV_ID_ENVIRON_KEY: str = "SYSTEM_TESTS_ENV_ID"
ENV_ID_KEY: str = "ENV_ID"
DEFAULT_ENV_ID_PREFIX: str = "env"
DEFAULT_ENV_ID_LEN: int = 8
DEFAULT_ENV_ID: str = f"{DEFAULT_ENV_ID_PREFIX}{uuid4()!s:.{DEFAULT_ENV_ID_LEN}}"
PURGE_LOGS_INTERVAL_PERIOD = 5

# All test file names will contain one of these strings.
TEST_FILE_IDENTIFIERS: list[str] = ["example_", "test_"]

INVALID_ENV_ID_MSG: str = (
    "To maximize compatibility, the SYSTEM_TESTS_ENV_ID must be an alphanumeric string "
    "which starts with a letter. Please see `providers/tests/system/amazon/README.md`."
)
LOWERCASE_ENV_ID_MSG: str = (
    "The provided Environment ID contains uppercase letters and "
    "will be converted to lowercase for the AWS System Tests."
)
NO_VALUE_MSG: str = "No Value Found: Variable {key} could not be found, and no default value was provided."

log = logging.getLogger(__name__)


def _get_test_name() -> str:
    """
    Extracts the module name from the test module.

    :return: The name of the test module that called the helper method.
    """
    # The exact layer of the stack will depend on if this is called directly
    # or from another helper, but the test will always contain the identifier.
    test_filename: str = next(
        frame.filename
        for frame in inspect.stack()
        if any(identifier in frame.filename for identifier in TEST_FILE_IDENTIFIERS)
    )
    return Path(test_filename).stem


def _validate_env_id(env_id: str) -> str:
    """
    Verifies that a prospective Environment ID value fits requirements.
    An Environment ID for an AWS System test must be a lowercase alphanumeric
    string which starts with a letter.

    :param env_id: An Environment ID to validate.
    :return: A validated string cast to lowercase.
    """
    if any(char.isupper() for char in str(env_id)):
        print(LOWERCASE_ENV_ID_MSG)
    if not env_id.isalnum() or not env_id[0].isalpha():
        raise ValueError(INVALID_ENV_ID_MSG)

    return env_id.lower()


def _fetch_from_ssm(key: str, test_name: str | None = None) -> str:
    """
    Test values are stored in the SSM Value as a JSON-encoded dict of key/value pairs.

    :param key: The key to search for within the returned Parameter Value.
    :return: The value of the provided key from SSM
    """
    _test_name: str = test_name or _get_test_name()
    hook = SsmHook(aws_conn_id=None)
    value: str = ""

    try:
        value = json.loads(hook.get_parameter_value(_test_name))[key]
    # Since a default value after the SSM check is allowed, these exceptions should not stop execution.
    except NoCredentialsError as e:
        log.info("No boto credentials found: %s", e)
    except ClientError as e:
        log.info("Client error when connecting to SSM: %s", e)
    except hook.conn.exceptions.ParameterNotFound as e:
        log.info("SSM does not contain any parameter for this test: %s", e)
    except KeyError as e:
        log.info(
            "SSM contains one parameter for this test, but not the requested value: %s", e
        )
    return value


class Variable:
    """
    Stores metadata about a variable to be fetched for AWS System Tests.

    :param name: The name of the variable to be fetched.
    :param to_split: If True, the input is a string-formatted List and needs to be split. Defaults to False.
    :param delimiter: If to_split is true, this will be used to split the string. Defaults to ','.
    :param test_name: The name of the system test that the variable is associated with.
    """

    def __init__(
        self,
        name: str,
        to_split: bool = False,
        delimiter: str | None = None,
        test_name: str | None = None,
        optional: bool = False,
    ):
        self.name = name
        self.test_name = test_name
        self.to_split = to_split
        if to_split:
            self.delimiter = delimiter or ","
        elif delimiter:
            raise ValueError(
                f"Variable {name} has a delimiter but split_string is set to False."
            )

        self.optional = optional

    def get_value(self):
        if hasattr(self, "default_value"):
            return self._format_value(
                fetch_variable(
                    key=self.name,
                    default_value=self.default_value,
                    test_name=self.test_name,
                    optional=self.optional,
                )
            )

        return self._format_value(
            fetch_variable(
                key=self.name, test_name=self.test_name, optional=self.optional
            )
        )

    def set_default(self, default):
        # Since 'None' is a potentially valid "default" value, we are only creating this
        # field when a default is provided, and in get_value we check if the field exists.
        self.default_value = default

    def _format_value(self, value):
        if self.to_split:
            if not isinstance(value, str):
                raise TypeError(
                    f"{self.name} is type {type(value)} and can not be split as requested."
                )
            return value.split(self.delimiter)
        return value


class SystemTestContextBuilder:
    """
    This builder class ultimately constructs a TaskFlow task which is run at
    runtime (task execution time). This task generates and stores the test ENV_ID as well
    as any external resources requested (e.g.g IAM Roles, VPC, etc)
    """

    def __init__(self):
        self.variables = set()
        self.env_id = set_env_id()
        self.test_name = _get_test_name()

    def add_variable(
        self,
        variable_name: str,
        split_string: bool = False,
        delimiter: str | None = None,
        optional: bool = False,
        **kwargs,
    ):
        """Register a variable to fetch from environment or cloud parameter store"""
        if variable_name in [variable.name for variable in self.variables]:
            raise ValueError(
                f"Variable name {variable_name} already exists in the fetched variables list."
            )

        new_variable = Variable(
            name=variable_name,
            to_split=split_string,
            delimiter=delimiter,
            test_name=self.test_name,
            optional=optional,
        )

        # default_value is accepted via kwargs so that it is completely optional and no
        # default value needs to be provided in the method stub.  For example, if we
        # set it to None, we would have no way to know if that was our default or the
        # legitimate default value that the caller wishes to pass through.
        if "default_value" in kwargs:
            new_variable.set_default(kwargs["default_value"])

        self.variables.add(new_variable)

        return self  # Builder recipe; returning self allows chaining

    def build(self):
        """Build and return a TaskFlow task which will create an env_id and
        fetch requested variables. Storing everything in xcom for downstream
        tasks to use."""

        @task
        def variable_fetcher(ti=None):
            ti.xcom_push(ENV_ID_KEY, self.env_id)
            for variable in self.variables:
                ti.xcom_push(variable.name, variable.get_value())

        return variable_fetcher


def fetch_variable(
    key: str,
    default_value: str | None = None,
    test_name: str | None = None,
    optional: bool = False,
) -> str | None:
    """
    Given a Parameter name: first check for an existing Environment Variable,
    then check SSM for a value. If neither are available, fall back on the
    optional default value.

    :param key: The name of the Parameter to fetch a value for.
    :param default_value: The default value to use if no value can be found.
    :param test_name: The system test name.
    :param optional: Whether the variable is optional. If True, does not raise `ValueError` if variable
        does not exist
    :return: The value of the parameter.
    """

    value: str | None = os.getenv(key, _fetch_from_ssm(key, test_name)) or default_value
    if not optional and not value:
        raise ValueError(NO_VALUE_MSG.format(key=key))
    return value


def set_env_id() -> str:
    """
    Retrieves or generates an Environment ID, validate that it is suitable,
    export it as an Environment Variable, and return it.

    If an Environment ID has already been generated, use that.
    Otherwise, try to fetch it and export it as an Environment Variable.
    If there is not one available to fetch then generate one and export it as an Environment Variable.

    :return: A valid System Test Environment ID.
    """
    env_id: str = str(fetch_variable(ENV_ID_ENVIRON_KEY, DEFAULT_ENV_ID))
    env_id = _validate_env_id(env_id)

    os.environ[ENV_ID_ENVIRON_KEY] = env_id
    return env_id


def all_tasks_passed(ti) -> bool:
    task_runs = ti.get_dagrun().get_task_instances()
    return all([_task.state != State.FAILED for _task in task_runs])


@task(trigger_rule=TriggerRule.ALL_DONE)
def prune_logs(
    logs: list[tuple[str, str | None]],
    force_delete: bool = False,
    retry: bool = False,
    retry_times: int = 3,
    ti=None,
):
    """
    If all tasks in this dagrun have succeeded, then delete the associated logs.
    Otherwise, append the logs with a retention policy.  This allows the logs
    to be used for troubleshooting but assures they won't build up indefinitely.

    :param logs: A list of log_group/stream_prefix tuples to delete.
    :param force_delete: Whether to check log streams within the log group before
        removal. If True, removes the log group and all its log streams inside it.
    :param retry: Whether to retry if the log group/stream was not found. In some
        cases, the log group/stream is created seconds after the main resource has
        been created. By default, it retries for 3 times with a 5s waiting period.
    :param retry_times: Number of retries.
    :param ti: Used to check the status of the tasks. This gets pulled from the
        DAG's context and does not need to be passed manually.
    """
    if all_tasks_passed(ti):
        _purge_logs(logs, force_delete, retry, retry_times)
    else:
        client: BaseClient = boto3.client("logs")
        for group, _ in logs:
            client.put_retention_policy(logGroupName=group, retentionInDays=30)


def _purge_logs(
    test_logs: list[tuple[str, str | None]],
    force_delete: bool = False,
    retry: bool = False,
    retry_times: int = 3,
) -> None:
    """
    Accepts a tuple in the format: ('log group name', 'log stream prefix').
    For each log group, it will delete any log streams matching the provided
    prefix then if the log group is empty, delete the group. If the group
    is not empty that indicates there are logs not generated by the test and
    those are left intact. If `check_log_streams` is True, it will simply delete the log group regardless
    of log streams within that log group.

    :param test_logs: A list of log_group/stream_prefix tuples to delete.
    :param force_delete: Whether to check log streams within the log group before removal. If True,
        removes the log group and all its log streams inside it
    :param retry: Whether to retry if the log group/stream was not found. In some cases, the log group/stream
        is created seconds after the main resource has been created. By default, it retries for 3 times
        with a 5s waiting period
    :param retry_times: Number of retries
    """
    client: BaseClient = boto3.client("logs")

    for group, prefix in test_logs:
        try:
            if prefix:
                log_streams = client.describe_log_streams(
                    logGroupName=group,
                    logStreamNamePrefix=prefix,
                )["logStreams"]

                for stream_name in [stream["logStreamName"] for stream in log_streams]:
                    client.delete_log_stream(
                        logGroupName=group, logStreamName=stream_name
                    )

            if (
                force_delete
                or not client.describe_log_streams(logGroupName=group)["logStreams"]
            ):
                client.delete_log_group(logGroupName=group)
        except ClientError as e:
            if (
                not retry
                or retry_times == 0
                or e.response["Error"]["Code"] != "ResourceNotFoundException"
            ):
                raise e

            time.sleep(PURGE_LOGS_INTERVAL_PERIOD)
            _purge_logs(
                test_logs=test_logs,
                force_delete=force_delete,
                retry=retry,
                retry_times=retry_times - 1,
            )


@task
def split_string(string):
    return string.split(",")
