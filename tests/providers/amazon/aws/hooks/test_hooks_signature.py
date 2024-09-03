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
from importlib import import_module
from pathlib import Path

import pytest

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

BASE_AWS_HOOKS = ["AwsGenericHook", "AwsBaseHook"]
ALLOWED_THICK_HOOKS_PARAMETERS: dict[str, set[str]] = {
    # This list should only be reduced not extended with new parameters,
    # unless there is an exceptional reason.
    "AthenaHook": {"sleep_time", "log_query"},
    "AthenaSQLHook": {"athena_conn_id"},
    "BatchClientHook": {"status_retries", "max_retries"},
    "BatchWaitersHook": {"waiter_config"},
    "DataSyncHook": {"wait_interval_seconds"},
    "DynamoDBHook": {"table_name", "table_keys"},
    "EC2Hook": {"api_type"},
    "ElastiCacheReplicationGroupHook": {
        "exponential_back_off_factor",
        "max_retries",
        "initial_poke_interval",
    },
    "EmrHook": {"emr_conn_id"},
    "EmrContainerHook": {"virtual_cluster_id"},
    "FirehoseHook": {"delivery_stream"},
    "GlueJobHook": {
        "job_name",
        "concurrent_run_limit",
        "job_poll_interval",
        "create_job_kwargs",
        "desc",
        "iam_role_arn",
        "s3_bucket",
        "iam_role_name",
        "update_config",
        "retry_limit",
        "num_of_dpus",
        "script_location",
    },
    "S3Hook": {"transfer_config_args", "aws_conn_id", "extra_args"},
}


def get_aws_hooks_modules():
    """Parse Amazon Provider metadata and find all hooks based on `AwsGenericHook` and return it."""
    import airflow.providers.amazon.aws.hooks as aws_hooks

    hooks_dir = Path(aws_hooks.__path__[0])
    if not hooks_dir.exists():
        msg = f"Amazon Provider hooks directory not found: {hooks_dir.__fspath__()!r}"
        raise FileNotFoundError(msg)
    elif not hooks_dir.is_dir():
        raise NotADirectoryError(hooks_dir.__fspath__())

    for module in sorted(hooks_dir.glob("*.py")):
        name = module.stem
        if name.startswith("_"):
            continue
        module_string = f"airflow.providers.amazon.aws.hooks.{name}"

        yield pytest.param(module_string, id=name)


def get_aws_hooks_from_module(hook_module: str) -> list[tuple[type[AwsGenericHook], str]]:
    try:
        imported_module = import_module(hook_module)
    except AirflowOptionalProviderFeatureException as ex:
        pytest.skip(str(ex))
    else:
        hooks = []
        for name, o in vars(imported_module).items():
            if name in BASE_AWS_HOOKS:
                continue

            if isinstance(o, type) and o.__module__ != "builtins" and issubclass(o, AwsGenericHook):
                hooks.append((o, name))
        return hooks


def validate_hook(hook: type[AwsGenericHook], hook_name: str, hook_module: str) -> tuple[bool, str | None]:
    hook_extra_parameters = set()
    for k, v in inspect.signature(hook.__init__).parameters.items():
        if v.kind == inspect.Parameter.VAR_POSITIONAL:
            k = "*args"
        elif v.kind == inspect.Parameter.VAR_KEYWORD:
            k = "**kwargs"

        hook_extra_parameters.add(k)
    hook_extra_parameters.difference_update({"self", "*args", "**kwargs"})

    allowed_parameters = ALLOWED_THICK_HOOKS_PARAMETERS.get(hook_name, set())
    if allowed_parameters:
        # Remove historically allowed parameters for Thick Wrapped Hooks
        hook_extra_parameters -= allowed_parameters

    if not hook_extra_parameters:
        # No additional arguments found
        return True, None

    if not allowed_parameters:
        msg = (
            f"'{hook_module}.{hook_name}' has additional attributes "
            f"{', '.join(map(repr, hook_extra_parameters))}. "
            "Expected that all `boto3` related hooks (based on `AwsGenericHook` or `AwsBaseHook`) "
            "should not use additional attributes in class constructor, "
            "please move them to method signatures. "
            f"Make sure that {hook_name!r} constructor has signature `def __init__(self, *args, **kwargs):`"
        )
    else:
        msg = (
            f"'{hook_module}.{hook_name}' allowed only "
            f"{', '.join(map(repr, allowed_parameters))} additional attributes, "
            f"but got extra parameters {', '.join(map(repr, hook_extra_parameters))}. "
            "Please move additional attributes from class constructor into method signatures. "
        )

    return False, msg


@pytest.mark.parametrize("hook_module", get_aws_hooks_modules())
def test_expected_thin_hooks(hook_module: str):
    """
    Test Amazon provider Hooks' signatures.

    All hooks should provide thin wrapper around boto3 / aiobotocore,
    that mean we should not define additional parameters in Hook parameters.
    It should be defined in appropriate methods.

    .. code-block:: python

        # Bad: Thick wrapper
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


        class AwsServiceName(AwsBaseHook):
            def __init__(self, foo: str, spam: str, *args, **kwargs) -> None:
                kwargs.update(dict(client_type="service", resource_type=None))
                super().__init__(*args, **kwargs)
                self.foo = foo
                self.spam = spam

            def method1(self):
                if self.foo == "bar":
                    ...

            def method2(self):
                if self.spam == "egg":
                    ...

    .. code-block:: python

        # Good: Thin wrapper
        class AwsServiceName(AwsBaseHook):
            def __init__(self, *args, **kwargs) -> None:
                kwargs.update(dict(client_type="service", resource_type=None))
                super().__init__(*args, **kwargs)

            def method1(self, foo: str):
                if foo == "bar":
                    ...

            def method2(self, spam: str):
                if spam == "egg":
                    ...

    """
    hooks = get_aws_hooks_from_module(hook_module)
    if not hooks:
        pytest.skip(reason=f"Module {hook_module!r} doesn't contain subclasses of `AwsGenericHook`.")

    errors = [
        message
        for valid, message in (validate_hook(hook, hook_name, hook_module) for hook, hook_name in hooks)
        if not valid and message
    ]

    if errors:
        errors_msg = "\n * ".join(errors)
        pytest.fail(reason=f"Found errors in {hook_module}:\n * {errors_msg}")
