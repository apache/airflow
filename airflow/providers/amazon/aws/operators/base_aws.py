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

import warnings
from functools import cached_property
from typing import Any, Generic, Sequence, TypeVar

from typing_extensions import final

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

_AwsHook = TypeVar("_AwsHook", bound=AwsGenericHook)
_REGION_MSG = "`region` is deprecated and will be removed in the future. Please use `region_name` instead."


class AwsBaseOperator(BaseOperator, Generic[_AwsHook]):
    """
    Base AWS (Amazon) Operator Class to build operators on top of AWS Hooks.

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.hooks.foo_bar import FooBarThinHook, FooBarThickHook


        class AwsFooBarOperator(AwsBaseOperator[FooBarThinHook]):
            aws_hook_class = FooBarThinHook

            def execute(self, context):
                pass


        class AwsFooBarOperator2(AwsBaseOperator[FooBarThickHook]):
            aws_hook_class = FooBarThickHook

            def __init__(self, *, spam: str, **kwargs):
                super().__init__(**kwargs)
                self.spam = spam

            @property
            def _hook_parameters(self):
                return {**super()._hook_parameters, "spam": self.spam}

            def execute(self, context):
                pass

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class: type[_AwsHook]
    template_fields: Sequence[str] = (
        "aws_conn_id",
        "region_name",
        "botocore_config",
    )

    def __init__(
        self,
        *,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        **kwargs,
    ):
        # Validate if ``aws_hook_class`` is properly set.
        if hasattr(self, "aws_hook_class"):
            try:
                if not issubclass(self.aws_hook_class, AwsGenericHook):
                    raise TypeError
            except TypeError:
                # Raise if ``aws_hook_class`` is not a class or not a subclass of Generic/Base AWS Hook
                raise AirflowException(
                    f"Class attribute '{type(self).__name__}.aws_hook_class' "
                    f"is not a subclass of AwsGenericHook."
                ) from None
        else:
            raise AirflowException(f"Class attribute '{type(self).__name__}.aws_hook_class' should be set.")

        if region := kwargs.pop("region", None):
            warnings.warn(_REGION_MSG, AirflowProviderDeprecationWarning, stacklevel=3)
            if region_name and region_name != region:
                raise AirflowException(
                    f"Conflicting `region_name` provided, region_name={region_name!r}, region={region!r}."
                )
            region_name = region

        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        """
        Mapping parameters to build boto3-related hooks.

        Only required to be overwritten for thick-wrapped Hooks.
        """
        return {
            "aws_conn_id": self.aws_conn_id,
            "region_name": self.region_name,
            "verify": self.verify,
            "config": self.botocore_config,
        }

    @cached_property
    @final
    def hook(self) -> _AwsHook:
        """
        Return AWS Provider's hook based on ``aws_hook_class``.

        This method implementation should be taken as a final for 
        thin-wrapped Hooks around boto3.  For thick-wrapped Hooks developer 
        should consider to overwrite ``_hook_parameters`` method instead.
        """
        return self.aws_hook_class(**self._hook_parameters)

    @property
    @final
    def region(self) -> str | None:
        """Alias for ``region_name``, used for compatibility (deprecated)."""
        warnings.warn(_REGION_MSG, AirflowProviderDeprecationWarning, stacklevel=3)
        return self.region_name
