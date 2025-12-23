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
This module contains different mixin classes for internal use within the Amazon provider.

.. warning::
    Only for internal usage, this module and all classes might be changed, renamed or removed in the future
    without any further notice.

:meta: private
"""

from __future__ import annotations

from functools import cache, cached_property
from typing import Any, Generic, NamedTuple, TypeVar

from typing_extensions import final

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

AwsHookType = TypeVar("AwsHookType", bound=AwsGenericHook)


class AwsHookParams(NamedTuple):
    """
    Default Aws Hook Parameters storage class.

    :meta private:
    """

    aws_conn_id: str | None
    region_name: str | None
    verify: bool | str | None
    botocore_config: dict[str, Any] | None

    @classmethod
    def from_constructor(
        cls,
        aws_conn_id: str | None,
        region_name: str | None,
        verify: bool | str | None,
        botocore_config: dict[str, Any] | None,
        additional_params: dict,
    ):
        """
        Resolve generic AWS Hooks parameters in class constructor.

        Examples:
         .. code-block:: python

            class AwsFooBarOperator(BaseOperator):
                def __init__(
                    self,
                    *,
                    aws_conn_id: str | None = "aws_default",
                    region_name: str | None = None,
                    verify: bool | str | None = None,
                    botocore_config: dict | None = None,
                    foo: str = "bar",
                    **kwargs,
                ):
                    params = AwsHookParams.from_constructor(
                        aws_conn_id, region_name, verify, botocore_config, additional_params=kwargs
                    )
                    super().__init__(**kwargs)
                    self.aws_conn_id = params.aws_conn_id
                    self.region_name = params.region_name
                    self.verify = params.verify
                    self.botocore_config = params.botocore_config
                    self.foo = foo
        """
        return cls(aws_conn_id, region_name, verify, botocore_config)


class AwsBaseHookMixin(Generic[AwsHookType]):
    """
    Mixin class for AWS Operators, Sensors, etc.

    .. warning::
        Only for internal usage, this class might be changed, renamed or removed in the future
        without any further notice.

    :meta private:
    """

    # Should be assigned in child class
    aws_hook_class: type[AwsHookType]

    aws_conn_id: str | None
    region_name: str | None
    verify: bool | str | None
    botocore_config: dict[str, Any] | None

    def validate_attributes(self):
        """Validate class attributes."""
        if hasattr(self, "aws_hook_class"):  # Validate if ``aws_hook_class`` is properly set.
            try:
                if not issubclass(self.aws_hook_class, AwsGenericHook):
                    raise TypeError
            except TypeError:
                # Raise if ``aws_hook_class`` is not a class or not a subclass of Generic/Base AWS Hook
                raise AttributeError(
                    f"Class attribute '{type(self).__name__}.aws_hook_class' "
                    f"is not a subclass of AwsGenericHook."
                ) from None
        else:
            raise AttributeError(f"Class attribute '{type(self).__name__}.aws_hook_class' should be set.")

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
    def hook(self) -> AwsHookType:
        """
        Return AWS Provider's hook based on ``aws_hook_class``.

        This method implementation should be taken as a final for
        thin-wrapped Hooks around boto3.  For thick-wrapped Hooks developer
        should consider to overwrite ``_hook_parameters`` method instead.
        """
        return self.aws_hook_class(**self._hook_parameters)


@cache
def aws_template_fields(*template_fields: str) -> tuple[str, ...]:
    """Merge provided template_fields with generic one and return in alphabetical order."""
    if not all(isinstance(tf, str) for tf in template_fields):
        msg = (
            "Expected that all provided arguments are strings, but got "
            f"{', '.join(map(repr, template_fields))}."
        )
        raise TypeError(msg)
    return tuple(sorted({"aws_conn_id", "region_name", "verify"} | set(template_fields)))
