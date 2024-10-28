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
import re
from datetime import date, datetime
from typing import Any, Callable

from deprecated import deprecated as standard_deprecated
from deprecated.classic import ClassicAdapter


class AirflowDeprecationAdapter(ClassicAdapter):
    """
    Build a detailed deprecation message based on the wrapped object type and other provided details.

    :param planned_removal_date: The date after which the deprecated object should be removed.
        The recommended date is six months ahead from today. The expected date format is `Month DD, YYYY`,
        for example: `August 22, 2024`.
        This parameter is required if the `planned_removal_release` parameter is not set.
    :param planned_removal_release: The package name and the version in which the deprecated object is
        expected to be removed. The expected format is `<package_name>==<package_version>`, for example
        `apache-airflow==2.10.0` or `apache-airflow-providers-google==10.22.0`.
        This parameter is required if the `planned_removal_date` parameter is not set.
    :param use_instead: Optional. Replacement of the deprecated object.
    :param reason: Optional. The detailed reason for the deprecated object.
    :param instructions: Optional. The detailed instructions for migrating from the deprecated object.
    :param category: Optional. The warning category to be used for the deprecation warning.
    """

    def __init__(
        self,
        planned_removal_date: str | None = None,
        planned_removal_release: str | None = None,
        use_instead: str | None = None,
        reason: str | None = None,
        instructions: str | None = None,
        category: type[DeprecationWarning] = DeprecationWarning,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.planned_removal_date: date | None = self._validate_date(planned_removal_date)
        self.planned_removal_release: str | None = self._validate_removal_release(
            planned_removal_release
        )
        self.use_instead: str | None = use_instead
        self.reason: str = reason or ""
        self.instructions: str | None = instructions
        self.category: type[DeprecationWarning] = category
        self._validate_fields()

    def get_deprecated_msg(self, wrapped: Callable, instance: Any):
        """
        Generate a deprecation message for wrapped callable.

        :param wrapped: Deprecated entity.
        :param instance: The instance to which the callable belongs. (not used)
        :return: A formatted deprecation message with all the details.
        """
        entity_type = self.entity_type(entity=wrapped)
        entity_path = self.entity_path(entity=wrapped)
        sunset = self.sunset_message()
        replacement = self.replacement_message()
        msg = f"The {entity_type} `{entity_path}` is deprecated and will be removed {sunset}. {replacement}"
        if self.reason:
            msg += f" The reason is: {self.reason}"
        if self.instructions:
            msg += f" Instructions: {self.instructions}"
        return msg

    @staticmethod
    def _validate_date(value: str | None) -> date | None:
        if value:
            try:
                return datetime.strptime(value, "%B %d, %Y").date()
            except ValueError as ex:
                error_message = (
                    f"Invalid date '{value}'. "
                    f"The expected format is 'Month DD, YYYY', for example 'August 22, 2024'."
                )
                raise ValueError(error_message) from ex
        return None

    @staticmethod
    def _validate_removal_release(value: str | None) -> str | None:
        if value:
            pattern = r"^apache-airflow(-providers-[a-zA-Z-]+)?==\d+\.\d+\.\d+.*$"
            if not bool(re.match(pattern, value)):
                raise ValueError(
                    f"`{value}` must follow the format 'apache-airflow(-providers-<name>)==<X.Y.Z>'."
                )
        return value

    def _validate_fields(self):
        msg = "Only one of two parameters must be set: `planned_removal_date` or 'planned_removal_release'."
        if self.planned_removal_release and self.planned_removal_date:
            raise ValueError(f"{msg} You specified both.")

    @staticmethod
    def entity_type(entity: Callable) -> str:
        return "class" if inspect.isclass(entity) else "function (or method)"

    @staticmethod
    def entity_path(entity: Callable) -> str:
        module_name = getattr(entity, "__module__", "")
        qualified_name = getattr(entity, "__qualname__", "")
        full_path = f"{module_name}.{qualified_name}".strip(".")

        if module_name and full_path:
            return full_path
        return str(entity)

    def sunset_message(self) -> str:
        if self.planned_removal_date:
            return f"after {self.planned_removal_date.strftime('%B %d, %Y')}"
        if self.planned_removal_release:
            return f"since version {self.planned_removal_release}"
        return "in the future"

    def replacement_message(self):
        if self.use_instead:
            replacements = ", ".join(
                f"`{replacement}`" for replacement in self.use_instead.split(", ")
            )
            return f"Please use {replacements} instead."
        return "There is no replacement."


def deprecated(
    *args,
    planned_removal_date: str | None = None,
    planned_removal_release: str | None = None,
    use_instead: str | None = None,
    reason: str | None = None,
    instructions: str | None = None,
    adapter_cls: type[AirflowDeprecationAdapter] = AirflowDeprecationAdapter,
    **kwargs,
):
    """
    Mark a class, method or a function deprecated.

    :param planned_removal_date: The date after which the deprecated object should be removed.
        The recommended date is six months ahead from today. The expected date format is `Month DD, YYYY`,
        for example: `August 22, 2024`.
        This parameter is required if the `planned_removal_release` parameter is not set.
    :param planned_removal_release: The package name and the version in which the deprecated object is
        expected to be removed. The expected format is `<package_name>==<package_version>`, for example
        `apache-airflow==2.10.0` or `apache-airflow-providers-google==10.22.0`.
        This parameter is required if the `planned_removal_date` parameter is not set.
    :param use_instead: Optional. Replacement of the deprecated object.
    :param reason: Optional. The detailed reason for the deprecated object.
    :param instructions: Optional. The detailed instructions for migrating from the deprecated object.
    :param adapter_cls: Optional. Adapter class that is used to get the deprecation message
        This should be a subclass of `AirflowDeprecationAdapter`.
    """
    _kwargs = {
        **kwargs,
        "planned_removal_date": planned_removal_date,
        "planned_removal_release": planned_removal_release,
        "use_instead": use_instead,
        "reason": reason,
        "instructions": instructions,
        "adapter_cls": adapter_cls,
    }
    return standard_deprecated(*args, **_kwargs)
