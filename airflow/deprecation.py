#
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
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state
"""Airflow deprecation utilities."""
from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

import re2
from deprecated import deprecated as _deprecated
from deprecated.classic import ClassicAdapter

if TYPE_CHECKING:
    from typing import Any, Callable, Literal


class RemovedInAirflow3Warning(DeprecationWarning):
    """Issued for usage of deprecated features that will be removed in Airflow3."""

    deprecated_since: str | None = None
    "Indicates the airflow version that started raising this deprecation warning"


class AirflowProviderDeprecationWarning(DeprecationWarning):
    """Issued for usage of deprecated features of Airflow provider."""

    deprecated_provider_since: str | None = None
    "Indicates the provider version that started raising this deprecation warning"


class AirflowDeprecationAdapter(ClassicAdapter):
    """Gets the deprecation message according to the wrapped object type and other details provided."""

    def __init__(
        self,
        instructions: str | None = None,
        use_instead: str | None = None,
        deprecated_release: str | None = None,
        removal_release: str | None = None,
        action: Literal["default", "error", "ignore", "always", "module", "once"] | None = None,
        category: type[DeprecationWarning] = DeprecationWarning,
    ):
        """Initializes a deprecation warning configuration object.

        :param instructions: Additional guidance to provide to the user about the deprecation.
            Please also see other arguments: `use_instead`, `deprecated_release` and `removal_release` to
            provide the most common information about the deprecation. Instructions should be used as a last
            resort way of adding custom information to the deprecation message.

            If `use_instead` is provided, this argument should contain any additional instructions needed,
            f.e. if `use_instead=airflow.providers.common.sql.operators.sql.SqlOperator`, we can provide:
            `instructions="with defferable=True, to maintain current behaviour"`. Then the final message
            would be similar to:

            `Consider using `[...]SqlOperator` instead with defferable=True, to maintain current behaviour`

            If `use_instead` is **not** provided, this message will be appended to the end of the
            deprecation message and should contain all additional instructions or guidance.
        :param use_instead: Specifies an alternative to the deprecated entity. This must be a full path to
            the suggested alternative, f.e. `airflow.providers.common.sql.operators.sql.BaseSqlOperator`.
            Set to None if there's no direct replacement.
        :param deprecated_release: The package name and version number in which the entity was deprecated,
            formatted as `<package_name>==<package_version>`, f.e. `apache-airflow-providers-common==1.2.0`,
            or `apache-airflow==2.8.0`. Set to None if the version is unknown or not applicable.
        :param removal_release: The package name and version number in which the deprecated entity is
            expected to be removed. Similar to `deprecated_release`, this should be formatted as
            `<package_name>==<package_version>`, f.e. `apache-airflow-providers-common==1.2.0`,
            or `apache-airflow==2.8.0`. Set to None if the removal version is unknown or not applicable.
        :param action: The action to take when the deprecation is encountered. This argument allow you to
            locally change the warning filtering This could be 'always', 'default', 'ignore', 'error',
            'once', or 'module'. Refer to the `The Warnings Filter`_ in the Python documentation
            for a description of these actions.
        :param category: The warning category to be used for the deprecation warning. By default,
            it is set to `DeprecationWarning`. This should be a subclass of `DeprecationWarning`.
        """
        super().__init__(action=action, category=category)
        self.instructions = instructions
        self.use_instead = use_instead
        self.deprecated_release = deprecated_release
        self.removal_release = removal_release

    def get_deprecated_msg(self, wrapped: Callable, instance: Any) -> str:
        """Generates a deprecation message for wrapped callable.

        :param wrapped: Deprecated entity.
        :param instance: The instance to which the callable belongs. (not used)
        :return: A formatted deprecation message with all the details.
        """
        entity_type = "class" if inspect.isclass(wrapped) else "function (or method)"
        entity_path = get_callable_full_path(wrapped)
        msg = f"The {entity_type} `{entity_path}` is deprecated "
        if self.deprecated_release:
            msg += f"since version `{self.deprecated_release}` "

        if self.removal_release:
            msg += f"and it will be removed in version `{self.removal_release}`"
        else:
            msg += "and it will be removed in future release"

        msg += ". "

        if self.use_instead:
            msg += f"Consider using `{self.use_instead}` instead "

        if self.instructions:
            msg += f"{self.instructions}"

        msg = msg.rstrip() if msg.rstrip().endswith(".") else msg + "."
        return msg


def get_callable_full_path(callable_obj: Callable) -> str:
    """Get the full path of a callable object including its module and qualified name.

    This function takes a callable object and returns its full path, which includes the module name and
    the qualified name (if available). If the callable object is not associated with a module or does
    not have a qualified name, it returns the string representation of the callable object itself.

    :param callable_obj: The callable object for which you want to obtain the full path.
    :return: The full path of the callable object, including module name and qualified name (if available).

    :examples:
    >> from mymodule import my_function
    >> get_callable_full_path(my_function)
    'mymodule.my_function'

    >> get_callable_full_path(len)
    'builtins.len'

    >> get_callable_full_path(lambda x: x * 2)
    '<lambda>'
    """
    module_name = getattr(callable_obj, "__module__", "")
    qualified_name = getattr(callable_obj, "__qualname__", "")

    full_path = f"{module_name}.{qualified_name}".strip(".")

    if not module_name or not full_path:
        return str(callable_obj)
    return full_path


def validate_release_string(release_string: str | None, arg_name: str) -> None:
    """Check if a release string follows the format 'apache-airflow(-providers-<name>)==<semver>'.

    :param release_string: The release string to be validated. If None, validation is skipped.
    :param arg_name: The name of the argument being validated, for error message purposes.
    :raises ValueError: If the release string does not follow the specified format.

    :examples:
    >> validate_release_string("apache-airflow==1.10.0", "my_arg")
    # No error is raised for a valid release string.

    >> validate_release_string("1.2.0", "my_arg")
    # ValueError is raised for an invalid release string.

    >> validate_release_string(None, "my_arg")
    # Validation is skipped when release_string is None.
    """
    if release_string is None:
        return
    pattern = r"^apache-airflow(-providers-[a-zA-Z-]+)?==\d+\.\d+\.\d+.*$"
    if not bool(re2.match(pattern, release_string)):
        raise ValueError(
            f"`{arg_name}` must follow the format 'apache-airflow(-providers-<name>)==<semver>'."
        )


def deprecated(
    *args,
    use_instead: str | None = None,
    instructions: str | None = None,
    deprecated_release: str | None = None,
    removal_release: str | None = None,
    action: Literal["default", "error", "ignore", "always", "module", "once"] | None = None,
    category: type[DeprecationWarning] = DeprecationWarning,
    adapter_cls: type[ClassicAdapter] = AirflowDeprecationAdapter,
    **kwargs,
):
    """A decorator which can be used to mark functions, classes and methods as deprecated.

    It will result in a warning being emitted when the function, class or method is used.

    :param args: A single string arg is allowed, that is treated as `instructions` argument. If provided,
        instructions kwarg must be None. See below for the description.
    :param instructions: Additional guidance to provide to the user about the deprecation.
        Please use other arguments like `use_instead`, `deprecated_release` and `removal_release` to
        provide the most common information about the deprecation. Instructions should be used as a last
        resort way of adding custom information to the deprecation message.

        If `use_instead` is provided, this argument should contain any additional instructions needed,
        f.e. if `use_instead=NewClass.new_method`, we can provide:
        `instructions="with defferable=True, to maintain current behaviour"`. Then the final message
        would be similar to:

        `Consider using `NewClass.new_method` instead with defferable=True, to maintain current behaviour`

        If `use_instead` is **not** provided, this message should contain all additional instructions
        or guidance to provide to the user about the deprecation.
    :param use_instead: Specifies an alternative to the deprecated entity. This must be a full path to
        the suggested alternative, f.e. `airflow.providers.common.sql.operators.sql.BaseSqlOperator`.
        Set to None if there's no direct replacement.
    :param deprecated_release: The package name and version number in which the entity was deprecated,
        formatted as `<package_name>==<package_version>`, f.e. `apache-airflow-providers-common==1.2.0`,
        or `apache-airflow==2.8.0`. Set to None if the version is unknown or not applicable.
    :param removal_release: The package name and version number in which the deprecated entity is
        expected to be removed. Similar to `deprecated_release`, this should be formatted as
        `<package_name>==<package_version>`, f.e. `apache-airflow-providers-common==1.2.0`,
        or `apache-airflow==2.8.0`. Set to None if the removal version is unknown or not applicable.
    :param action: The action to take when the deprecation is encountered. This argument allow you to
        locally change the warning filtering This could be 'always', 'default', 'ignore', 'error',
        'once', or 'module'. Refer to the `The Warnings Filter`_ in the Python documentation
        for a description of these actions.
    :param category: The warning category to be used for the deprecation warning. By default,
        it is set to `DeprecationWarning`. This should be a subclass of `DeprecationWarning`.
    :param adapter_cls: Adapter class that is used to get the deprecation message
     according to the wrapped object type and other arguments. This should be a subclass of `ClassicAdapter`.
    :param kwargs: Other arguments that will be passed to adapter.

    :raise ValueError: When `instructions` are provided both as argument and a keyword argument.
    :raise TypeError: When additional args are provided. You can only provide a single string arg,
        the second one is added automatically and should be a wrapped callable. Any other args are ignored.
    :raise ValueError: When `use_instead` is provided not as a full import path, f.e.`BaseSqlOperator`
        instead of `airflow.providers.common.sql.operators.sql.BaseSqlOperator`.
    """
    # Treat first arg as instructions, when called like @deprecated("switch to class X")
    if args and isinstance(args[0], (bytes, str)):
        if instructions:
            raise ValueError(
                "Both instructions and an string argument are provided, "
                "but only one of them is allowed. Please choose either "
                "instructions as a keyword argument or provide an argument string."
            )
        instructions = str(args[0])
        args = args[1:]

    # When called with no args (@deprecated), a wrapped entity is passed as first arg
    if args and not callable(args[0]):
        raise TypeError(f"Arg provided should be a callable and not {repr(type(args[0]))}")

    if use_instead and not use_instead.startswith("airflow."):
        raise ValueError(f"Provide full import path as `use_instead` argument, instead of `{use_instead}`.")

    validate_release_string(deprecated_release, "deprecated_release")
    validate_release_string(removal_release, "removal_release")

    kwargs = {
        **kwargs,
        "instructions": instructions,
        "use_instead": use_instead,
        "deprecated_release": deprecated_release,
        "removal_release": removal_release,
        "action": action,
        "category": category,
        "adapter_cls": adapter_cls,
    }
    return _deprecated(*args, **kwargs)
