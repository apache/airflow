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
from __future__ import annotations

import copy
import itertools
import re
import signal
from datetime import datetime
from functools import reduce
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
    Iterable,
    Mapping,
    MutableMapping,
    TypeVar,
    cast,
)

from lazy_object_proxy import Proxy

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    import jinja2

    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.context import Context

KEY_REGEX = re.compile(r"^[\w.-]+$")
GROUP_KEY_REGEX = re.compile(r"^[\w-]+$")
CAMELCASE_TO_SNAKE_CASE_REGEX = re.compile(r"(?!^)([A-Z]+)")

T = TypeVar("T")
S = TypeVar("S")


def validate_key(k: str, max_length: int = 250):
    """Validate value used as a key."""
    if not isinstance(k, str):
        raise TypeError(f"The key has to be a string and is {type(k)}:{k}")
    if len(k) > max_length:
        raise AirflowException(
            f"The key: {k} has to be less than {max_length} characters"
        )
    if not KEY_REGEX.match(k):
        raise AirflowException(
            f"The key {k!r} has to be made of alphanumeric characters, dashes, "
            f"dots and underscores exclusively"
        )


def validate_instance_args(instance: object, expected_arg_types: dict[str, Any]) -> None:
    """Validate that the instance has the expected types for the arguments."""
    for arg_name, expected_arg_type in expected_arg_types.items():
        instance_arg_value = getattr(instance, arg_name, None)
        if instance_arg_value is not None and not isinstance(
            instance_arg_value, expected_arg_type
        ):
            raise TypeError(
                f"'{arg_name}' has an invalid type {type(instance_arg_value)} with value "
                f"{instance_arg_value}, expected type is {expected_arg_type}"
            )


def validate_group_key(k: str, max_length: int = 200):
    """Validate value used as a group key."""
    if not isinstance(k, str):
        raise TypeError(f"The key has to be a string and is {type(k)}:{k}")
    if len(k) > max_length:
        raise AirflowException(f"The key has to be less than {max_length} characters")
    if not GROUP_KEY_REGEX.match(k):
        raise AirflowException(
            f"The key {k!r} has to be made of alphanumeric characters, dashes and underscores exclusively"
        )


def alchemy_to_dict(obj: Any) -> dict | None:
    """Transform a SQLAlchemy model instance into a dictionary."""
    if not obj:
        return None
    output = {}
    for col in obj.__table__.columns:
        value = getattr(obj, col.name)
        if isinstance(value, datetime):
            value = value.isoformat()
        output[col.name] = value
    return output


def ask_yesno(question: str, default: bool | None = None) -> bool:
    """Get a yes or no answer from the user."""
    yes = {"yes", "y"}
    no = {"no", "n"}

    print(question)
    while True:
        choice = input().lower()
        if choice == "" and default is not None:
            return default
        if choice in yes:
            return True
        if choice in no:
            return False
        print("Please respond with y/yes or n/no.")


def prompt_with_timeout(question: str, timeout: int, default: bool | None = None) -> bool:
    """Ask the user a question and timeout if they don't respond."""

    def handler(signum, frame):
        raise AirflowException(f"Timeout {timeout}s reached")

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout)
    try:
        return ask_yesno(question, default)
    finally:
        signal.alarm(0)


def is_container(obj: Any) -> bool:
    """Test if an object is a container (iterable) but not a string."""
    if isinstance(obj, Proxy):
        # Proxy of any object is considered a container because it implements __iter__
        # to forward the call to the lazily initialized object
        # Unwrap Proxy before checking __iter__ to evaluate the proxied object
        obj = obj.__wrapped__
    return hasattr(obj, "__iter__") and not isinstance(obj, str)


def as_tuple(obj: Any) -> tuple:
    """Return obj as a tuple if obj is a container, otherwise return a tuple containing obj."""
    if is_container(obj):
        return tuple(obj)
    else:
        return tuple([obj])


def chunks(items: list[T], chunk_size: int) -> Generator[list[T], None, None]:
    """Yield successive chunks of a given size from a list of items."""
    if chunk_size <= 0:
        raise ValueError("Chunk size must be a positive integer")
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def reduce_in_chunks(
    fn: Callable[[S, list[T]], S], iterable: list[T], initializer: S, chunk_size: int = 0
):
    """Split the list of items into chunks of a given size and pass each chunk through the reducer."""
    if not iterable:
        return initializer
    if chunk_size == 0:
        chunk_size = len(iterable)
    return reduce(fn, chunks(iterable, chunk_size), initializer)


def as_flattened_list(iterable: Iterable[Iterable[T]]) -> list[T]:
    """
    Return an iterable with one level flattened.

    >>> as_flattened_list((("blue", "red"), ("green", "yellow", "pink")))
    ['blue', 'red', 'green', 'yellow', 'pink']
    """
    return [e for i in iterable for e in i]


def parse_template_string(
    template_string: str,
) -> tuple[str | None, jinja2.Template | None]:
    """Parse Jinja template string."""
    import jinja2

    if "{{" in template_string:  # jinja mode
        return None, jinja2.Template(template_string)
    else:
        return template_string, None


def render_log_filename(ti: TaskInstance, try_number, filename_template) -> str:
    """
    Given task instance, try_number, filename_template, return the rendered log filename.

    :param ti: task instance
    :param try_number: try_number of the task
    :param filename_template: filename template, which can be jinja template or
        python string template
    """
    filename_template, filename_jinja_template = parse_template_string(filename_template)
    if filename_jinja_template:
        jinja_context = ti.get_template_context()
        jinja_context["try_number"] = try_number
        return render_template_to_string(filename_jinja_template, jinja_context)

    return filename_template.format(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        execution_date=ti.execution_date.isoformat(),
        try_number=try_number,
    )


def convert_camel_to_snake(camel_str: str) -> str:
    """Convert CamelCase to snake_case."""
    return CAMELCASE_TO_SNAKE_CASE_REGEX.sub(r"_\1", camel_str).lower()


def merge_dicts(dict1: dict, dict2: dict) -> dict:
    """
    Merge two dicts recursively, returning new dict (input dict is not mutated).

    Lists are not concatenated. Items in dict2 overwrite those also found in dict1.
    """
    merged = dict1.copy()
    for k, v in dict2.items():
        if k in merged and isinstance(v, dict):
            merged[k] = merge_dicts(merged.get(k, {}), v)
        else:
            merged[k] = v
    return merged


def partition(
    pred: Callable[[T], bool], iterable: Iterable[T]
) -> tuple[Iterable[T], Iterable[T]]:
    """Use a predicate to partition entries into false entries and true entries."""
    iter_1, iter_2 = itertools.tee(iterable)
    return itertools.filterfalse(pred, iter_1), filter(pred, iter_2)


def build_airflow_url_with_query(query: dict[str, Any]) -> str:
    """
    Build airflow url using base_url and default_view and provided query.

    For example:
    http://0.0.0.0:8000/base/graph?dag_id=my-task&root=&execution_date=2020-10-27T10%3A59%3A25.615587
    """
    import flask

    view = conf.get_mandatory_value("webserver", "dag_default_view").lower()
    return flask.url_for(f"Airflow.{view}", **query)


# The 'template' argument is typed as Any because the jinja2.Template is too
# dynamic to be effectively type-checked.
def render_template(
    template: Any, context: MutableMapping[str, Any], *, native: bool
) -> Any:
    """
    Render a Jinja2 template with given Airflow context.

    The default implementation of ``jinja2.Template.render()`` converts the
    input context into dict eagerly many times, which triggers deprecation
    messages in our custom context class. This takes the implementation apart
    and retain the context mapping without resolving instead.

    :param template: A Jinja2 template to render.
    :param context: The Airflow task context to render the template with.
    :param native: If set to *True*, render the template into a native type. A
        DAG can enable this with ``render_template_as_native_obj=True``.
    :returns: The render result.
    """
    context = copy.copy(context)
    env = template.environment
    if template.globals:
        context.update((k, v) for k, v in template.globals.items() if k not in context)
    try:
        nodes = template.root_render_func(
            env.context_class(env, context, template.name, template.blocks)
        )
    except Exception:
        env.handle_exception()  # Rewrite traceback to point to the template.
    if native:
        import jinja2.nativetypes

        return jinja2.nativetypes.native_concat(nodes)
    return "".join(nodes)


def render_template_to_string(template: jinja2.Template, context: Context) -> str:
    """Shorthand to ``render_template(native=False)`` with better typing support."""
    return render_template(
        template, cast(MutableMapping[str, Any], context), native=False
    )


def render_template_as_native(template: jinja2.Template, context: Context) -> Any:
    """Shorthand to ``render_template(native=True)`` with better typing support."""
    return render_template(template, cast(MutableMapping[str, Any], context), native=True)


def exactly_one(*args) -> bool:
    """
    Return True if exactly one of *args is "truthy", and False otherwise.

    If user supplies an iterable, we raise ValueError and force them to unpack.
    """
    if is_container(args[0]):
        raise ValueError(
            "Not supported for iterable args. Use `*` to unpack your iterable in the function call."
        )
    return sum(map(bool, args)) == 1


def at_most_one(*args) -> bool:
    """
    Return True if at most one of *args is "truthy", and False otherwise.

    NOTSET is treated the same as None.

    If user supplies an iterable, we raise ValueError and force them to unpack.
    """

    def is_set(val):
        if val is NOTSET:
            return False
        else:
            return bool(val)

    return sum(map(is_set, args)) in (0, 1)


def prune_dict(val: Any, mode="strict"):
    """
    Given dict ``val``, returns new dict based on ``val`` with all empty elements removed.

    What constitutes "empty" is controlled by the ``mode`` parameter.  If mode is 'strict'
    then only ``None`` elements will be removed.  If mode is ``truthy``, then element ``x``
    will be removed if ``bool(x) is False``.
    """

    def is_empty(x):
        if mode == "strict":
            return x is None
        elif mode == "truthy":
            return bool(x) is False
        raise ValueError("allowable values for `mode` include 'truthy' and 'strict'")

    if isinstance(val, dict):
        new_dict = {}
        for k, v in val.items():
            if is_empty(v):
                continue
            elif isinstance(v, (list, dict)):
                new_val = prune_dict(v, mode=mode)
                if not is_empty(new_val):
                    new_dict[k] = new_val
            else:
                new_dict[k] = v
        return new_dict
    elif isinstance(val, list):
        new_list = []
        for v in val:
            if is_empty(v):
                continue
            elif isinstance(v, (list, dict)):
                new_val = prune_dict(v, mode=mode)
                if not is_empty(new_val):
                    new_list.append(new_val)
            else:
                new_list.append(v)
        return new_list
    else:
        return val


def prevent_duplicates(
    kwargs1: dict[str, Any], kwargs2: Mapping[str, Any], *, fail_reason: str
) -> None:
    """
    Ensure *kwargs1* and *kwargs2* do not contain common keys.

    :raises TypeError: If common keys are found.
    """
    duplicated_keys = set(kwargs1).intersection(kwargs2)
    if not duplicated_keys:
        return
    if len(duplicated_keys) == 1:
        raise TypeError(f"{fail_reason} argument: {duplicated_keys.pop()}")
    duplicated_keys_display = ", ".join(sorted(duplicated_keys))
    raise TypeError(f"{fail_reason} arguments: {duplicated_keys_display}")
