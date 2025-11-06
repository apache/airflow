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

import contextlib
import inspect
import itertools
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence, Sized
from functools import singledispatch
from typing import TYPE_CHECKING, Any, overload

import attrs

from airflow.sdk import TriggerRule
from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
from airflow.sdk.definitions._internal.mixins import DependencyMixin, ResolveMixin
from airflow.sdk.definitions._internal.setup_teardown import SetupTeardownContext
from airflow.sdk.definitions._internal.types import NOTSET, is_arg_set
from airflow.sdk.exceptions import AirflowException, XComNotFound
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence
from airflow.sdk.execution_time.xcom import BaseXCom

if TYPE_CHECKING:
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.edges import EdgeModifier
    from airflow.sdk.types import Operator

# Callable objects contained by MapXComArg. We only accept callables from
# the user, but deserialize them into strings in a serialized XComArg for
# safety (those callables are arbitrary user code).
MapCallables = Sequence[Callable[[Any], Any]]


class XComArg(ResolveMixin, DependencyMixin):
    """
    Reference to an XCom value pushed from another operator.

    The implementation supports::

        xcomarg >> op
        xcomarg << op
        op >> xcomarg  # By BaseOperator code
        op << xcomarg  # By BaseOperator code

    **Example**: The moment you get a result from any operator (decorated or regular) you can ::

        any_op = AnyOperator()
        xcomarg = XComArg(any_op)
        # or equivalently
        xcomarg = any_op.output
        my_op = MyOperator()
        my_op >> xcomarg

    This object can be used in legacy Operators via Jinja.

    **Example**: You can make this result to be part of any generated string::

        any_op = AnyOperator()
        xcomarg = any_op.output
        op1 = MyOperator(my_text_message=f"the value is {xcomarg}")
        op2 = MyOperator(my_text_message=f"the value is {xcomarg['topic']}")

    :param operator: Operator instance to which the XComArg references.
    :param key: Key used to pull the XCom value. Defaults to *XCOM_RETURN_KEY*,
        i.e. the referenced operator's return value.
    """

    @overload
    def __new__(cls: type[XComArg], operator: Operator, key: str = BaseXCom.XCOM_RETURN_KEY) -> XComArg:
        """Execute when the user writes ``XComArg(...)`` directly."""

    @overload
    def __new__(cls: type[XComArg]) -> XComArg:
        """Execute by Python internals from subclasses."""

    def __new__(cls, *args, **kwargs) -> XComArg:
        if cls is XComArg:
            return PlainXComArg(*args, **kwargs)
        return super().__new__(cls)

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        raise NotImplementedError()

    @staticmethod
    def iter_xcom_references(arg: Any) -> Iterator[tuple[Operator, str]]:
        """
        Return XCom references in an arbitrary value.

        Recursively traverse ``arg`` and look for XComArg instances in any
        collection objects, and instances with ``template_fields`` set.
        """
        if isinstance(arg, ResolveMixin):
            yield from arg.iter_references()
        elif isinstance(arg, (tuple, set, list)):
            for elem in arg:
                yield from XComArg.iter_xcom_references(elem)
        elif isinstance(arg, dict):
            for elem in arg.values():
                yield from XComArg.iter_xcom_references(elem)
        elif isinstance(arg, AbstractOperator):
            for attr in arg.template_fields:
                yield from XComArg.iter_xcom_references(getattr(arg, attr))

    @staticmethod
    def apply_upstream_relationship(op: DependencyMixin, arg: Any):
        """
        Set dependency for XComArgs.

        This looks for XComArg objects in ``arg`` "deeply" (looking inside
        collections objects and classes decorated with ``template_fields``), and
        sets the relationship to ``op`` on any found.
        """
        for operator, _ in XComArg.iter_xcom_references(arg):
            op.set_upstream(operator)

    @property
    def roots(self) -> list[Operator]:
        """Required by DependencyMixin."""
        return [op for op, _ in self.iter_references()]

    @property
    def leaves(self) -> list[Operator]:
        """Required by DependencyMixin."""
        return [op for op, _ in self.iter_references()]

    def set_upstream(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ):
        """Proxy to underlying operator set_upstream method. Required by DependencyMixin."""
        for operator, _ in self.iter_references():
            operator.set_upstream(task_or_task_list, edge_modifier)

    def set_downstream(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ):
        """Proxy to underlying operator set_downstream method. Required by DependencyMixin."""
        for operator, _ in self.iter_references():
            operator.set_downstream(task_or_task_list, edge_modifier)

    def _serialize(self) -> dict[str, Any]:
        """
        Serialize an XComArg.

        The implementation should be the inverse function to ``deserialize``,
        returning a data dict converted from this XComArg derivative. DAG
        serialization does not call this directly, but ``serialize_xcom_arg``
        instead, which adds additional information to dispatch deserialization
        to the correct class.
        """
        raise NotImplementedError()

    def map(self, f: Callable[[Any], Any]) -> MapXComArg:
        return MapXComArg(self, [f])

    def zip(self, *others: XComArg, fillvalue: Any = NOTSET) -> ZipXComArg:
        return ZipXComArg([self, *others], fillvalue=fillvalue)

    def concat(self, *others: XComArg) -> ConcatXComArg:
        return ConcatXComArg([self, *others])

    def resolve(self, context: Mapping[str, Any]) -> Any:
        raise NotImplementedError()

    def __enter__(self):
        if not self.operator.is_setup and not self.operator.is_teardown:
            raise AirflowException("Only setup/teardown tasks can be used as context managers.")
        SetupTeardownContext.push_setup_teardown_task(self.operator)
        return SetupTeardownContext

    def __exit__(self, exc_type, exc_val, exc_tb):
        SetupTeardownContext.set_work_task_roots_and_leaves()


@attrs.define
class PlainXComArg(XComArg):
    """
    Reference to one single XCom without any additional semantics.

    This class should not be accessed directly, but only through XComArg. The
    class inheritance chain and ``__new__`` is implemented in this slightly
    convoluted way because we want to

    a. Allow the user to continue using XComArg directly for the simple
       semantics (see documentation of the base class for details).
    b. Make ``isinstance(thing, XComArg)`` be able to detect all kinds of XCom
       references.
    c. Not allow many properties of PlainXComArg (including ``__getitem__`` and
       ``__str__``) to exist on other kinds of XComArg implementations since
       they don't make sense.

    :meta private:
    """

    operator: Operator
    key: str = BaseXCom.XCOM_RETURN_KEY

    def __getitem__(self, item: str) -> XComArg:
        """Implement xcomresult['some_result_key']."""
        if not isinstance(item, str):
            raise ValueError(f"XComArg only supports str lookup, received {type(item).__name__}")
        return PlainXComArg(operator=self.operator, key=item)

    def __iter__(self):
        """
        Override iterable protocol to raise error explicitly.

        The default ``__iter__`` implementation in Python calls ``__getitem__``
        with 0, 1, 2, etc. until it hits an ``IndexError``. This does not work
        well with our custom ``__getitem__`` implementation, and results in poor
        Dag-writing experience since a misplaced ``*`` expansion would create an
        infinite loop consuming the entire Dag parser.

        This override catches the error eagerly, so an incorrectly implemented
        Dag fails fast and avoids wasting resources on nonsensical iterating.
        """
        raise TypeError("'XComArg' object is not iterable")

    def __repr__(self) -> str:
        if self.key == BaseXCom.XCOM_RETURN_KEY:
            return f"XComArg({self.operator!r})"
        return f"XComArg({self.operator!r}, {self.key!r})"

    def __str__(self) -> str:
        """
        Backward compatibility for old-style jinja used in Airflow Operators.

        **Example**: to use XComArg at BashOperator::

            BashOperator(cmd=f"... {xcomarg} ...")

        :return:
        """
        xcom_pull_kwargs = [
            f"task_ids='{self.operator.task_id}'",
            f"dag_id='{self.operator.dag_id}'",
        ]
        if self.key is not None:
            xcom_pull_kwargs.append(f"key='{self.key}'")

        xcom_pull_str = ", ".join(xcom_pull_kwargs)
        # {{{{ are required for escape {{ in f-string
        xcom_pull = f"{{{{ task_instance.xcom_pull({xcom_pull_str}) }}}}"
        return xcom_pull

    def _serialize(self) -> dict[str, Any]:
        return {"task_id": self.operator.task_id, "key": self.key}

    @property
    def is_setup(self) -> bool:
        return self.operator.is_setup

    @is_setup.setter
    def is_setup(self, val: bool):
        self.operator.is_setup = val

    @property
    def is_teardown(self) -> bool:
        return self.operator.is_teardown

    @is_teardown.setter
    def is_teardown(self, val: bool):
        self.operator.is_teardown = val

    @property
    def on_failure_fail_dagrun(self) -> bool:
        return self.operator.on_failure_fail_dagrun

    @on_failure_fail_dagrun.setter
    def on_failure_fail_dagrun(self, val: bool):
        self.operator.on_failure_fail_dagrun = val

    def as_setup(self) -> DependencyMixin:
        for operator, _ in self.iter_references():
            operator.is_setup = True
        return self

    def as_teardown(
        self,
        *,
        setups: BaseOperator | Iterable[BaseOperator] | None = None,
        on_failure_fail_dagrun: bool | None = None,
    ):
        for operator, _ in self.iter_references():
            operator.is_teardown = True
            operator.trigger_rule = TriggerRule.ALL_DONE_SETUP_SUCCESS
            if on_failure_fail_dagrun is not None:
                operator.on_failure_fail_dagrun = on_failure_fail_dagrun
            if setups is not None:
                setups = [setups] if isinstance(setups, DependencyMixin) else setups
                for s in setups:
                    s.is_setup = True
                    s >> operator
        return self

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        yield self.operator, self.key

    def map(self, f: Callable[[Any], Any]) -> MapXComArg:
        if self.key != BaseXCom.XCOM_RETURN_KEY:
            raise ValueError("cannot map against non-return XCom")
        return super().map(f)

    def zip(self, *others: XComArg, fillvalue: Any = NOTSET) -> ZipXComArg:
        if self.key != BaseXCom.XCOM_RETURN_KEY:
            raise ValueError("cannot map against non-return XCom")
        return super().zip(*others, fillvalue=fillvalue)

    def concat(self, *others: XComArg) -> ConcatXComArg:
        if self.key != BaseXCom.XCOM_RETURN_KEY:
            raise ValueError("cannot concatenate non-return XCom")
        return super().concat(*others)

    def resolve(self, context: Mapping[str, Any]) -> Any:
        ti = context["ti"]
        task_id = self.operator.task_id

        if self.operator.is_mapped:
            return LazyXComSequence(xcom_arg=self, ti=ti)
        tg = self.operator.get_closest_mapped_task_group()
        if tg is None:
            map_indexes = None
        else:
            upstream_map_indexes = getattr(ti, "_upstream_map_indexes", {})
            map_indexes = upstream_map_indexes.get(task_id, None)
        result = ti.xcom_pull(
            task_ids=task_id,
            key=self.key,
            default=NOTSET,
            map_indexes=map_indexes,
        )
        if is_arg_set(result):
            return result
        if self.key == BaseXCom.XCOM_RETURN_KEY:
            return None
        if getattr(self.operator, "multiple_outputs", False):
            # If the operator is set to have multiple outputs and it was not executed,
            # we should return "None" instead of showing an error. This is because when
            # multiple outputs XComs are created, the XCom keys associated with them will have
            # different names than the predefined "XCOM_RETURN_KEY" and won't be found.
            # Therefore, it's better to return "None" like we did above where self.key==XCOM_RETURN_KEY.
            return None
        raise XComNotFound(ti.dag_id, task_id, self.key)


def _get_callable_name(f: Callable | str) -> str:
    """Try to "describe" a callable by getting its name."""
    if callable(f):
        return f.__name__
    # Parse the source to find whatever is behind "def". For safety, we don't
    # want to evaluate the code in any meaningful way!
    with contextlib.suppress(Exception):
        kw, name, _ = f.lstrip().split(None, 2)
        if kw == "def":
            return name
    return "<function>"


@attrs.define
class _MapResult(Sequence):
    value: Sequence | dict
    callables: MapCallables

    def __getitem__(self, index: Any) -> Any:
        value = self.value[index]

        for f in self.callables:
            value = f(value)
        return value

    def __len__(self) -> int:
        return len(self.value)


@attrs.define
class MapXComArg(XComArg):
    """
    An XCom reference with ``map()`` call(s) applied.

    This is based on an XComArg, but also applies a series of "transforms" that
    convert the pulled XCom value.

    :meta private:
    """

    arg: XComArg
    callables: MapCallables

    def __attrs_post_init__(self) -> None:
        for c in self.callables:
            if getattr(c, "_airflow_is_task_decorator", False):
                raise ValueError("map() argument must be a plain function, not a @task operator")

    def __repr__(self) -> str:
        map_calls = "".join(f".map({_get_callable_name(f)})" for f in self.callables)
        return f"{self.arg!r}{map_calls}"

    def _serialize(self) -> dict[str, Any]:
        return {
            "arg": serialize_xcom_arg(self.arg),
            "callables": [inspect.getsource(c) if callable(c) else c for c in self.callables],
        }

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        yield from self.arg.iter_references()

    def map(self, f: Callable[[Any], Any]) -> MapXComArg:
        # Flatten arg.map(f1).map(f2) into one MapXComArg.
        return MapXComArg(self.arg, [*self.callables, f])

    def resolve(self, context: Mapping[str, Any]) -> Any:
        value = self.arg.resolve(context)
        if not isinstance(value, (Sequence, dict)):
            raise ValueError(f"XCom map expects sequence or dict, not {type(value).__name__}")
        return _MapResult(value, self.callables)


@attrs.define
class _ZipResult(Sequence):
    values: Sequence[Sequence | dict]
    fillvalue: Any = attrs.field(default=NOTSET, kw_only=True)

    @staticmethod
    def _get_or_fill(container: Sequence | dict, index: Any, fillvalue: Any) -> Any:
        try:
            return container[index]
        except (IndexError, KeyError):
            return fillvalue

    def __getitem__(self, index: Any) -> Any:
        if index >= len(self):
            raise IndexError(index)
        return tuple(self._get_or_fill(value, index, self.fillvalue) for value in self.values)

    def __len__(self) -> int:
        lengths = (len(v) for v in self.values)
        if is_arg_set(self.fillvalue):
            return max(lengths)
        return min(lengths)


@attrs.define
class ZipXComArg(XComArg):
    """
    An XCom reference with ``zip()`` applied.

    This is constructed from multiple XComArg instances, and presents an
    iterable that "zips" them together like the built-in ``zip()`` (and
    ``itertools.zip_longest()`` if ``fillvalue`` is provided).
    """

    args: Sequence[XComArg] = attrs.field(validator=attrs.validators.min_len(1))
    fillvalue: Any = attrs.field(default=NOTSET, kw_only=True)

    def __repr__(self) -> str:
        args_iter = iter(self.args)
        first = repr(next(args_iter))
        rest = ", ".join(repr(arg) for arg in args_iter)
        if is_arg_set(self.fillvalue):
            return f"{first}.zip({rest}, fillvalue={self.fillvalue!r})"
        return f"{first}.zip({rest})"

    def _serialize(self) -> dict[str, Any]:
        args = [serialize_xcom_arg(arg) for arg in self.args]
        if is_arg_set(self.fillvalue):
            return {"args": args, "fillvalue": self.fillvalue}
        return {"args": args}

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        for arg in self.args:
            yield from arg.iter_references()

    def resolve(self, context: Mapping[str, Any]) -> Any:
        values = [arg.resolve(context) for arg in self.args]
        for value in values:
            if not isinstance(value, (Sequence, dict)):
                raise ValueError(f"XCom zip expects sequence or dict, not {type(value).__name__}")
        return _ZipResult(values, fillvalue=self.fillvalue)


@attrs.define
class _ConcatResult(Sequence):
    values: Sequence[Sequence | dict]

    def __getitem__(self, index: Any) -> Any:
        if index >= 0:
            i = index
        else:
            i = len(self) + index
        for value in self.values:
            if i < 0:
                break
            if i >= (curlen := len(value)):
                i -= curlen
            elif isinstance(value, Sequence):
                return value[i]
            else:
                return next(itertools.islice(iter(value), i, None))
        raise IndexError("list index out of range")

    def __len__(self) -> int:
        return sum(len(v) for v in self.values)


@attrs.define
class ConcatXComArg(XComArg):
    """
    Concatenating multiple XCom references into one.

    This is done by calling ``concat()`` on an XComArg to combine it with
    others. The effect is similar to Python's :func:`itertools.chain`, but the
    return value also supports index access.
    """

    args: Sequence[XComArg] = attrs.field(validator=attrs.validators.min_len(1))

    def __repr__(self) -> str:
        args_iter = iter(self.args)
        first = repr(next(args_iter))
        rest = ", ".join(repr(arg) for arg in args_iter)
        return f"{first}.concat({rest})"

    def _serialize(self) -> dict[str, Any]:
        return {"args": [serialize_xcom_arg(arg) for arg in self.args]}

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        for arg in self.args:
            yield from arg.iter_references()

    def concat(self, *others: XComArg) -> ConcatXComArg:
        # Flatten foo.concat(x).concat(y) into one call.
        return ConcatXComArg([*self.args, *others])

    def resolve(self, context: Mapping[str, Any]) -> Any:
        values = [arg.resolve(context) for arg in self.args]
        for value in values:
            if not isinstance(value, (Sequence, dict)):
                raise ValueError(f"XCom concat expects sequence or dict, not {type(value).__name__}")
        return _ConcatResult(values)


_XCOM_ARG_TYPES: Mapping[str, type[XComArg]] = {
    "": PlainXComArg,
    "concat": ConcatXComArg,
    "map": MapXComArg,
    "zip": ZipXComArg,
}


def serialize_xcom_arg(value: XComArg) -> dict[str, Any]:
    """Dag serialization interface."""
    key = next(k for k, v in _XCOM_ARG_TYPES.items() if isinstance(value, v))
    if key:
        return {"type": key, **value._serialize()}
    return value._serialize()


@singledispatch
def get_task_map_length(
    xcom_arg: XComArg, resolved_val: Sized, upstream_map_indexes: dict[str, int]
) -> int | None:
    # The base implementation -- specific XComArg subclasses have specialised implementations
    raise NotImplementedError()


@get_task_map_length.register
def _(xcom_arg: PlainXComArg, resolved_val: Sized, upstream_map_indexes: dict[str, int]):
    task_id = xcom_arg.operator.task_id

    if xcom_arg.operator.is_mapped:
        # TODO: How to tell if all the upstream TIs finished?
        pass
    return (upstream_map_indexes.get(task_id) or 1) * len(resolved_val)


@get_task_map_length.register
def _(xcom_arg: MapXComArg, resolved_val: Sized, upstream_map_indexes: dict[str, int]):
    return get_task_map_length(xcom_arg.arg, resolved_val, upstream_map_indexes)


@get_task_map_length.register
def _(xcom_arg: ZipXComArg, resolved_val: Sized, upstream_map_indexes: dict[str, int]):
    all_lengths = (get_task_map_length(arg, resolved_val, upstream_map_indexes) for arg in xcom_arg.args)
    ready_lengths = [length for length in all_lengths if length is not None]
    if len(ready_lengths) != len(xcom_arg.args):
        return None  # If any of the referenced XComs is not ready, we are not ready either.
    if is_arg_set(xcom_arg.fillvalue):
        return max(ready_lengths)
    return min(ready_lengths)


@get_task_map_length.register
def _(xcom_arg: ConcatXComArg, resolved_val: Sized, upstream_map_indexes: dict[str, int]):
    all_lengths = (get_task_map_length(arg, resolved_val, upstream_map_indexes) for arg in xcom_arg.args)
    ready_lengths = [length for length in all_lengths if length is not None]
    if len(ready_lengths) != len(xcom_arg.args):
        return None  # If any of the referenced XComs is not ready, we are not ready either.
    return sum(ready_lengths)
