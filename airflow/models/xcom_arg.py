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
from typing import TYPE_CHECKING, Any, Callable, Iterator, Mapping, Sequence, Union, overload

from sqlalchemy import func
from sqlalchemy.orm import Session

from airflow.exceptions import XComNotFound
from airflow.models.abstractoperator import AbstractOperator
from airflow.models.mappedoperator import MappedOperator
from airflow.models.taskmixin import DAGNode, DependencyMixin
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.utils.context import Context
from airflow.utils.edgemodifier import EdgeModifier
from airflow.utils.mixins import ResolveMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from airflow.models.dag import DAG
    from airflow.models.operator import Operator

# Callable objects contained by MapXComArg. We only accept callables from
# the user, but deserialize them into strings in a serialized XComArg for
# safety (those callables are arbitrary user code).
MapCallables = Sequence[Union[Callable[[Any], Any], str]]


class XComArg(ResolveMixin, DependencyMixin):
    """Reference to an XCom value pushed from another operator.

    The implementation supports::

        xcomarg >> op
        xcomarg << op
        op >> xcomarg   # By BaseOperator code
        op << xcomarg   # By BaseOperator code

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
    def __new__(cls: type[XComArg], operator: Operator, key: str = XCOM_RETURN_KEY) -> XComArg:
        """Called when the user writes ``XComArg(...)`` directly."""

    @overload
    def __new__(cls: type[XComArg]) -> XComArg:
        """Called by Python internals from subclasses."""

    def __new__(cls, *args, **kwargs) -> XComArg:
        if cls is XComArg:
            return PlainXComArg(*args, **kwargs)
        return super().__new__(cls)

    @staticmethod
    def iter_xcom_references(arg: Any) -> Iterator[tuple[Operator, str]]:
        """Return XCom references in an arbitrary value.

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
    def apply_upstream_relationship(op: Operator, arg: Any):
        """Set dependency for XComArgs.

        This looks for XComArg objects in ``arg`` "deeply" (looking inside
        collections objects and classes decorated with ``template_fields``), and
        sets the relationship to ``op`` on any found.
        """
        for operator, _ in XComArg.iter_xcom_references(arg):
            op.set_upstream(operator)

    @property
    def roots(self) -> list[DAGNode]:
        """Required by TaskMixin"""
        return [op for op, _ in self.iter_references()]

    @property
    def leaves(self) -> list[DAGNode]:
        """Required by TaskMixin"""
        return [op for op, _ in self.iter_references()]

    def set_upstream(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ):
        """Proxy to underlying operator set_upstream method. Required by TaskMixin."""
        for operator, _ in self.iter_references():
            operator.set_upstream(task_or_task_list, edge_modifier)

    def set_downstream(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ):
        """Proxy to underlying operator set_downstream method. Required by TaskMixin."""
        for operator, _ in self.iter_references():
            operator.set_downstream(task_or_task_list, edge_modifier)

    def _serialize(self) -> dict[str, Any]:
        """Called by DAG serialization.

        The implementation should be the inverse function to ``deserialize``,
        returning a data dict converted from this XComArg derivative. DAG
        serialization does not call this directly, but ``serialize_xcom_arg``
        instead, which adds additional information to dispatch deserialization
        to the correct class.
        """
        raise NotImplementedError()

    @classmethod
    def _deserialize(cls, data: dict[str, Any], dag: DAG) -> XComArg:
        """Called when deserializing a DAG.

        The implementation should be the inverse function to ``serialize``,
        implementing given a data dict converted from this XComArg derivative,
        how the original XComArg should be created. DAG serialization relies on
        additional information added in ``serialize_xcom_arg`` to dispatch data
        dicts to the correct ``_deserialize`` information, so this function does
        not need to validate whether the incoming data contains correct keys.
        """
        raise NotImplementedError()

    def map(self, f: Callable[[Any], Any]) -> MapXComArg:
        return MapXComArg(self, [f])

    def zip(self, *others: XComArg, fillvalue: Any = NOTSET) -> ZipXComArg:
        return ZipXComArg([self, *others], fillvalue=fillvalue)

    def get_task_map_length(self, run_id: str, *, session: Session) -> int | None:
        """Inspect length of pushed value for task-mapping.

        This is used to determine how many task instances the scheduler should
        create for a downstream using this XComArg for task-mapping.

        *None* may be returned if the depended XCom has not been pushed.
        """
        raise NotImplementedError()

    def resolve(self, context: Context, session: Session = NEW_SESSION) -> Any:
        """Pull XCom value.

        This should only be called during ``op.execute()`` with an appropriate
        context (e.g. generated from ``TaskInstance.get_template_context()``).
        Although the ``ResolveMixin`` parent mixin also has a ``resolve``
        protocol, this adds the optional ``session`` argument that some of the
        subclasses need.

        :meta private:
        """
        raise NotImplementedError()


class PlainXComArg(XComArg):
    """Reference to one single XCom without any additional semantics.

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

    def __init__(self, operator: Operator, key: str = XCOM_RETURN_KEY):
        self.operator = operator
        self.key = key

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PlainXComArg):
            return NotImplemented
        return self.operator == other.operator and self.key == other.key

    def __getitem__(self, item: str) -> XComArg:
        """Implements xcomresult['some_result_key']"""
        if not isinstance(item, str):
            raise ValueError(f"XComArg only supports str lookup, received {type(item).__name__}")
        return PlainXComArg(operator=self.operator, key=item)

    def __iter__(self):
        """Override iterable protocol to raise error explicitly.

        The default ``__iter__`` implementation in Python calls ``__getitem__``
        with 0, 1, 2, etc. until it hits an ``IndexError``. This does not work
        well with our custom ``__getitem__`` implementation, and results in poor
        DAG-writing experience since a misplaced ``*`` expansion would create an
        infinite loop consuming the entire DAG parser.

        This override catches the error eagerly, so an incorrectly implemented
        DAG fails fast and avoids wasting resources on nonsensical iterating.
        """
        raise TypeError("'XComArg' object is not iterable")

    def __repr__(self) -> str:
        if self.key == XCOM_RETURN_KEY:
            return f"XComArg({self.operator!r})"
        return f"XComArg({self.operator!r}, {self.key!r})"

    def __str__(self) -> str:
        """
        Backward compatibility for old-style jinja used in Airflow Operators

        **Example**: to use XComArg at BashOperator::

            BashOperator(cmd=f"... { xcomarg } ...")

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

    @classmethod
    def _deserialize(cls, data: dict[str, Any], dag: DAG) -> XComArg:
        return cls(dag.get_task(data["task_id"]), data["key"])

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        yield self.operator, self.key

    def map(self, f: Callable[[Any], Any]) -> MapXComArg:
        if self.key != XCOM_RETURN_KEY:
            raise ValueError("cannot map against non-return XCom")
        return super().map(f)

    def zip(self, *others: XComArg, fillvalue: Any = NOTSET) -> ZipXComArg:
        if self.key != XCOM_RETURN_KEY:
            raise ValueError("cannot map against non-return XCom")
        return super().zip(*others, fillvalue=fillvalue)

    def get_task_map_length(self, run_id: str, *, session: Session) -> int | None:
        from airflow.models.taskmap import TaskMap
        from airflow.models.xcom import XCom

        task = self.operator
        if isinstance(task, MappedOperator):
            query = session.query(func.count(XCom.map_index)).filter(
                XCom.dag_id == task.dag_id,
                XCom.run_id == run_id,
                XCom.task_id == task.task_id,
                XCom.map_index >= 0,
                XCom.key == XCOM_RETURN_KEY,
            )
        else:
            query = session.query(TaskMap.length).filter(
                TaskMap.dag_id == task.dag_id,
                TaskMap.run_id == run_id,
                TaskMap.task_id == task.task_id,
                TaskMap.map_index < 0,
            )
        return query.scalar()

    @provide_session
    def resolve(self, context: Context, session: Session = NEW_SESSION) -> Any:
        ti = context["ti"]
        task_id = self.operator.task_id
        map_indexes = ti.get_relevant_upstream_map_indexes(
            self.operator,
            context["expanded_ti_count"],
            session=session,
        )
        result = ti.xcom_pull(
            task_ids=task_id,
            map_indexes=map_indexes,
            key=self.key,
            default=NOTSET,
            session=session,
        )
        if not isinstance(result, ArgNotSet):
            return result
        if self.key == XCOM_RETURN_KEY:
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


class _MapResult(Sequence):
    def __init__(self, value: Sequence | dict, callables: MapCallables) -> None:
        self.value = value
        self.callables = callables

    def __getitem__(self, index: Any) -> Any:
        value = self.value[index]

        # In the worker, we can access all actual callables. Call them.
        callables = [f for f in self.callables if callable(f)]
        if len(callables) == len(self.callables):
            for f in callables:
                value = f(value)
            return value

        # In the scheduler, we don't have access to the actual callables, nor do
        # we want to run it since it's arbitrary code. This builds a string to
        # represent the call chain in the UI or logs instead.
        for v in self.callables:
            value = f"{_get_callable_name(v)}({value})"
        return value

    def __len__(self) -> int:
        return len(self.value)


class MapXComArg(XComArg):
    """An XCom reference with ``map()`` call(s) applied.

    This is based on an XComArg, but also applies a series of "transforms" that
    convert the pulled XCom value.

    :meta private:
    """

    def __init__(self, arg: XComArg, callables: MapCallables) -> None:
        for c in callables:
            if getattr(c, "_airflow_is_task_decorator", False):
                raise ValueError("map() argument must be a plain function, not a @task operator")
        self.arg = arg
        self.callables = callables

    def __repr__(self) -> str:
        map_calls = "".join(f".map({_get_callable_name(f)})" for f in self.callables)
        return f"{self.arg!r}{map_calls}"

    def _serialize(self) -> dict[str, Any]:
        return {
            "arg": serialize_xcom_arg(self.arg),
            "callables": [inspect.getsource(c) if callable(c) else c for c in self.callables],
        }

    @classmethod
    def _deserialize(cls, data: dict[str, Any], dag: DAG) -> XComArg:
        # We are deliberately NOT deserializing the callables. These are shown
        # in the UI, and displaying a function object is useless.
        return cls(deserialize_xcom_arg(data["arg"], dag), data["callables"])

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        yield from self.arg.iter_references()

    def map(self, f: Callable[[Any], Any]) -> MapXComArg:
        # Flatten arg.map(f1).map(f2) into one MapXComArg.
        return MapXComArg(self.arg, [*self.callables, f])

    def get_task_map_length(self, run_id: str, *, session: Session) -> int | None:
        return self.arg.get_task_map_length(run_id, session=session)

    @provide_session
    def resolve(self, context: Context, session: Session = NEW_SESSION) -> Any:
        value = self.arg.resolve(context, session=session)
        if not isinstance(value, (Sequence, dict)):
            raise ValueError(f"XCom map expects sequence or dict, not {type(value).__name__}")
        return _MapResult(value, self.callables)


class _ZipResult(Sequence):
    def __init__(self, values: Sequence[Sequence | dict], *, fillvalue: Any = NOTSET) -> None:
        self.values = values
        self.fillvalue = fillvalue

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
        if isinstance(self.fillvalue, ArgNotSet):
            return min(lengths)
        return max(lengths)


class ZipXComArg(XComArg):
    """An XCom reference with ``zip()`` applied.

    This is constructed from multiple XComArg instances, and presents an
    iterable that "zips" them together like the built-in ``zip()`` (and
    ``itertools.zip_longest()`` if ``fillvalue`` is provided).
    """

    def __init__(self, args: Sequence[XComArg], *, fillvalue: Any = NOTSET) -> None:
        if not args:
            raise ValueError("At least one input is required")
        self.args = args
        self.fillvalue = fillvalue

    def __repr__(self) -> str:
        args_iter = iter(self.args)
        first = repr(next(args_iter))
        rest = ", ".join(repr(arg) for arg in args_iter)
        if isinstance(self.fillvalue, ArgNotSet):
            return f"{first}.zip({rest})"
        return f"{first}.zip({rest}, fillvalue={self.fillvalue!r})"

    def _serialize(self) -> dict[str, Any]:
        args = [serialize_xcom_arg(arg) for arg in self.args]
        if isinstance(self.fillvalue, ArgNotSet):
            return {"args": args}
        return {"args": args, "fillvalue": self.fillvalue}

    @classmethod
    def _deserialize(cls, data: dict[str, Any], dag: DAG) -> XComArg:
        return cls(
            [deserialize_xcom_arg(arg, dag) for arg in data["args"]],
            fillvalue=data.get("fillvalue", NOTSET),
        )

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        for arg in self.args:
            yield from arg.iter_references()

    def get_task_map_length(self, run_id: str, *, session: Session) -> int | None:
        all_lengths = (arg.get_task_map_length(run_id, session=session) for arg in self.args)
        ready_lengths = [length for length in all_lengths if length is not None]
        if len(ready_lengths) != len(self.args):
            return None  # If any of the referenced XComs is not ready, we are not ready either.
        if isinstance(self.fillvalue, ArgNotSet):
            return min(ready_lengths)
        return max(ready_lengths)

    @provide_session
    def resolve(self, context: Context, session: Session = NEW_SESSION) -> Any:
        values = [arg.resolve(context, session=session) for arg in self.args]
        for value in values:
            if not isinstance(value, (Sequence, dict)):
                raise ValueError(f"XCom zip expects sequence or dict, not {type(value).__name__}")
        return _ZipResult(values, fillvalue=self.fillvalue)


_XCOM_ARG_TYPES: Mapping[str, type[XComArg]] = {
    "": PlainXComArg,
    "map": MapXComArg,
    "zip": ZipXComArg,
}


def serialize_xcom_arg(value: XComArg) -> dict[str, Any]:
    """DAG serialization interface."""
    key = next(k for k, v in _XCOM_ARG_TYPES.items() if v == type(value))
    if key:
        return {"type": key, **value._serialize()}
    return value._serialize()


def deserialize_xcom_arg(data: dict[str, Any], dag: DAG) -> XComArg:
    """DAG serialization interface."""
    klass = _XCOM_ARG_TYPES[data.get("type", "")]
    return klass._deserialize(data, dag)
