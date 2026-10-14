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
"""The fingerprint of an indexed task's input, which tells whether its checkpoint still applies."""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import TYPE_CHECKING, Any

from airflow.sdk._shared.serialization import CLASSNAME, DATA
from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
from airflow.sdk.definitions.xcom_arg import XComArg
from airflow.sdk.serde import serialize

if TYPE_CHECKING:
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.types import Logger

# The sequences an XComArg in a partial kwarg renders to that hold their items. Any other sequence is
# lazy: an iterated upstream's XComIterable or a mapped upstream's LazyXComSequence, read one XCom per
# item, or the _MapResult, _ZipResult and _ConcatResult that .map(), .zip() and .concat() wrap around an
# upstream. serde keeps only a handle of some, fetches the whole of others and cannot serialize a .map()
# callable at all, so the fingerprint digests their items instead, once per run (see _LazyDigests).
EAGER_SEQUENCES = (str, bytes, list, tuple)

# What an item of a lazy upstream value that cannot be read or serialized is digested as, with the name of
# the exception's type, so that the other items still count (see _LazyDigests).
UNREADABLE_ITEM = "__unreadable_item__"

# How serde names a set and a frozenset it writes: their members come out in iteration order, which
# for strings depends on the process's hash seed (see to_canonical_json).
SET_CLASSNAMES = frozenset({"builtins.set", "builtins.frozenset"})


def to_canonical_json(value: Any) -> str:
    """
    Serialize ``value`` to JSON that reads the same in every process, for a digest.

    ``sort_keys`` orders the keys of every mapping, but serde writes a set's members in iteration
    order, which for strings depends on the hash seed: a retry in another pod or on a restarted
    worker would digest the same input differently and run every index again. The members of every
    set are put in a fixed order as well, only when the JSON names one: a value without a set comes
    out as plain ``json.dumps(serialize(value), sort_keys=True)``, at the cost of one substring
    search, and the digests of earlier checkpoints still match. Raises what serde raises.
    """
    serialized = serialize(value)
    text = json.dumps(serialized, sort_keys=True)
    if not any(classname in text for classname in SET_CLASSNAMES):
        return text
    return json.dumps(_sort_set_members(serialized), sort_keys=True)


def _sort_set_members(serialized: Any) -> Any:
    """Return serde's output with the members of every set, at any depth, in a fixed order."""
    if isinstance(serialized, list):
        return [_sort_set_members(member) for member in serialized]
    if not isinstance(serialized, dict):
        return serialized
    result = {key: _sort_set_members(member) for key, member in serialized.items()}
    if result.get(CLASSNAME) in SET_CLASSNAMES:
        result[DATA] = sorted(result[DATA], key=partial(json.dumps, sort_keys=True))
    return result


class _LazyDigests:
    """
    The digests of the lazy upstream values of one run, each read once by its items.

    A lazy upstream value (an ``XComIterable``, a ``LazyXComSequence`` or a ``.map()`` over them) is
    read one XCom per item. It is the same for every index of a run, so the first indexed task to ask
    reads it and the others wait for its digest under the lock, as indexed tasks are created in
    worker threads.

    :param stop_requested: whether the run was asked to stop; a kill ends a read at the next item.
    :param log: the iterated task's logger, for an item that cannot be read.
    """

    __slots__ = ("_digests", "_lock", "_log", "_stop_requested")

    def __init__(self, *, stop_requested: Callable[[], bool], log: Logger) -> None:
        self._stop_requested = stop_requested
        self._log = log
        self._digests: dict[str, str | None] = {}
        self._lock = threading.Lock()

    def of(self, path: str, items: Sequence[Any]) -> str | None:
        """Return the digest of the lazy upstream value at ``path``, read by the first task that asks."""
        with self._lock:
            if path not in self._digests:
                self._digests[path] = self._read(path, items)
            return self._digests[path]

    def _read(self, path: str, items: Sequence[Any]) -> str | None:
        """
        Digest the items of the lazy upstream value at ``path``, read one at a time by position.

        Each item is serialized on its own and fed to the hash followed by a newline, which JSON never
        leaves unescaped inside a value, so no item is held longer than it takes to digest it. Two
        upstream runs of the same length but other items give other digests. Reading the items of a
        ``.map()`` runs its callables on each of them, as the indexed task does when it reads them.

        An item that cannot be read or serialized (a callable that raises for it) is digested as a
        fixed marker naming the exception type, and the others still count: a cleared upstream that
        produced other values changes the digest even when that item fails on both attempts. It must
        not fail every indexed task either, which may never read that item. A warning names the path,
        since the indexed tasks then run again, or not, for a reason found in the upstream.

        ``stop_requested`` is asked before every read, as the executor asks it before every pull: a
        kill that lands while a large upstream is read ends the read at the next item, and the value
        gets no digest. No indexed task runs after the kill to use it.
        """
        if self._stop_requested():
            return None
        digest = hashlib.sha256()
        failed: list[int] = []
        for position in range(len(items)):
            try:
                text = to_canonical_json(items[position])
            except Exception as e:
                failed.append(position)
                text = json.dumps({UNREADABLE_ITEM: type(e).__qualname__})
            digest.update(text.encode())
            digest.update(b"\n")
            if self._stop_requested():
                return None
        if failed:
            self._log.warning(
                "Could not read %d of the %d items of %s for the input fingerprint (first at position "
                "%d); they count as unreadable, so the indexed tasks may run again or replay their "
                "checkpoints for a reason found in that upstream",
                len(failed),
                len(items),
                path,
                failed[0],
            )
        return digest.hexdigest()


class _UpstreamInputs:
    """
    The ``.partial()`` kwargs an upstream task provides, as one run's indexed tasks render them.

    Clearing the upstream together with the task can change them while an index's iterated input
    stays the same, so they belong in the fingerprint. Only XComArg values count, read back from the
    rendered operator wherever they sit, at the top level or at any depth of the mappings, lists and
    tuples a partial kwarg holds, such as a ``@task``'s ``op_kwargs`` and ``op_args``. Other
    templated values are left out on purpose: one like ``{{ ti.try_number }}`` changes with every
    attempt and would make every checkpoint look stale.

    They are the same for every index, so the first indexed task to ask serializes them, under the
    lock, and the others reuse the text.

    :param partial_kwargs: the iterated operator's ``.partial()`` kwargs, as written in the DAG.
    :param lazy_digests: the run's digests of lazy upstream values, which stand for those values.
    """

    __slots__ = ("_declared", "_json", "_lazy_digests", "_lock")

    def __init__(self, partial_kwargs: Mapping[str, Any], lazy_digests: _LazyDigests) -> None:
        self._declared = {
            key: value for key, value in partial_kwargs.items() if any(XComArg.iter_xcom_references(value))
        }
        self._lazy_digests = lazy_digests
        self._json: str | None | ArgNotSet = NOTSET
        self._lock = threading.Lock()

    def json(self, unmapped_task: BaseOperator) -> str | None:
        """
        Return their canonical JSON, taken once per run from ``unmapped_task`` once rendered.

        ``""`` when no partial kwarg comes from an upstream, None when serde cannot serialize one.
        """
        if not self._declared:
            return ""
        with self._lock:
            if isinstance(self._json, ArgNotSet):
                try:
                    self._json = to_canonical_json(self.collect(unmapped_task))
                except (TypeError, ValueError, AttributeError, RecursionError):
                    self._json = None
            return self._json

    def collect(self, unmapped_task: BaseOperator) -> dict[str, Any]:
        """Collect the rendered value of every XComArg in the partial kwargs, by its path."""
        inputs: dict[str, Any] = {}
        for key, value in self._declared.items():
            self._collect(key, value, getattr(unmapped_task, key, None), inputs)
        return inputs

    def _collect(self, path: str, declared: Any, rendered: Any, inputs: dict[str, Any]) -> None:
        """
        Record under its path the rendered value of every XComArg in ``declared``, at any depth.

        ``declared`` is a partial kwarg as written in the DAG and ``rendered`` the same value once
        rendered, walked side by side: a mapping by key (``op_kwargs.cfg.x``), a list or tuple by
        position (``op_args[0]``). A part rendered into another shape than it was declared in is
        recorded as None. A lazy sequence, any but :data:`EAGER_SEQUENCES`, is recorded as the digest
        of its items.
        """
        if isinstance(declared, XComArg):
            inputs[path] = (
                self._lazy_digests.of(path, rendered)
                if isinstance(rendered, Sequence) and not isinstance(rendered, EAGER_SEQUENCES)
                else rendered
            )
        elif isinstance(declared, Mapping):
            for name, nested in declared.items():
                value = rendered.get(name) if isinstance(rendered, Mapping) else None
                self._collect(f"{path}.{name}", nested, value, inputs)
        elif isinstance(declared, (list, tuple)):
            for position, nested in enumerate(declared):
                value = (
                    rendered[position]
                    if isinstance(rendered, (list, tuple)) and position < len(rendered)
                    else None
                )
                self._collect(f"{path}[{position}]", nested, value, inputs)


class InputFingerprint:
    """
    Fingerprint the input of each indexed task of one run of an iterated task.

    A retry may run on another input than the attempt that wrote the checkpoints: the upstream was
    cleared together with the task and produced other values. An index then no longer means the same
    work, and replaying its result would hand downstream a value computed from the old value. The
    fingerprint stored on each checkpoint tells the two apart. The input is an index's own iterated
    kwargs plus the run's :class:`_UpstreamInputs`.

    :param partial_kwargs: the iterated operator's ``.partial()`` kwargs, as written in the DAG.
    :param stop_requested: whether the run was asked to stop; a kill ends the read of a large lazy
        upstream at the next item.
    :param log: the iterated task's logger, for a lazy upstream item that cannot be read.
    """

    __slots__ = ("upstream_inputs",)

    def __init__(
        self, partial_kwargs: Mapping[str, Any], *, stop_requested: Callable[[], bool], log: Logger
    ) -> None:
        self.upstream_inputs = _UpstreamInputs(
            partial_kwargs, _LazyDigests(stop_requested=stop_requested, log=log)
        )

    def of(self, mapped_kwargs: Mapping[str, Any], unmapped_task: BaseOperator) -> str | None:
        """
        Return the fingerprint of one indexed task's input, or None when serde cannot serialize it.

        Taken once ``unmapped_task`` is rendered, so the partial kwargs an upstream provides are in it
        with their value. An index without partial kwargs from an upstream is digested by its iterated
        kwargs alone. An input without a digest has its checkpoint honoured by index alone.
        """
        upstream_inputs = self.upstream_inputs.json(unmapped_task)
        if upstream_inputs is None:
            return None
        try:
            serialized = to_canonical_json(mapped_kwargs)
        except (TypeError, ValueError, AttributeError, RecursionError):
            return None
        if upstream_inputs:
            serialized = f"{serialized}\n{upstream_inputs}"
        return hashlib.sha256(serialized.encode()).hexdigest()
