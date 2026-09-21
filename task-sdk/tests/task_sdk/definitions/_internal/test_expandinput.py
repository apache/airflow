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

import asyncio
import threading
from collections import deque

import pytest
from task_sdk.definitions.conftest import make_xcom_arg

from airflow.sdk.definitions._internal.expandinput import (
    BatchedExpandInput,
    DecoratedExpandInput,
    DictOfListsExpandInput,
    ExpandInput,
    ListOfDictsExpandInput,
    aiterate,
)


class AsyncOnlyValues:
    """A resolved value that can only be read asynchronously, like an XComIterable on the loop."""

    def __init__(self, values):
        self.values = values

    async def __aiter__(self):
        for value in self.values:
            yield value


def _async_only_xcom_arg(values):
    """An XComArg whose sync ``resolve`` must never run and whose ``aresolve`` gives an async iterable."""
    xcom_arg = make_xcom_arg(None)
    xcom_arg.resolve = lambda *a, **kw: pytest.fail("synchronous resolve() used on the async path")

    async def aresolve(*a, **kw):
        return AsyncOnlyValues(values)

    xcom_arg.aresolve = aresolve
    return xcom_arg


async def _alist(aiterable):
    return [item async for item in aiterable]


class TestExpandInput:
    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ({"a": 1}, [{"a": 1}]),
            ({"a": [1, 2, 3]}, [{"a": 1}, {"a": 2}, {"a": 3}]),
            ({"a": "hello"}, [{"a": "hello"}]),
            (
                {"a": [1, 2], "b": [10, 20]},
                [{"a": 1, "b": 10}, {"a": 1, "b": 20}, {"a": 2, "b": 10}, {"a": 2, "b": 20}],
            ),
            ({"a": (x for x in [1, 2])}, [{"a": 1}, {"a": 2}]),
            (
                {"a": {"x": 1, "y": 2}},
                [{"a": ("x", 1)}, {"a": ("y", 2)}],
            ),
        ],
    )
    def test_dict_of_lists_expand_input_iter_values(self, actual, expected):
        """
        A dict value expands to its (key, value) pairs, mirroring _expand_mapped_field's
        handling of dict values for the classic .expand() resolve() path, so .iterate() and
        .expand() hand sub-tasks the same per-index value for a dict argument.
        """
        expand_input = DictOfListsExpandInput(actual)

        with pytest.raises(RuntimeError, match="Length of DictOfListsExpandInput is not yet known"):
            len(expand_input)

        result = list(expand_input.iter_values({}))
        assert result == expected
        assert len(expand_input) == len(expected)

    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ([{"a": 1}, {"a": 2}], [{"a": 1}, {"a": 2}]),
            ([{"a": 1, "b": 2}], [{"a": 1, "b": 2}]),
            ([], []),
        ],
    )
    def test_list_of_dicts_expand_input_iter_values(self, actual, expected):
        expand_input = ListOfDictsExpandInput(actual)

        with pytest.raises(RuntimeError, match="Length of ListOfDictsExpandInput is not yet known"):
            len(expand_input)

        result = list(expand_input.iter_values({}))
        assert result == expected
        assert len(expand_input) == len(expected)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ({"a": 1}, [{"a": 1}]),
            ({"a": [1, 2, 3]}, [{"a": 1}, {"a": 2}, {"a": 3}]),
            ({"a": "hello"}, [{"a": "hello"}]),
            (
                {"a": [1, 2], "b": [10, 20]},
                [{"a": 1, "b": 10}, {"a": 1, "b": 20}, {"a": 2, "b": 10}, {"a": 2, "b": 20}],
            ),
            ({"a": (x for x in [1, 2])}, [{"a": 1}, {"a": 2}]),
            ({"a": {"x": 1, "y": 2}}, [{"a": ("x", 1)}, {"a": ("y", 2)}]),
            ({"a": AsyncOnlyValues([1, 2])}, [{"a": 1}, {"a": 2}]),
        ],
    )
    async def test_dict_of_lists_expand_input_aiter_values(self, actual, expected):
        """``aiter_values`` yields the same combinations as ``iter_values`` and also accepts async sources."""
        expand_input = DictOfListsExpandInput(actual)

        result = await _alist(expand_input.aiter_values({}))
        assert result == expected
        assert len(expand_input) == len(expected)

    @pytest.mark.asyncio
    async def test_dict_of_lists_expand_input_aiter_values_resolves_xcom_args_with_aresolve(self):
        expand_input = DictOfListsExpandInput({"a": _async_only_xcom_arg([1, 2]), "b": [10, 20]})

        result = await _alist(expand_input.aiter_values({}))
        assert result == [{"a": 1, "b": 10}, {"a": 1, "b": 20}, {"a": 2, "b": 10}, {"a": 2, "b": 20}]

    @pytest.mark.asyncio
    async def test_dict_of_lists_expand_input_aiter_values_is_lazy(self):
        """Sources are read only as far as the consumer goes; nothing is materialized up front."""
        pulled: list[int] = []

        async def source():
            for value in range(3):
                pulled.append(value)
                yield value

        expand_input = DictOfListsExpandInput({"a": source(), "b": [10, 20]})
        values = expand_input.aiter_values({})

        assert await values.__anext__() == {"a": 0, "b": 10}
        assert await values.__anext__() == {"a": 0, "b": 20}
        assert pulled == [0]
        assert await values.__anext__() == {"a": 1, "b": 10}
        assert pulled == [0, 1]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ([{"a": 1}, {"a": 2}], [{"a": 1}, {"a": 2}]),
            ([{"a": 1, "b": 2}], [{"a": 1, "b": 2}]),
            ([], []),
        ],
    )
    async def test_list_of_dicts_expand_input_aiter_values(self, actual, expected):
        expand_input = ListOfDictsExpandInput(actual)

        result = await _alist(expand_input.aiter_values({}))
        assert result == expected
        assert len(expand_input) == len(expected)

    @pytest.mark.asyncio
    async def test_list_of_dicts_expand_input_aiter_values_resolves_xcom_args_with_aresolve(self):
        whole = ListOfDictsExpandInput(_async_only_xcom_arg([{"a": 1}, {"a": 2}]))
        assert await _alist(whole.aiter_values({})) == [{"a": 1}, {"a": 2}]

        per_item = ListOfDictsExpandInput([{"a": 0}, _async_only_xcom_arg([{"a": 1}, {"a": 2}])])
        assert await _alist(per_item.aiter_values({})) == [{"a": 0}, {"a": 1}, {"a": 2}]

    @pytest.mark.asyncio
    async def test_decorated_expand_input_aiter_values_wraps_op_kwargs(self):
        decorated = DecoratedExpandInput(ListOfDictsExpandInput([{"a": 1}, {"a": 2}]))

        assert await _alist(decorated.aiter_values({})) == [{"op_kwargs": {"a": 1}}, {"op_kwargs": {"a": 2}}]
        assert len(decorated) == 2

    def test_base_aiter_values_is_abstract(self):
        class Incomplete(ExpandInput):
            EXPAND_INPUT_TYPE = "incomplete"

            @property
            def value(self):
                return None

        with pytest.raises(NotImplementedError):
            Incomplete().aiter_values({})


class TestAiterate:
    @pytest.mark.asyncio
    async def test_async_iterable_is_consumed_with_async_for(self):
        assert await _alist(aiterate(AsyncOnlyValues([1, 2]))) == [1, 2]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("container", [[1, 2], (1, 2), range(1, 3), deque([1, 2])])
    async def test_in_memory_containers_are_iterated_in_place(self, container):
        assert await _alist(aiterate(container)) == [1, 2]

    @pytest.mark.asyncio
    async def test_other_iterables_are_advanced_off_the_loop_thread_and_lazily(self):
        """
        A generic iterator may block on ``next()`` with a synchronous supervisor call, which on the
        loop thread would deadlock with the ``asend`` calls in flight, so it runs in a worker thread.
        """
        threads: list[threading.Thread] = []
        pulled: list[int] = []

        def values():
            for value in range(3):
                threads.append(threading.current_thread())
                pulled.append(value)
                yield value

        items = aiterate(values())
        assert await items.__anext__() == 0
        assert pulled == [0]
        assert await _alist(items) == [1, 2]
        assert len(threads) == 3
        assert all(thread is not threading.current_thread() for thread in threads)
        assert asyncio.get_running_loop().is_running()


class TestBatchedExpandInput:
    @pytest.mark.parametrize(
        "size",
        [pytest.param(-1), pytest.param(0), pytest.param(1)],
    )
    def test_invalid_size_raises(self, size: int):
        inner = DictOfListsExpandInput({"a": [1, 2, 3]})
        with pytest.raises(ValueError, match="batch size must be at least 2"):
            BatchedExpandInput(inner, size=size)

    @pytest.mark.parametrize(
        ("size", "map_index", "items", "expected"),
        [
            (2, 0, [1, 2, 3, 4, 5], [1, 3, 5]),
            (2, 1, [1, 2, 3, 4, 5], [2, 4]),
            (3, 0, [1, 2, 3, 4, 5, 6], [1, 4]),
            (3, 1, [1, 2, 3, 4, 5, 6], [2, 5]),
            (3, 2, [1, 2, 3, 4, 5, 6], [3, 6]),
        ],
    )
    def test_iter_values_striding(self, size: int, map_index: int, items: list, expected: list):
        inner = DictOfListsExpandInput({"a": items})
        batched = BatchedExpandInput(inner, size=size)
        context = {"ti": type("TI", (), {"map_index": map_index})()}

        with pytest.raises(RuntimeError, match="Length of BatchedExpandInput is not yet known"):
            len(batched)

        result = [combo["a"] for combo in batched.iter_values(context)]
        assert result == expected
        assert len(batched) == len(expected)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("size", "map_index", "items", "expected"),
        [
            (2, 0, [1, 2, 3, 4, 5], [1, 3, 5]),
            (2, 1, [1, 2, 3, 4, 5], [2, 4]),
            (3, 0, [1, 2, 3, 4, 5, 6], [1, 4]),
            (3, 1, [1, 2, 3, 4, 5, 6], [2, 5]),
            (3, 2, [1, 2, 3, 4, 5, 6], [3, 6]),
        ],
    )
    async def test_aiter_values_striding(self, size: int, map_index: int, items: list, expected: list):
        inner = DictOfListsExpandInput({"a": items})
        batched = BatchedExpandInput(inner, size=size)
        context = {"ti": type("TI", (), {"map_index": map_index})()}

        result = [combo["a"] async for combo in batched.aiter_values(context)]
        assert result == expected
        assert len(batched) == len(expected)
        # delegate length must remain the full item count, not the batch slice
        assert len(inner) == len(items)
