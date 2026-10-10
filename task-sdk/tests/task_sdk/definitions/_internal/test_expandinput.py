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
from collections.abc import Sequence

import pytest
from task_sdk.definitions.conftest import make_xcom_arg

from airflow.sdk.definitions._internal.expandinput import (
    DecoratedExpandInput,
    DictOfListsExpandInput,
    ExpandInput,
    ListOfDictsExpandInput,
    MappedArgument,
    Resolved,
    Source,
    index_for_each_field,
)
from airflow.sdk.exceptions import UnmappableXComTypePushed, XComForMappingNotPushed


class AsyncOnlyValues(Sequence):
    """A Sequence read through its own async accessors, like an XComIterable on the loop; sync reads fail."""

    def __init__(self, values):
        self.values = values
        self.reads: list[int] = []

    def __len__(self):
        pytest.fail("synchronous __len__ used on the async path")

    def __getitem__(self, index):
        pytest.fail("synchronous __getitem__ used on the async path")

    async def alen(self):
        return len(self.values)

    async def aget(self, index):
        self.reads.append(index)
        return self.values[index]


def _async_only_xcom_arg(values):
    """An XComArg whose sync ``resolve`` must never run and whose ``aresolve`` gives an async-read value."""
    xcom_arg = make_xcom_arg(None)
    xcom_arg.resolve = lambda *a, **kw: pytest.fail("synchronous resolve() used on the async path")

    async def aresolve(*a, **kw):
        return AsyncOnlyValues(values)

    xcom_arg.aresolve = aresolve
    return xcom_arg


async def _items(expand_input: ExpandInput, context=None) -> list:
    """Every item of the input in index order, the way IterableOperator.execute reads it."""
    length, aget = await expand_input.aresolve(context or {})
    return [await aget(index) for index in range(length)]


class TestExpandInput:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ({"a": [1, 2, 3]}, [{"a": 1}, {"a": 2}, {"a": 3}]),
            (
                {"a": [1, 2], "b": [10, 20]},
                [{"a": 1, "b": 10}, {"a": 1, "b": 20}, {"a": 2, "b": 10}, {"a": 2, "b": 20}],
            ),
            ({"a": [1, 2], "b": [10, 20], "c": ["x"]}, None),
            ({"a": (x for x in [1, 2])}, [{"a": 1}, {"a": 2}]),
            ({"a": {"x": 1, "y": 2}}, [{"a": ("x", 1)}, {"a": ("y", 2)}]),
            ({"a": []}, []),
            ({"a": [1, 2], "b": []}, []),
            ({"a": AsyncOnlyValues([1, 2])}, [{"a": 1}, {"a": 2}]),
        ],
    )
    async def test_dict_of_lists_expand_input_aresolve(self, actual, expected):
        """
        The cross product in the order ``.expand()`` uses (the last argument varies fastest). A dict
        value expands to its (key, value) pairs, mirroring _expand_mapped_field's handling of dict
        values, so .iterate() and .expand() hand sub-tasks the same per-index value for a dict argument.
        """
        expand_input = DictOfListsExpandInput(actual)
        resolved = await expand_input.aresolve({})
        assert isinstance(resolved, Resolved)

        # Read from this resolution: a generator argument is consumed by the pull and cannot be resolved twice.
        items = [await resolved.aget(index) for index in range(resolved.length)]
        if expected is None:
            expected = [
                {"a": a, "b": b, "c": "x"} for a in (1, 2) for b in (10, 20)
            ]  # 3 arguments: 2 x 2 x 1 combinations
        assert items == expected
        assert resolved.length == len(expected)

    @pytest.mark.asyncio
    async def test_dict_of_lists_expand_input_aresolve_pulls_xcom_args_with_aresolve(self):
        expand_input = DictOfListsExpandInput({"a": _async_only_xcom_arg([1, 2]), "b": [10, 20]})

        assert await _items(expand_input) == [
            {"a": 1, "b": 10},
            {"a": 1, "b": 20},
            {"a": 2, "b": 10},
            {"a": 2, "b": 20},
        ]

    @pytest.mark.asyncio
    async def test_dict_of_lists_expand_input_aresolve_reads_each_index_on_demand(self):
        """Sources are pulled once up front, but items are read only as the consumer asks for them."""
        source = AsyncOnlyValues([0, 1, 2])
        expand_input = DictOfListsExpandInput({"a": source, "b": [10, 20]})
        length, aget = await expand_input.aresolve({})

        assert length == 6
        assert source.reads == []
        assert await aget(0) == {"a": 0, "b": 10}
        assert await aget(1) == {"a": 0, "b": 20}
        assert source.reads == [0, 0]
        assert await aget(2) == {"a": 1, "b": 10}
        assert source.reads == [0, 0, 1]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ([{"a": 1}, {"a": 2}], [{"a": 1}, {"a": 2}]),
            ([{"a": 1, "b": 2}], [{"a": 1, "b": 2}]),
            ([], []),
        ],
    )
    async def test_list_of_dicts_expand_input_aresolve(self, actual, expected):
        expand_input = ListOfDictsExpandInput(actual)
        assert await _items(expand_input) == expected

    @pytest.mark.asyncio
    async def test_list_of_dicts_expand_input_aresolve_pulls_xcom_args_with_aresolve(self):
        """An XComArg is one mapping per item, as for ``expand_kwargs``: the whole list, or one entry of it."""
        whole = ListOfDictsExpandInput(_async_only_xcom_arg([{"a": 1}, {"a": 2}]))
        assert await _items(whole) == [{"a": 1}, {"a": 2}]

        entry = make_xcom_arg(None)

        async def aresolve(*a, **kw):
            return {"a": 1}

        entry.aresolve = aresolve
        per_item = ListOfDictsExpandInput([{"a": 0}, entry])
        assert await _items(per_item) == [{"a": 0}, {"a": 1}]

    @pytest.mark.asyncio
    async def test_list_of_dicts_expand_input_aresolve_rejects_non_mapping_items(self):
        expand_input = ListOfDictsExpandInput(_async_only_xcom_arg([{"a": 1}, 2]))
        _, aget = await expand_input.aresolve({})

        assert await aget(0) == {"a": 1}
        with pytest.raises(ValueError, match=r"iterate_kwargs\(\) expects a list\[dict\], not list\[int\]"):
            await aget(1)

    def test_decorated_expand_inputs_compare_by_their_delegate(self):
        one = DecoratedExpandInput(ListOfDictsExpandInput([{"a": 1}]))
        same = DecoratedExpandInput(ListOfDictsExpandInput([{"a": 1}]))
        other = DecoratedExpandInput(ListOfDictsExpandInput([{"a": 2}]))

        assert one == same
        assert one != other

    @pytest.mark.asyncio
    async def test_decorated_expand_input_aresolve_wraps_op_kwargs(self):
        decorated = DecoratedExpandInput(ListOfDictsExpandInput([{"a": 1}, {"a": 2}]))

        assert await _items(decorated) == [{"op_kwargs": {"a": 1}}, {"op_kwargs": {"a": 2}}]

    @pytest.mark.asyncio
    async def test_base_aresolve_is_abstract(self):
        class Incomplete(ExpandInput):
            EXPAND_INPUT_TYPE = "incomplete"

            @property
            def value(self):
                return None

        with pytest.raises(NotImplementedError):
            await Incomplete().aresolve({})


class TestSource:
    """One resolved expand argument, read by index the way ``_expand_mapped_field`` picks it."""

    @pytest.mark.asyncio
    async def test_mapping_is_read_as_its_items(self):
        source = await Source.from_argument({"x": 1, "y": 2}, {})
        assert await source.alen() == 2
        assert [await source.aget(index) for index in range(2)] == [("x", 1), ("y", 2)]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["hello", b"bytes", 1, None, 2.5])
    async def test_scalars_and_strings_are_refused(self, value):
        """A literal that is no collection is refused at parse time; one that gets here is refused too."""
        with pytest.raises(TypeError, match=f"cannot iterate over a '{type(value).__name__}' argument"):
            await Source.from_argument(value, {})

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [[1, 2], (1, 2), range(1, 3)])
    async def test_in_memory_sequences_are_read_in_place(self, value, monkeypatch):
        """Nothing here can block on a supervisor call, so no read goes through a worker thread."""
        monkeypatch.setattr(asyncio, "to_thread", lambda *a, **kw: pytest.fail("to_thread used"))
        source = await Source.from_argument(value, {})
        assert [await source.aget(index) for index in range(2)] == [1, 2]

    @pytest.mark.asyncio
    async def test_other_iterables_are_materialized(self):
        source = await Source.from_argument((x for x in [1, 2]), {})
        assert await source.alen() == 2
        assert await source.aget(1) == 2

    @pytest.mark.asyncio
    async def test_xcom_arg_is_pulled_with_aresolve_then_normalised(self):
        source = await Source.from_argument(_async_only_xcom_arg([1, 2]), {})
        assert await source.alen() == 2
        assert await source.aget(1) == 2

        xcom_arg = make_xcom_arg(None)

        async def aresolve(*a, **kw):
            return {"x": 1}

        xcom_arg.aresolve = aresolve
        source = await Source.from_argument(xcom_arg, {})
        assert await source.aget(0) == ("x", 1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["hello", b"bytes", 1, 2.5, object()])
    async def test_an_unmappable_upstream_value_is_rejected(self, value):
        """
        What ``.expand()`` refuses at push time is refused here at read time.

        An upstream with a mapped dependant raises ``UnmappableXComTypePushed`` from
        ``_push_xcom_if_needed``, but an IterableOperator is no ``MappedOperator`` and is not found
        by ``iter_mapped_dependants``, so that check never fires for it; without this one a string
        from upstream would be iterated as a single item.
        """
        with pytest.raises(UnmappableXComTypePushed, match=type(value).__name__):
            await Source.from_argument(make_xcom_arg(value), {})

    @pytest.mark.asyncio
    async def test_an_upstream_that_pushed_nothing_is_rejected(self):
        with pytest.raises(XComForMappingNotPushed):
            await Source.from_argument(make_xcom_arg(None), {})

    @pytest.mark.asyncio
    async def test_async_accessors_are_preferred(self, monkeypatch):
        monkeypatch.setattr(asyncio, "to_thread", lambda *a, **kw: pytest.fail("to_thread used"))
        value = AsyncOnlyValues([1, 2])
        source = await Source.from_argument(value, {})
        assert await source.alen() == 2
        assert await source.aget(1) == 2
        assert value.reads == [1]

    @pytest.mark.asyncio
    async def test_sequences_without_async_accessors_are_read_off_the_loop_thread(self):
        """
        A sequence may block in ``__len__``/``__getitem__`` with a synchronous supervisor call (a
        ``.map()`` result over a mapped upstream), which on the loop thread would deadlock with the
        ``asend`` calls in flight, so it is read in a worker thread.
        """
        threads: list[threading.Thread] = []

        class Blocking:
            def __len__(self):
                threads.append(threading.current_thread())
                return 2

            def __getitem__(self, index):
                threads.append(threading.current_thread())
                return [1, 2][index]

        source = Source(Blocking())
        assert await source.alen() == 2
        assert await source.aget(1) == 2
        assert len(threads) == 2
        assert all(thread is not threading.current_thread() for thread in threads)
        assert asyncio.get_running_loop().is_running()


class TestIndexForEachField:
    """The one cross-product rule behind ``_expand_mapped_field`` (``.expand()``) and ``aresolve`` (``.iterate()``)."""

    @pytest.mark.parametrize(
        ("map_index", "expected"),
        [
            (0, {"a": 0, "b": 0, "c": 0}),
            (1, {"a": 0, "b": 0, "c": 1}),
            (2, {"a": 0, "b": 1, "c": 0}),
            (5, {"a": 0, "b": 2, "c": 1}),
            (6, {"a": 1, "b": 0, "c": 0}),
            (11, {"a": 1, "b": 2, "c": 1}),
        ],
    )
    def test_last_argument_varies_fastest(self, map_index, expected):
        assert index_for_each_field(map_index, {"a": 2, "b": 3, "c": 2}) == expected

    def test_single_argument_is_the_position_itself(self):
        assert index_for_each_field(4, {"a": 9}) == {"a": 4}

    def test_matches_itertools_product_order(self):
        import itertools

        lengths = {"a": 2, "b": 3, "c": 2}
        positions = [tuple(index_for_each_field(i, lengths).values()) for i in range(12)]
        assert positions == list(itertools.product(range(2), range(3), range(2)))

    def test_zero_length_argument_cannot_be_expanded(self):
        with pytest.raises(RuntimeError, match="cannot expand field mapped to length 0"):
            index_for_each_field(0, {"a": 2, "b": 0})

    @pytest.mark.asyncio
    async def test_expand_and_iterate_pick_the_same_item_for_a_position(self):
        """``resolve`` at ``map_index`` and ``aresolve``'s ``aget`` at that index agree, dict argument included."""
        expand_input = DictOfListsExpandInput({"a": [1, 2], "b": {"x": 10, "y": 20, "z": 30}})
        _, aget = await expand_input.aresolve({})
        for map_index in range(6):
            ti = type("TI", (), {"map_index": map_index, "_upstream_map_indexes": {}})()
            expanded, _ = expand_input.resolve({"ti": ti})
            assert await aget(map_index) == dict(expanded)


def test_mapped_argument_is_keyword_only():
    """``MappedArgument`` takes its input and key by keyword, as on main."""
    expand_input = DictOfListsExpandInput({"a": [1, 2]})

    with pytest.raises(TypeError):
        MappedArgument(expand_input, "a")  # type: ignore[misc]
    assert MappedArgument(input=expand_input, key="a") == MappedArgument(input=expand_input, key="a")
