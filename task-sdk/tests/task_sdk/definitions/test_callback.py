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

import importlib
import sys
from pathlib import Path
from typing import cast

import pytest

from airflow.sdk._shared.module_loading import qualname
from airflow.sdk._shared.serialization import DATA
from airflow.sdk.definitions.callback import AsyncCallback, Callback, SyncCallback
from airflow.serialization.serde import deserialize, serialize


async def empty_async_callback_for_deadline_tests():
    """Used in a number of tests to confirm that Deadlines and DeadlineAlerts function correctly."""
    pass


def empty_sync_callback_for_deadline_tests():
    """Used in a number of tests to confirm that Deadlines and DeadlineAlerts function correctly."""
    pass


TEST_CALLBACK_PATH = qualname(empty_async_callback_for_deadline_tests)
TEST_CALLBACK_KWARGS = {"arg1": "value1"}
UNIMPORTABLE_DOT_PATH = "valid.but.nonexistent.path"

# A module which leaves a trace when its body is executed, so that a test can tell whether
# it was imported. Written to a temporary directory by the `callback_module` fixture below.
CALLBACK_MODULE_SOURCE = '''
from pathlib import Path

Path(__file__).with_suffix(".imported").touch()


def sync_callback():
    """A callback which can be reached by dot path once this module is importable."""


async def async_callback():
    """An awaitable callback which can be reached by dot path once this module is importable."""
'''


@pytest.fixture
def callback_module(tmp_path, monkeypatch):
    """
    Provide a factory which makes a named module importable for the duration of a test.

    The factory returns the module name and the path of a marker file which the module
    creates when it is imported; the marker only exists if the module body has been run.
    """
    monkeypatch.syspath_prepend(str(tmp_path))
    created: list[str] = []

    def _create(module_name: str) -> tuple[str, Path]:
        (tmp_path / f"{module_name}.py").write_text(CALLBACK_MODULE_SOURCE)
        importlib.invalidate_caches()
        created.append(module_name)
        return module_name, tmp_path / f"{module_name}.imported"

    yield _create

    for module_name in created:
        sys.modules.pop(module_name, None)


class TestCallback:
    @pytest.mark.parametrize(
        ("subclass", "callable"),
        [
            pytest.param(AsyncCallback, empty_async_callback_for_deadline_tests, id="async"),
            pytest.param(SyncCallback, empty_sync_callback_for_deadline_tests, id="sync"),
        ],
    )
    def test_init_error_reserved_kwarg(self, subclass, callable):
        with pytest.raises(ValueError, match="context is a reserved kwarg for this class"):
            subclass(callable, {"context": None})

    @pytest.mark.parametrize(
        ("callback_callable", "expected_path"),
        [
            pytest.param(
                empty_sync_callback_for_deadline_tests,
                qualname(empty_sync_callback_for_deadline_tests),
                id="valid_sync_callable",
            ),
            pytest.param(
                empty_async_callback_for_deadline_tests,
                qualname(empty_async_callback_for_deadline_tests),
                id="valid_async_callable",
            ),
            pytest.param(TEST_CALLBACK_PATH, TEST_CALLBACK_PATH, id="valid_path_string"),
            pytest.param(lambda x: x, None, id="lambda_function"),
            pytest.param(TEST_CALLBACK_PATH + "  ", TEST_CALLBACK_PATH, id="path_with_whitespace"),
            pytest.param(UNIMPORTABLE_DOT_PATH, UNIMPORTABLE_DOT_PATH, id="valid_format_not_importable"),
        ],
    )
    def test_get_callback_path_happy_cases(self, callback_callable, expected_path):
        path = Callback.get_callback_path(callback_callable)
        if expected_path is None:
            assert path.endswith("<lambda>")
        else:
            assert path == expected_path

    @pytest.mark.parametrize(
        ("callback_callable", "error_type"),
        [
            pytest.param(42, ImportError, id="not_a_string"),
            pytest.param("", ImportError, id="empty_string"),
            pytest.param("os.path", AttributeError, id="non_callable_module"),
        ],
    )
    def test_get_callback_path_error_cases(self, callback_callable, error_type):
        expected_message = ""
        if error_type is ImportError:
            expected_message = "doesn't look like a valid dot path."
        elif error_type is AttributeError:
            expected_message = "is not callable."

        with pytest.raises(error_type, match=expected_message):
            Callback.get_callback_path(callback_callable)

    @pytest.mark.parametrize(
        ("callback1_args", "callback2_args", "should_equal"),
        [
            pytest.param(
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                True,
                id="identical",
            ),
            pytest.param(
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (UNIMPORTABLE_DOT_PATH, TEST_CALLBACK_KWARGS),
                False,
                id="different_path",
            ),
            pytest.param(
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, {"other": "kwargs"}),
                False,
                id="different_kwargs",
            ),
            pytest.param((TEST_CALLBACK_PATH, None), (TEST_CALLBACK_PATH, None), True, id="both_no_kwargs"),
        ],
    )
    def test_callback_equality(self, callback1_args, callback2_args, should_equal):
        callback1 = AsyncCallback(*callback1_args)
        callback2 = AsyncCallback(*callback2_args)
        assert (callback1 == callback2) == should_equal

    @pytest.mark.parametrize(
        ("callback_class", "args1", "args2", "should_be_same_hash"),
        [
            pytest.param(
                AsyncCallback,
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                True,
                id="async_identical",
            ),
            pytest.param(
                SyncCallback,
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                True,
                id="sync_identical",
            ),
            pytest.param(
                AsyncCallback,
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (UNIMPORTABLE_DOT_PATH, TEST_CALLBACK_KWARGS),
                False,
                id="async_different_path",
            ),
            pytest.param(
                SyncCallback,
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, {"other": "kwargs"}),
                False,
                id="sync_different_kwargs",
            ),
            pytest.param(
                AsyncCallback,
                (TEST_CALLBACK_PATH, None),
                (TEST_CALLBACK_PATH, None),
                True,
                id="async_no_kwargs",
            ),
        ],
    )
    def test_callback_hash_and_set_behavior(self, callback_class, args1, args2, should_be_same_hash):
        callback1 = callback_class(*args1)
        callback2 = callback_class(*args2)
        assert (hash(callback1) == hash(callback2)) == should_be_same_hash


class TestAsyncCallback:
    @pytest.mark.parametrize(
        ("callback_callable", "kwargs", "expected_path"),
        [
            pytest.param(
                empty_async_callback_for_deadline_tests,
                TEST_CALLBACK_KWARGS,
                TEST_CALLBACK_PATH,
                id="callable",
            ),
            pytest.param(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS, TEST_CALLBACK_PATH, id="string_path"),
            pytest.param(
                UNIMPORTABLE_DOT_PATH, TEST_CALLBACK_KWARGS, UNIMPORTABLE_DOT_PATH, id="unimportable_path"
            ),
        ],
    )
    def test_init(self, callback_callable, kwargs, expected_path):
        callback = AsyncCallback(callback_callable, kwargs=kwargs)
        assert callback.path == expected_path
        assert callback.kwargs == kwargs
        assert isinstance(callback, Callback)

    def test_init_error(self):
        with pytest.raises(AttributeError, match="is not awaitable."):
            AsyncCallback(empty_sync_callback_for_deadline_tests)

    def test_serialize_deserialize(self):
        callback = AsyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS)
        serialized = serialize(callback)
        deserialized = cast("Callback", deserialize(serialized.copy()))
        assert callback == deserialized


class TestSyncCallback:
    @pytest.mark.parametrize(
        ("callback_callable", "executor"),
        [
            pytest.param(empty_sync_callback_for_deadline_tests, "remote", id="with_executor"),
            pytest.param(empty_sync_callback_for_deadline_tests, None, id="without_executor"),
            pytest.param(qualname(empty_sync_callback_for_deadline_tests), None, id="importable_path"),
            pytest.param(UNIMPORTABLE_DOT_PATH, None, id="unimportable_path"),
        ],
    )
    def test_init(self, callback_callable, executor):
        callback = SyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS, executor=executor)

        assert callback.path == TEST_CALLBACK_PATH
        assert callback.kwargs == TEST_CALLBACK_KWARGS
        assert callback.executor == executor
        assert isinstance(callback, Callback)

    def test_serialize_deserialize(self):
        callback = SyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS, executor="local")
        serialized = serialize(callback)
        deserialized = cast("Callback", deserialize(serialized.copy()))
        assert callback == deserialized


class TestCallbackPathHandling:
    """Cover how a callback path is treated when it is supplied, versus read back from storage."""

    def test_init_imports_the_module_named_by_the_path(self, callback_module):
        """A Callback created from a dot path resolves it, so its author gets feedback on it."""
        module_name, marker = callback_module("callback_module_resolved_on_creation")
        path = f"{module_name}.sync_callback"

        callback = SyncCallback(path)

        assert marker.exists(), "the module named by the path should have been imported"
        assert callback.path == path

    def test_deserialize_does_not_import_the_module_named_by_the_stored_path(self, callback_module):
        """Rebuilding a Callback keeps the stored path without resolving it again."""
        module_name, marker = callback_module("callback_module_not_resolved_on_deserialize")
        path = f"{module_name}.sync_callback"

        callback = SyncCallback.deserialize({"path": path, "kwargs": {}, "executor": None}, 0)

        assert not marker.exists(), "the module named by the stored path should not have been imported"
        assert module_name not in sys.modules
        assert callback.path == path
        assert type(callback.path) is str

    def test_deserialize_async_does_not_import_the_module_named_by_the_stored_path(self, callback_module):
        module_name, marker = callback_module("async_callback_module_not_resolved_on_deserialize")
        path = f"{module_name}.async_callback"

        callback = AsyncCallback.deserialize({"path": path, "kwargs": {}}, 0)

        assert not marker.exists(), "the module named by the stored path should not have been imported"
        assert module_name not in sys.modules
        assert callback.path == path

    def test_serde_deserialize_does_not_import_the_module_named_by_the_stored_path(self, callback_module):
        """The same holds when the Callback is rebuilt through serde, as it is from stored data."""
        module_name, marker = callback_module("callback_module_not_resolved_through_serde")
        path = f"{module_name}.sync_callback"

        serialized = serialize(SyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS))
        serialized[DATA]["path"] = path

        deserialized = cast("Callback", deserialize(serialized))

        assert not marker.exists(), "the module named by the stored path should not have been imported"
        assert module_name not in sys.modules
        assert deserialized.path == path

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("not a dot path", id="not_a_dot_path"),
            pytest.param("", id="empty_string"),
            pytest.param(None, id="none"),
            pytest.param(42, id="not_a_string"),
        ],
    )
    def test_deserialize_rejects_a_path_which_is_not_a_dot_path(self, path):
        """A stored value which is not shaped like a dot path is still rejected."""
        with pytest.raises(ImportError, match="doesn't look like a valid dot path."):
            SyncCallback.deserialize({"path": path, "kwargs": {}, "executor": None}, 0)

    def test_deserialize_keeps_a_stored_path_which_no_longer_points_at_a_callable(self):
        """
        The stored path is taken as it was stored.

        Unlike a path handed to the constructor, it is not checked against what it currently
        resolves to; that check belongs to the moment the Callback is created.
        """
        callback = SyncCallback.deserialize({"path": "os.path", "kwargs": {}, "executor": None}, 0)

        assert callback.path == "os.path"

    def test_deserialize_round_trip_keeps_kwargs_and_executor(self):
        callback = SyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS, executor="local")

        deserialized = cast("SyncCallback", deserialize(serialize(callback)))

        assert deserialized == callback
        assert deserialized.kwargs == TEST_CALLBACK_KWARGS
        assert deserialized.executor == "local"


# While DeadlineReference lives in the SDK package, the unit tests to confirm it
# works need database access so they live in the models/test_deadline.py module.
