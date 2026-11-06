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

import textwrap
from pathlib import Path

import check_trigger_serialize_init as check_module
import pytest
from check_trigger_serialize_init import ModuleAnalyzer, _get_init_param_names


@pytest.fixture
def analyzer_factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Factory: write Python source to a temp file rooted at *tmp_path* and return a
    ModuleAnalyzer for it.

    ``ModuleAnalyzer.get_violations()`` computes paths relative to
    ``AIRFLOW_PROVIDERS_ROOT_PATH`` for its violation report, so we redirect that
    constant to *tmp_path* for the lifetime of each test. We resolve both ends of
    the comparison so symlinked tmp dirs (``/var`` vs ``/private/var`` on macOS)
    don't cause spurious ``relative_to`` failures.
    """
    resolved_root = tmp_path.resolve()
    monkeypatch.setattr(check_module, "AIRFLOW_PROVIDERS_ROOT_PATH", resolved_root)

    def _make(source: str, *, name: str = "trigger.py") -> ModuleAnalyzer:
        path = resolved_root / name
        path.write_text(textwrap.dedent(source))
        return ModuleAnalyzer(path)

    return _make


def _missing_init_params(analyzer: ModuleAnalyzer, class_name: str) -> set[str] | None:
    """Compute ``__init__`` params not preserved by ``serialize()``.

    Returns ``None`` when ``serialize()`` is unresolvable. Mirrors the core check that
    ``ModuleAnalyzer.get_violations()`` performs, without the report-path machinery.
    """
    cls = analyzer.classes[class_name]
    init_resolved = analyzer._resolve_method(cls, "__init__")
    assert init_resolved is not None, f"{class_name}.__init__ should resolve in this test"
    params = _get_init_param_names(init_resolved[0])
    serialize_keys = analyzer._get_serialize_keys(cls)
    if serialize_keys is None:
        return None
    return params - serialize_keys


class TestDictLiteralResolver:
    """The original resolver path: ``return "<path>", {"key": ..., ...}``."""

    def test_literal_dict_keys_resolved(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            class FooTrigger:
                def __init__(self, a, b, c):
                    self.a, self.b, self.c = a, b, c
                def serialize(self):
                    return "x.FooTrigger", {"a": self.a, "b": self.b, "c": self.c}
            """,
        )
        keys = analyzer._get_serialize_keys(analyzer.classes["FooTrigger"])
        assert keys == {"a", "b", "c"}

    def test_missing_param_flagged(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            class FooTrigger:
                def __init__(self, a, b, c):
                    self.a, self.b, self.c = a, b, c
                def serialize(self):
                    return "x.FooTrigger", {"a": self.a, "b": self.b}
            """,
        )
        assert _missing_init_params(analyzer, "FooTrigger") == {"c"}
        assert analyzer.get_violations() == [("FooTrigger", ["c"])]

    def test_super_spread_resolved_via_in_file_base(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            class BaseTrigger:
                def __init__(self, a):
                    self.a = a
                def serialize(self):
                    return "x.BaseTrigger", {"a": self.a}

            class ChildTrigger(BaseTrigger):
                def __init__(self, a, b):
                    super().__init__(a)
                    self.b = b
                def serialize(self):
                    return "x.ChildTrigger", {**super().serialize()[1], "b": self.b}
            """,
        )
        keys = analyzer._get_serialize_keys(analyzer.classes["ChildTrigger"])
        assert keys == {"a", "b"}

    def test_non_constant_key_unresolvable(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            KEY = "a"

            class FooTrigger:
                def __init__(self, a):
                    self.a = a
                def serialize(self):
                    return "x.FooTrigger", {KEY: self.a}
            """,
        )
        assert analyzer._get_serialize_keys(analyzer.classes["FooTrigger"]) is None


class TestDictViaVariableResolver:
    """New resolver: ``data = {...}; data['x'] = ...; data.update({...}); return ..., data``."""

    def test_plain_assign_init(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            class FooTrigger:
                def __init__(self, a, b):
                    self.a, self.b = a, b
                def serialize(self):
                    data = {"a": self.a, "b": self.b}
                    return "x.FooTrigger", data
            """,
        )
        keys = analyzer._get_serialize_keys(analyzer.classes["FooTrigger"])
        assert keys == {"a", "b"}

    def test_annotated_assign_init(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            from typing import Any

            class FooTrigger:
                def __init__(self, a, b):
                    self.a, self.b = a, b
                def serialize(self):
                    data: dict[str, Any] = {"a": self.a, "b": self.b}
                    return "x.FooTrigger", data
            """,
        )
        keys = analyzer._get_serialize_keys(analyzer.classes["FooTrigger"])
        assert keys == {"a", "b"}

    def test_subscript_assignment_adds_keys(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            class FooTrigger:
                def __init__(self, a, b, c):
                    self.a, self.b, self.c = a, b, c
                def serialize(self):
                    data = {"a": self.a}
                    data["b"] = self.b
                    data["c"] = self.c
                    return "x.FooTrigger", data
            """,
        )
        keys = analyzer._get_serialize_keys(analyzer.classes["FooTrigger"])
        assert keys == {"a", "b", "c"}

    def test_update_with_literal_dict_adds_keys(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            class FooTrigger:
                def __init__(self, a, b, c):
                    self.a, self.b, self.c = a, b, c
                def serialize(self):
                    data = {"a": self.a}
                    data.update({"b": self.b, "c": self.c})
                    return "x.FooTrigger", data
            """,
        )
        keys = analyzer._get_serialize_keys(analyzer.classes["FooTrigger"])
        assert keys == {"a", "b", "c"}

    def test_conditional_branches_unioned(self, analyzer_factory) -> None:
        """Mirrors the WorkflowTrigger pattern: keys conditionally added are still preserved."""
        analyzer = analyzer_factory(
            """
            FLAG = True

            class FooTrigger:
                def __init__(self, a, b, c):
                    self.a, self.b, self.c = a, b, c
                def serialize(self):
                    data = {"a": self.a}
                    if FLAG:
                        data["b"] = self.b
                    else:
                        data["c"] = self.c
                    return "x.FooTrigger", data
            """,
        )
        keys = analyzer._get_serialize_keys(analyzer.classes["FooTrigger"])
        assert keys == {"a", "b", "c"}

    def test_missing_param_flagged_with_var_pattern(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            class FooTrigger:
                def __init__(self, a, b, c):
                    self.a, self.b, self.c = a, b, c
                def serialize(self):
                    data = {"a": self.a, "b": self.b}
                    return "x.FooTrigger", data
            """,
        )
        assert _missing_init_params(analyzer, "FooTrigger") == {"c"}
        assert analyzer.get_violations() == [("FooTrigger", ["c"])]

    @pytest.mark.parametrize(
        "serialize_body",
        [
            pytest.param(
                ["data = {}", "data = some_helper()", "return 'x.T', data"],
                id="reassign-non-dict",
            ),
            pytest.param(
                ["data = {'a': 1}", "data = {'b': 2}", "return 'x.T', data"],
                id="multiple-literal-inits",
            ),
            pytest.param(
                ["k = 'a'", "data = {}", "data[k] = self.a", "return 'x.T', data"],
                id="dynamic-subscript-key",
            ),
            pytest.param(
                ["data = {}", "data.update(other)", "return 'x.T', data"],
                id="update-with-non-literal",
            ),
            pytest.param(
                ["data = {}", "data.update(a=1)", "return 'x.T', data"],
                id="update-with-kwargs",
            ),
            pytest.param(
                ["return 'x.T', data"],
                id="never-initialized",
            ),
        ],
    )
    def test_unresolvable_returns_none(self, analyzer_factory, serialize_body: list[str]) -> None:
        """The resolver returns ``None`` rather than guess on any of these dynamic shapes."""
        # ``serialize_body`` is a list of statement lines; join with leading whitespace
        # matching the column of ``{body}`` in the f-string template (20 spaces) so all
        # body lines land at the same column after textwrap.dedent.
        body = "\n                    ".join(serialize_body)
        analyzer = analyzer_factory(
            f"""
            class FooTrigger:
                def __init__(self, a):
                    self.a = a
                def serialize(self):
                    {body}
            """,
        )
        assert analyzer._get_serialize_keys(analyzer.classes["FooTrigger"]) is None

    def test_nested_helper_does_not_pollute_outer_var(self, analyzer_factory) -> None:
        """A same-named ``data`` inside a nested function must not contribute keys to the outer var."""
        analyzer = analyzer_factory(
            """
            class FooTrigger:
                def __init__(self, a):
                    self.a = a
                def serialize(self):
                    data = {"a": self.a}

                    def _bogus():
                        data = {"b": 1}      # different scope -- must be ignored
                        data["c"] = 2
                        return data

                    return "x.FooTrigger", data
            """,
        )
        keys = analyzer._get_serialize_keys(analyzer.classes["FooTrigger"])
        # Outer ``data`` only has ``a``; the nested helper's ``b`` and ``c`` must not leak in.
        assert keys == {"a"}


class TestUnresolvedSerializeShapes:
    """Verify shapes that should still be skipped post-extension."""

    def test_multiple_returns_unresolvable(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            class FooTrigger:
                def __init__(self, a):
                    self.a = a
                def serialize(self):
                    if self.a:
                        return "x.FooTrigger", {"a": self.a}
                    return "x.FooTrigger", {"a": None}
            """,
        )
        # Original constraint: exactly one return statement.
        assert analyzer._get_serialize_keys(analyzer.classes["FooTrigger"]) is None

    def test_return_value_unrecognized_shape(self, analyzer_factory) -> None:
        analyzer = analyzer_factory(
            """
            class FooTrigger:
                def __init__(self, a):
                    self.a = a
                def serialize(self):
                    return self._build_payload()
            """,
        )
        assert analyzer._get_serialize_keys(analyzer.classes["FooTrigger"]) is None
