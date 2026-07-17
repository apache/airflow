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

import ast
import textwrap

import pytest
from ci.prek.new_session_in_provide_session import (
    _annotation_has_none,
    _get_session_arg_and_default,
    _is_decorated_correctly,
    _is_new_session_or_none,
    _iter_incorrect_new_session_usages,
    _SessionDefault,
)


def _parse_func_args(code: str) -> ast.arguments:
    """Parse a function definition and return its arguments node."""
    node = ast.parse(textwrap.dedent(code)).body[0]
    assert isinstance(node, ast.FunctionDef)
    return node.args


def _parse_expr(code: str) -> ast.expr:
    """Parse a single expression."""
    node = ast.parse(code, mode="eval").body
    return node


@pytest.fixture
def check_session_code(write_python_file):
    """Factory fixture: write code to a temp file and check for incorrect NEW_SESSION usages."""

    def _check(code: str) -> list[ast.FunctionDef]:
        path = write_python_file(code)
        return list(_iter_incorrect_new_session_usages(path))

    return _check


class TestGetSessionArgAndDefault:
    def test_no_session_arg(self):
        args = _parse_func_args("def foo(x, y): pass")
        assert _get_session_arg_and_default(args) is None

    def test_session_positional_no_default(self):
        args = _parse_func_args("def foo(session): pass")
        result = _get_session_arg_and_default(args)
        assert result is not None
        assert result.argument.arg == "session"
        assert result.default is None

    def test_session_positional_with_default_none(self):
        args = _parse_func_args("def foo(session=None): pass")
        result = _get_session_arg_and_default(args)
        assert result is not None
        assert result.argument.arg == "session"
        assert isinstance(result.default, ast.Constant)
        assert result.default.value is None

    def test_session_kwonly_with_default(self):
        args = _parse_func_args("def foo(*, session=NEW_SESSION): pass")
        result = _get_session_arg_and_default(args)
        assert result is not None
        assert result.argument.arg == "session"
        assert isinstance(result.default, ast.Name)

    def test_session_among_other_args(self):
        args = _parse_func_args("def foo(x, y, session=None, z=5): pass")
        result = _get_session_arg_and_default(args)
        assert result is not None
        assert result.argument.arg == "session"

    def test_kwonly_session_among_other_kwargs(self):
        args = _parse_func_args("def foo(x, *, timeout=30, session=None): pass")
        result = _get_session_arg_and_default(args)
        assert result is not None
        assert result.argument.arg == "session"


class TestIsNewSessionOrNone:
    def test_none_constant(self):
        expr = _parse_expr("None")
        assert _is_new_session_or_none(expr) == _SessionDefault.none

    def test_new_session_name(self):
        expr = _parse_expr("NEW_SESSION")
        assert _is_new_session_or_none(expr) == _SessionDefault.new_session

    def test_other_name(self):
        expr = _parse_expr("SOMETHING_ELSE")
        assert _is_new_session_or_none(expr) is None

    def test_integer_constant(self):
        expr = _parse_expr("42")
        assert _is_new_session_or_none(expr) is None

    def test_string_constant(self):
        expr = _parse_expr("'hello'")
        assert _is_new_session_or_none(expr) is None


class TestIsDecoratedCorrectly:
    def test_provide_session_decorator(self):
        func = ast.parse("@provide_session\ndef foo(): pass").body[0]
        assert _is_decorated_correctly(func.decorator_list) is True

    def test_overload_decorator(self):
        func = ast.parse("@overload\ndef foo(): pass").body[0]
        assert _is_decorated_correctly(func.decorator_list) is True

    def test_abstractmethod_decorator(self):
        func = ast.parse("@abstractmethod\ndef foo(): pass").body[0]
        assert _is_decorated_correctly(func.decorator_list) is True

    def test_no_decorator(self):
        func = ast.parse("def foo(): pass").body[0]
        assert _is_decorated_correctly(func.decorator_list) is False

    def test_unrelated_decorator(self):
        func = ast.parse("@staticmethod\ndef foo(): pass").body[0]
        assert _is_decorated_correctly(func.decorator_list) is False

    def test_multiple_decorators_with_provide_session(self):
        code = "@staticmethod\n@provide_session\ndef foo(): pass"
        func = ast.parse(code).body[0]
        assert _is_decorated_correctly(func.decorator_list) is True


class TestAnnotationHasNone:
    def test_none_value(self):
        assert _annotation_has_none(None) is False

    def test_none_constant(self):
        expr = _parse_expr("None")
        assert _annotation_has_none(expr) is True

    def test_non_none_constant(self):
        expr = _parse_expr("42")
        assert _annotation_has_none(expr) is False

    def test_union_with_none(self):
        expr = _parse_expr("int | None")
        assert _annotation_has_none(expr) is True

    def test_union_without_none(self):
        expr = _parse_expr("int | str")
        assert _annotation_has_none(expr) is False

    def test_nested_union_with_none(self):
        expr = _parse_expr("int | str | None")
        assert _annotation_has_none(expr) is True

    def test_name_type(self):
        expr = _parse_expr("Session")
        assert _annotation_has_none(expr) is False


class TestIterIncorrectNewSessionUsages:
    def test_correct_provide_session(self, check_session_code):
        code = """\
        @provide_session
        def foo(session=NEW_SESSION):
            pass
        """
        assert check_session_code(code) == []

    def test_incorrect_new_session_without_decorator(self, check_session_code):
        code = """\
        def foo(session=NEW_SESSION):
            pass
        """
        errors = check_session_code(code)
        assert len(errors) == 1
        assert errors[0].name == "foo"

    def test_no_session_arg(self, check_session_code):
        code = """\
        def foo(x, y):
            pass
        """
        assert check_session_code(code) == []

    def test_session_no_default(self, check_session_code):
        code = """\
        def foo(session):
            pass
        """
        assert check_session_code(code) == []

    def test_none_default_with_none_annotation(self, check_session_code):
        code = """\
        def foo(session: Session | None = None):
            pass
        """
        assert check_session_code(code) == []

    def test_none_default_without_none_annotation(self, check_session_code):
        code = """\
        def foo(session: Session = None):
            pass
        """
        errors = check_session_code(code)
        assert len(errors) == 1

    def test_overload_allows_new_session(self, check_session_code):
        code = """\
        @overload
        def foo(session=NEW_SESSION):
            pass
        """
        assert check_session_code(code) == []

    def test_abstractmethod_allows_new_session(self, check_session_code):
        code = """\
        @abstractmethod
        def foo(session=NEW_SESSION):
            pass
        """
        assert check_session_code(code) == []

    def test_other_default_value_is_ignored(self, check_session_code):
        code = """\
        def foo(session="default"):
            pass
        """
        assert check_session_code(code) == []
