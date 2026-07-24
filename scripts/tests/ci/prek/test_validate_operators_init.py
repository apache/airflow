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
import sys
import textwrap
from pathlib import Path

import pytest
import validate_operators_init
from validate_operators_init import (
    _check_constructor_field_logic,
    _check_constructor_template_fields,
    _extract_template_fields,
    _is_operator,
    main,
)


def _first_class(code: str) -> ast.ClassDef:
    tree = ast.parse(textwrap.dedent(code))
    return next(node for node in ast.walk(tree) if isinstance(node, ast.ClassDef))


def _logic_findings(code: str, template_fields: list[str]) -> int:
    code = textwrap.dedent(code)
    return _check_constructor_field_logic(_first_class(code), template_fields, code.splitlines())


def _operator_code(ctor_body: str) -> str:
    body = textwrap.indent(textwrap.dedent(ctor_body).strip("\n"), " " * 8)
    return (
        "class MyOperator(BaseOperator):\n"
        '    template_fields = ("foo",)\n'
        "\n"
        "    def __init__(self, foo=None, **kwargs):\n"
        f"{body}\n"
    )


class TestConstructorFieldLogic:
    @pytest.mark.parametrize(
        "ctor_body, expected",
        [
            pytest.param("self.foo = foo", 0, id="plain-assignment"),
            pytest.param("self.foo = foo or 'default'", 0, id="or-default"),
            pytest.param("self.foo = foo if foo else 'default'", 0, id="ternary-truthiness"),
            pytest.param("self.foo = foo if foo is not None else 'default'", 0, id="ternary-is-not-none"),
            pytest.param("foo = foo or 'default'\nself.foo = foo", 0, id="local-rebind"),
            pytest.param("super().__init__(foo=foo)", 0, id="super-forwarding"),
            pytest.param("self._validate(foo)\nself.foo = foo", 1, id="validation-call"),
            pytest.param(
                "if foo is not None:\n    self._validate(foo)\nself.foo = foo",
                2,
                id="nested-validation-call",
            ),
            pytest.param("self.foo = foo\nself.bar = foo.upper()", 1, id="derived-assignment"),
            pytest.param(
                "self.foo = foo\nif self.foo:\n    self.bar = 1",
                1,
                id="self-attribute-read",
            ),
            pytest.param(
                "if not foo:\n    raise ValueError(f'unsupported: {foo}')\nself.foo = foo",
                2,
                id="ctor-validation-raise",
            ),
        ],
    )
    def test_flags_logic_but_not_sanctioned_patterns(self, ctor_body: str, expected: int):
        assert _logic_findings(_operator_code(ctor_body), ["foo"]) == expected

    def test_name_in_parameter_default_is_not_the_field(self):
        # A field named like a module (e.g. "conf") used in a parameter default evaluates at
        # class-definition scope and must not be flagged.
        code = """
        class MyOperator(BaseOperator):
            template_fields = ("conf",)

            def __init__(self, conf=None, deferrable=conf.getboolean("operators", "x"), **kwargs):
                self.conf = conf
        """
        assert _logic_findings(code, ["conf"]) == 0

    def test_unbound_module_name_matching_field_is_not_flagged(self):
        code = """
        class MyOperator(BaseOperator):
            template_fields = ("json",)

            def __init__(self, **kwargs):
                self.data = json.dumps({})
        """
        assert _logic_findings(code, ["json"]) == 0

    def test_no_constructor_is_clean(self):
        code = """
        class MyOperator(BaseOperator):
            template_fields = ("foo",)
        """
        assert _logic_findings(code, ["foo"]) == 0


class TestOperatorDetection:
    @pytest.mark.parametrize(
        "class_def, expected",
        [
            pytest.param("class Op(BaseOperator):", True, id="base-operator"),
            pytest.param("class Sensor(BaseSensorOperator):", True, id="base-sensor"),
            pytest.param("class Op(AwsBaseOperator[EmrHook]):", True, id="subscripted-base"),
            pytest.param("class Helper:", False, id="plain-class"),
            pytest.param("class Hook(BaseHook):", False, id="hook"),
        ],
    )
    def test_detects_operator_bases(self, class_def: str, expected: bool):
        assert _is_operator(_first_class(f"{class_def}\n    pass")) is expected


class TestTemplateFieldExtraction:
    def test_extracts_helper_call_without_injected_fields(self):
        code = """
        class Op(AwsBaseOperator[EC2Hook]):
            template_fields = aws_template_fields("instance_id", "region_name", "aws_conn_id")
        """
        # region_name / aws_conn_id are injected and assigned by the AWS base operator.
        assert _extract_template_fields(_first_class(code)) == ["instance_id"]

    def test_extracts_tuple(self):
        code = """
        class Op(BaseOperator):
            template_fields = ("a", "b")
        """
        assert _extract_template_fields(_first_class(code)) == ["a", "b"]


class TestValuePreservingTernaryAssignment:
    def test_ternary_default_is_a_valid_assignment(self):
        code = """
        class Op(BaseOperator):
            template_fields = ("conf",)

            def __init__(self, conf=None, **kwargs):
                self.conf = conf if conf else {}
        """
        assert _check_constructor_template_fields(_first_class(code), ["conf"]) == 0


class TestExemptions:
    VIOLATING_OPERATOR = textwrap.dedent(
        """
        class MyOperator(BaseOperator):
            template_fields = ("foo",)

            def __init__(self, foo=None, **kwargs):
                self._validate(foo)
                self.foo = foo
        """
    )
    CLEAN_OPERATOR = textwrap.dedent(
        """
        class MyOperator(BaseOperator):
            template_fields = ("foo",)

            def __init__(self, foo=None, **kwargs):
                self.foo = foo
        """
    )

    def _run(self, monkeypatch, tmp_path: Path, code: str, exemption_line: str | None) -> int:
        target = tmp_path / "my_operator.py"
        target.write_text(code)
        exemptions = tmp_path / "exemptions.txt"
        exemptions.write_text(f"# comment\n{exemption_line}\n" if exemption_line else "# comment\n")
        monkeypatch.setattr(validate_operators_init, "EXEMPTIONS_PATH", exemptions)
        monkeypatch.setattr(sys, "argv", ["validate_operators_init.py", str(target)])
        return main()

    def test_exempted_class_is_suppressed(self, monkeypatch, tmp_path: Path):
        err = self._run(monkeypatch, tmp_path, self.VIOLATING_OPERATOR, "my_operator.py::MyOperator")
        assert err == 0

    def test_violation_without_exemption_fails(self, monkeypatch, tmp_path: Path):
        err = self._run(monkeypatch, tmp_path, self.VIOLATING_OPERATOR, None)
        assert err > 0

    def test_stale_exemption_fails(self, monkeypatch, tmp_path: Path):
        err = self._run(monkeypatch, tmp_path, self.CLEAN_OPERATOR, "my_operator.py::MyOperator")
        assert err == 1

    def test_exemption_for_other_class_does_not_apply(self, monkeypatch, tmp_path: Path):
        err = self._run(monkeypatch, tmp_path, self.VIOLATING_OPERATOR, "my_operator.py::OtherOperator")
        assert err > 0
