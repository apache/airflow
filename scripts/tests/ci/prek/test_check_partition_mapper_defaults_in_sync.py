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

import pytest
from check_partition_mapper_defaults_in_sync import (
    extract_class_error_messages,
    extract_class_field_names,
)


class TestExtractClassErrorMessages:
    @pytest.mark.parametrize(
        ("raise_stmt", "expected"),
        [
            pytest.param('raise ValueError("msg a")', {"msg a"}, id="plain-string"),
            pytest.param('raise ValueError(f"msg b at {i}")', {"msg b at {}"}, id="fstring-template"),
        ],
    )
    def test_extracts_message(self, tmp_path: Path, raise_stmt: str, expected: set[str]):
        """Plain strings are kept verbatim; f-string interpolations become {} placeholders."""
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent(f"""\
                class MyClass:
                    def validate(self, x, i):
                        {raise_stmt}
            """)
        )
        assert extract_class_error_messages(f, "MyClass") == expected

    def test_extracts_both_plain_and_fstring(self, tmp_path: Path):
        """Covers the explicit test requirement: plain + f-string together."""
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                class MyClass:
                    def check(self, item, i):
                        raise ValueError("msg a")

                    def check2(self, item, i):
                        raise ValueError(f"msg b at {i}")
            """)
        )
        result = extract_class_error_messages(f, "MyClass")
        assert result == {"msg a", "msg b at {}"}

    def test_ignores_other_exception_types(self, tmp_path: Path):
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                class MyClass:
                    def check(self):
                        raise TypeError("not a value error")
            """)
        )
        result = extract_class_error_messages(f, "MyClass")
        assert result == set()

    def test_returns_empty_for_missing_class(self, tmp_path: Path):
        f = tmp_path / "code.py"
        f.write_text("x = 1\n")
        result = extract_class_error_messages(f, "Missing")
        assert result == set()

    def test_nested_function_messages_included(self, tmp_path: Path):
        """Messages raised inside nested helpers within the class body are collected."""
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                class MyClass:
                    def __init__(self, items):
                        def _check(i, item):
                            if not isinstance(item, str):
                                raise ValueError(f"must be str; got {type(item).__name__!r} at {i}")
                        for i, item in enumerate(items):
                            _check(i, item)
            """)
        )
        result = extract_class_error_messages(f, "MyClass")
        assert result == {"must be str; got {} at {}"}

    def test_multipart_fstring_message(self, tmp_path: Path):
        """f-string with multiple literal parts and multiple interpolations."""
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                class C:
                    def v(self, item, i):
                        raise ValueError(
                            f"Prefix segment keys must be str; "
                            f"got {type(item).__name__!r} at index {i}: {item!r}"
                        )
            """)
        )
        result = extract_class_error_messages(f, "C")
        # The two adjacent f-strings form a single BinOp(Add) node at AST level;
        # the extractor concatenates them into one template.
        assert "Prefix segment keys must be str; got {} at index {}: {}" in result


class TestExtractClassFieldNames:
    def test_extracts_annotated_instance_fields(self, tmp_path: Path):
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                from typing import ClassVar
                import attrs

                @attrs.define
                class MyClass:
                    expected_decoded_type: ClassVar[type] = str
                    _segments: frozenset[str] = attrs.field()
            """)
        )
        result = extract_class_field_names(f, "MyClass")
        # ClassVar is excluded; _segments is included
        assert result == {"_segments"}

    def test_excludes_classvar_fields(self, tmp_path: Path):
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                from typing import ClassVar

                class MyClass:
                    flag: ClassVar[bool] = False
                    name: str
            """)
        )
        result = extract_class_field_names(f, "MyClass")
        assert result == {"name"}

    def test_returns_empty_for_missing_class(self, tmp_path: Path):
        f = tmp_path / "code.py"
        f.write_text("x = 1\n")
        result = extract_class_field_names(f, "Missing")
        assert result == set()

    def test_private_field_name_preserved(self, tmp_path: Path):
        """Leading underscore in field name is kept verbatim."""
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                import attrs

                @attrs.define
                class Container:
                    _segments: frozenset[str] = attrs.field()
            """)
        )
        result = extract_class_field_names(f, "Container")
        assert "_segments" in result


class TestInSyncPasses:
    def test_in_sync_passes(self, tmp_path: Path):
        """Two files with identical field names and messages compare equal."""
        code = textwrap.dedent("""\
            import attrs

            @attrs.define
            class MyMapper:
                downstream_key: str = attrs.field()

                def validate(self, x):
                    raise ValueError(f"must be non-empty str; got {x!r}.")
        """)
        core_file = tmp_path / "core.py"
        sdk_file = tmp_path / "sdk.py"
        core_file.write_text(code)
        sdk_file.write_text(code)

        core_fields = extract_class_field_names(core_file, "MyMapper")
        sdk_fields = extract_class_field_names(sdk_file, "MyMapper")
        assert core_fields == sdk_fields

        core_msgs = extract_class_error_messages(core_file, "MyMapper")
        sdk_msgs = extract_class_error_messages(sdk_file, "MyMapper")
        assert core_msgs == sdk_msgs


class TestConverterPatternExtraction:
    """Tests for the module-level converter= follow-through in extract_class_error_messages."""

    def test_extracts_messages_from_module_level_converter(self, tmp_path: Path):
        """Converter function outside the class body is followed and its messages are collected."""
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                import attrs

                def _my_convert(items):
                    for i, item in enumerate(items):
                        if not isinstance(item, str):
                            raise ValueError(f"must be str; got {type(item).__name__!r} at {i}")
                    if not items:
                        raise ValueError("must not be empty")
                    return frozenset(items)

                @attrs.define
                class MyClass:
                    _data: frozenset = attrs.field(converter=_my_convert)
            """)
        )
        result = extract_class_error_messages(f, "MyClass")
        assert "must be str; got {} at {}" in result
        assert "must not be empty" in result

    def test_converter_fstring_template_placeholders(self, tmp_path: Path):
        """f-string expressions in the converter become {} placeholders in the template."""
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                import attrs

                def _convert(items):
                    for i, item in enumerate(items):
                        raise ValueError(f"bad item {item!r} at index {i}")
                    return frozenset(items)

                @attrs.define
                class Widget:
                    _items: frozenset = attrs.field(converter=_convert)
            """)
        )
        result = extract_class_error_messages(f, "Widget")
        assert "bad item {} at index {}" in result

    def test_converter_messages_not_collected_for_unrelated_class(self, tmp_path: Path):
        """The converter is only followed for the class that references it."""
        f = tmp_path / "code.py"
        f.write_text(
            textwrap.dedent("""\
                import attrs

                def _my_convert(items):
                    raise ValueError("converter error")
                    return frozenset(items)

                @attrs.define
                class ClassA:
                    _data: frozenset = attrs.field(converter=_my_convert)

                @attrs.define
                class ClassB:
                    _name: str = attrs.field()
            """)
        )
        # ClassA references the converter — should see the message
        result_a = extract_class_error_messages(f, "ClassA")
        assert "converter error" in result_a

        # ClassB does not reference the converter — should NOT see the message
        result_b = extract_class_error_messages(f, "ClassB")
        assert "converter error" not in result_b


class TestConverterDivergenceDetected:
    """Verify that message drift in a module-level converter is caught by the extractor."""

    def test_divergent_converter_message_detected(self, tmp_path: Path):
        """Changing 'non-empty' to 'non-empty strings' in a converter is detected as drift."""
        core_code = textwrap.dedent("""\
            import attrs

            def _convert_segments(segments):
                for i, item in enumerate(segments):
                    if not item:
                        raise ValueError(f"keys must be non-empty; got empty at {i}.")
                return frozenset(segments)

            @attrs.define
            class SegmentWindow:
                _segments: frozenset = attrs.field(converter=_convert_segments)
        """)
        sdk_code_diverged = textwrap.dedent("""\
            import attrs

            def _convert_segments(segments):
                for i, item in enumerate(segments):
                    if not item:
                        raise ValueError(f"keys must be non-empty strings; got empty at {i}.")
                return frozenset(segments)

            @attrs.define
            class SegmentWindow:
                _segments: frozenset = attrs.field(converter=_convert_segments)
        """)
        core_file = tmp_path / "core.py"
        sdk_file = tmp_path / "sdk.py"
        core_file.write_text(core_code)
        sdk_file.write_text(sdk_code_diverged)

        core_msgs = extract_class_error_messages(core_file, "SegmentWindow")
        sdk_msgs = extract_class_error_messages(sdk_file, "SegmentWindow")
        assert core_msgs != sdk_msgs


class TestDivergentMessageFails:
    def test_divergent_message_detected(self, tmp_path: Path):
        """Changing 'non-empty' to 'non-empty strings' on one side is detected."""
        core_code = textwrap.dedent("""\
            import attrs

            @attrs.define
            class SegmentWindow:
                _segments: frozenset[str] = attrs.field()

                def validate(self, item, i):
                    raise ValueError(f"keys must be non-empty; got empty at {i}.")
        """)
        sdk_code_diverged = textwrap.dedent("""\
            import attrs

            @attrs.define
            class SegmentWindow:
                _segments: frozenset[str] = attrs.field()

                def validate(self, item, i):
                    raise ValueError(f"keys must be non-empty strings; got empty at {i}.")
        """)
        core_file = tmp_path / "core.py"
        sdk_file = tmp_path / "sdk.py"
        core_file.write_text(core_code)
        sdk_file.write_text(sdk_code_diverged)

        core_msgs = extract_class_error_messages(core_file, "SegmentWindow")
        sdk_msgs = extract_class_error_messages(sdk_file, "SegmentWindow")
        assert core_msgs != sdk_msgs

    @pytest.mark.parametrize(
        ("core_msg", "sdk_msg"),
        [
            pytest.param(
                "keys must be non-empty; got empty at {}.",
                "keys must be non-empty strings; got empty at {}.",
                id="non-empty-vs-non-empty-strings",
            ),
            pytest.param(
                "requires at least one key; got an empty iterable.",
                "requires at least one key.",
                id="different-constant-wording",
            ),
        ],
    )
    def test_parametrized_divergence(self, tmp_path: Path, core_msg: str, sdk_msg: str):
        def _make_file(path: Path, msg: str) -> None:
            # Use a plain f-string if the message contains '{}', else a constant.
            if "{}" in msg:
                # Reconstruct as f-string source: replace {} with {i}
                src_msg = msg.replace("{}", "{i}")
                stmt = f'raise ValueError(f"{src_msg}")'
            else:
                stmt = f'raise ValueError("{msg}")'
            path.write_text(
                textwrap.dedent(f"""\
                    class C:
                        def v(self, i):
                            {stmt}
                """)
            )

        core_file = tmp_path / "core.py"
        sdk_file = tmp_path / "sdk.py"
        _make_file(core_file, core_msg)
        _make_file(sdk_file, sdk_msg)

        core_msgs = extract_class_error_messages(core_file, "C")
        sdk_msgs = extract_class_error_messages(sdk_file, "C")
        assert core_msgs != sdk_msgs
