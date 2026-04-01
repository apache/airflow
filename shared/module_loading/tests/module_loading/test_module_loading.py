#
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

import functools

import pytest

from airflow_shared.module_loading import import_string, is_valid_dotpath, qualname


def _import_string():
    pass


def _sample_function():
    pass


class TestModuleImport:
    def test_import_string(self):
        cls = import_string("module_loading.test_module_loading._import_string")
        assert cls == _import_string

        # Test exceptions raised
        with pytest.raises(ImportError):
            import_string("no_dots_in_path")
        msg = 'Module "module_loading.test_module_loading" does not define a "nonexistent" attribute'
        with pytest.raises(ImportError, match=msg):
            import_string("module_loading.test_module_loading.nonexistent")


class TestModuleLoading:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            pytest.param("valid_path", True, id="module_no_dots"),
            pytest.param("valid.dot.path", True, id="standard_dotpath"),
            pytest.param("package.sub_package.module", True, id="dotpath_with_underscores"),
            pytest.param("MyPackage.MyClass", True, id="mixed_case_path"),
            pytest.param("invalid..path", False, id="consecutive_dots_fails"),
            pytest.param(".invalid.path", False, id="leading_dot_fails"),
            pytest.param("invalid.path.", False, id="trailing_dot_fails"),
            pytest.param("1invalid.path", False, id="leading_number_fails"),
            pytest.param(42, False, id="not_a_string"),
        ],
    )
    def test_is_valid_dotpath(self, path, expected):
        assert is_valid_dotpath(path) == expected


class TestQualname:
    def test_qualname_default_includes_module(self):
        """Test that qualname() by default includes the module path."""
        result = qualname(_sample_function)
        assert result == "module_loading.test_module_loading._sample_function"

    def test_qualname_exclude_module_simple_function(self):
        """Test that exclude_module=True returns only the function name."""
        result = qualname(_sample_function, exclude_module=True)
        assert result == "_sample_function"

    def test_qualname_exclude_module_nested_function(self):
        """Test that exclude_module=True works with nested functions."""

        def outer():
            def inner():
                pass

            return inner

        inner_func = outer()
        result = qualname(inner_func, exclude_module=True)
        assert (
            result
            == "TestQualname.test_qualname_exclude_module_nested_function.<locals>.outer.<locals>.inner"
        )

    def test_qualname_exclude_module_functools_partial(self):
        """Test that exclude_module=True handles functools.partial correctly."""

        def base_func(x, y):
            pass

        partial_func = functools.partial(base_func, x=1)
        result = qualname(partial_func, exclude_module=True)
        assert result == "TestQualname.test_qualname_exclude_module_functools_partial.<locals>.base_func"

    def test_qualname_exclude_module_class(self):
        """Test that exclude_module=True works with classes."""

        class MyClass:
            pass

        result = qualname(MyClass, exclude_module=True)
        assert result == "TestQualname.test_qualname_exclude_module_class.<locals>.MyClass"

    def test_qualname_exclude_module_instance(self):
        """Test that exclude_module=True works with class instances."""

        class MyClass:
            pass

        instance = MyClass()
        result = qualname(instance, exclude_module=True)
        assert result == "TestQualname.test_qualname_exclude_module_instance.<locals>.MyClass"

    def test_qualname_use_qualname_still_includes_module(self):
        """Test that use_qualname=True still includes module prefix."""
        result = qualname(_sample_function, use_qualname=True)
        assert result == "module_loading.test_module_loading._sample_function"
