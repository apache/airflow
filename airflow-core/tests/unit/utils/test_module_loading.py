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

from airflow.utils.module_loading import import_string, qualname


def _sample_function():
    pass


class TestModuleImport:
    def test_import_string(self):
        cls = import_string("airflow.utils.module_loading.import_string")
        assert cls == import_string

        # Test exceptions raised
        with pytest.raises(ImportError):
            import_string("no_dots_in_path")
        msg = 'Module "airflow.utils" does not define a "nonexistent" attribute'
        with pytest.raises(ImportError, match=msg):
            import_string("airflow.utils.nonexistent")


class TestQualname:
    def test_qualname_default_includes_module(self):
        """Test that qualname() by default includes the module path."""
        result = qualname(_sample_function)
        assert result == "unit.utils.test_module_loading._sample_function"

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
        assert result == "unit.utils.test_module_loading._sample_function"
