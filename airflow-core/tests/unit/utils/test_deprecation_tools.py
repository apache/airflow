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

import contextlib
import re
import sys
import uuid
import warnings
from contextlib import contextmanager
from types import ModuleType
from unittest import mock

import pytest

from airflow.utils.deprecation_tools import (
    DeprecatedImportWarning,
    add_deprecated_classes,
    getattr_with_deprecation,
)


@contextmanager
def temporary_module(module_name):
    """Context manager to safely add and remove modules from sys.modules."""
    original_module = sys.modules.get(module_name)
    try:
        yield
    finally:
        if original_module is not None:
            sys.modules[module_name] = original_module
        elif module_name in sys.modules:
            del sys.modules[module_name]


def get_unique_module_name(base_name="test_module"):
    """Generate a unique module name to avoid conflicts."""
    return f"{base_name}_{uuid.uuid4().hex[:8]}"


class TestGetAttrWithDeprecation:
    """Tests for the getattr_with_deprecation function."""

    def test_getattr_with_deprecation_specific_class(self):
        """Test deprecated import for a specific class."""
        imports = {"OldClass": "new.module.NewClass"}

        # Mock the new module and class
        mock_module = mock.MagicMock()
        mock_new_class = mock.MagicMock()
        mock_module.NewClass = mock_new_class

        with mock.patch("airflow.utils.deprecation_tools.importlib.import_module", return_value=mock_module):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = getattr_with_deprecation(
                    imports=imports,
                    module="old.module",
                    override_deprecated_classes={},
                    extra_message="",
                    name="OldClass",
                )

                assert result == mock_new_class
                assert len(w) == 1
                assert issubclass(w[0].category, DeprecatedImportWarning)
                assert "old.module.OldClass" in str(w[0].message)
                assert "new.module.NewClass" in str(w[0].message)

    def test_getattr_with_deprecation_wildcard(self):
        """Test deprecated import using wildcard pattern."""
        imports = {"*": "new.module"}

        # Mock the new module and attribute
        mock_module = mock.MagicMock()
        mock_attribute = mock.MagicMock()
        mock_module.SomeAttribute = mock_attribute

        with mock.patch("airflow.utils.deprecation_tools.importlib.import_module", return_value=mock_module):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = getattr_with_deprecation(
                    imports=imports,
                    module="old.module",
                    override_deprecated_classes={},
                    extra_message="",
                    name="SomeAttribute",
                )

                assert result == mock_attribute
                assert len(w) == 1
                assert issubclass(w[0].category, DeprecatedImportWarning)
                assert "old.module.SomeAttribute" in str(w[0].message)
                assert "new.module.SomeAttribute" in str(w[0].message)

    def test_getattr_with_deprecation_wildcard_with_override(self):
        """Test wildcard pattern with override deprecated classes."""
        imports = {"*": "new.module"}
        override_deprecated_classes = {"SomeAttribute": "override.module.OverrideClass"}

        # Mock the new module and attribute
        mock_module = mock.MagicMock()
        mock_attribute = mock.MagicMock()
        mock_module.SomeAttribute = mock_attribute

        with mock.patch("airflow.utils.deprecation_tools.importlib.import_module", return_value=mock_module):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = getattr_with_deprecation(
                    imports=imports,
                    module="old.module",
                    override_deprecated_classes=override_deprecated_classes,
                    extra_message="",
                    name="SomeAttribute",
                )

                assert result == mock_attribute
                assert len(w) == 1
                assert issubclass(w[0].category, DeprecatedImportWarning)
                assert "old.module.SomeAttribute" in str(w[0].message)
                assert "override.module.OverrideClass" in str(w[0].message)

    def test_getattr_with_deprecation_specific_class_priority(self):
        """Test that specific class mapping takes priority over wildcard."""
        imports = {"SpecificClass": "specific.module.SpecificClass", "*": "wildcard.module"}

        # Mock the specific module and class
        mock_module = mock.MagicMock()
        mock_specific_class = mock.MagicMock()
        mock_module.SpecificClass = mock_specific_class

        with mock.patch("airflow.utils.deprecation_tools.importlib.import_module", return_value=mock_module):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = getattr_with_deprecation(
                    imports=imports,
                    module="old.module",
                    override_deprecated_classes={},
                    extra_message="",
                    name="SpecificClass",
                )

                assert result == mock_specific_class
                assert len(w) == 1
                assert issubclass(w[0].category, DeprecatedImportWarning)
                assert "old.module.SpecificClass" in str(w[0].message)
                assert "specific.module.SpecificClass" in str(w[0].message)

    def test_getattr_with_deprecation_attribute_not_found(self):
        """Test AttributeError when attribute not found."""
        imports = {"ExistingClass": "new.module.ExistingClass"}

        with pytest.raises(AttributeError, match=r"has no attribute.*NonExistentClass"):
            getattr_with_deprecation(
                imports=imports,
                module="old.module",
                override_deprecated_classes={},
                extra_message="",
                name="NonExistentClass",
            )

    def test_getattr_with_deprecation_import_error(self):
        """Test ImportError when target module cannot be imported."""
        imports = {"*": "nonexistent.module"}

        with mock.patch(
            "airflow.utils.deprecation_tools.importlib.import_module",
            side_effect=ImportError("Module not found"),
        ):
            with pytest.raises(ImportError, match="Could not import"):
                getattr_with_deprecation(
                    imports=imports,
                    module="old.module",
                    override_deprecated_classes={},
                    extra_message="",
                    name="SomeAttribute",
                )

    def test_getattr_with_deprecation_with_extra_message(self):
        """Test that extra message is included in warning."""
        imports = {"*": "new.module"}
        extra_message = "This is an extra message"

        # Mock the new module and attribute
        mock_module = mock.MagicMock()
        mock_attribute = mock.MagicMock()
        mock_module.SomeAttribute = mock_attribute

        with mock.patch("airflow.utils.deprecation_tools.importlib.import_module", return_value=mock_module):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                getattr_with_deprecation(
                    imports=imports,
                    module="old.module",
                    override_deprecated_classes={},
                    extra_message=extra_message,
                    name="SomeAttribute",
                )

                assert len(w) == 1
                assert extra_message in str(w[0].message)

    @pytest.mark.parametrize("dunder_attribute", ["__path__", "__file__"])
    def test_getattr_with_deprecation_wildcard_skips_dunder_attributes(self, dunder_attribute):
        """Test that wildcard pattern skips Python special attributes."""
        imports = {"*": "new.module"}

        # Special attributes should raise AttributeError, not be redirected
        with pytest.raises(AttributeError, match=rf"has no attribute.*{re.escape(dunder_attribute)}"):
            getattr_with_deprecation(
                imports=imports,
                module="old.module",
                override_deprecated_classes={},
                extra_message="",
                name=dunder_attribute,
            )

    @pytest.mark.parametrize("non_dunder_attr", ["__version", "__author", "_private", "public"])
    def test_getattr_with_deprecation_wildcard_allows_non_dunder_attributes(self, non_dunder_attr):
        """Test that wildcard pattern allows non-dunder attributes (including single underscore prefixed)."""
        imports = {"*": "unittest.mock"}

        # These should be redirected through wildcard pattern
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with contextlib.suppress(ImportError, AttributeError):
                # Expected - the target module might not have the attribute
                # The important thing is that it tried to redirect (didn't raise AttributeError immediately)
                getattr_with_deprecation(
                    imports=imports,
                    module="old.module",
                    override_deprecated_classes={},
                    extra_message="",
                    name=non_dunder_attr,
                )

            # Should have generated a deprecation warning
            assert len(w) == 1
            assert "deprecated" in str(w[0].message).lower()


class TestAddDeprecatedClasses:
    """Tests for the add_deprecated_classes function."""

    @pytest.mark.parametrize(
        ("test_case", "module_imports", "override_classes", "expected_behavior"),
        [
            (
                "basic_class_mapping",
                {"old_module": {"OldClass": "new.module.NewClass"}},
                None,
                "creates_virtual_module",
            ),
            (
                "wildcard_pattern",
                {"timezone": {"*": "airflow.sdk.timezone"}},
                None,
                "creates_virtual_module",
            ),
            (
                "with_override",
                {"old_module": {"OldClass": "new.module.NewClass"}},
                {"old_module": {"OldClass": "override.module.OverrideClass"}},
                "creates_virtual_module",
            ),
        ],
        ids=["basic_class_mapping", "wildcard_pattern", "with_override"],
    )
    def test_virtual_module_creation(self, test_case, module_imports, override_classes, expected_behavior):
        """Test add_deprecated_classes creates virtual modules correctly."""
        # Use unique package and module names to avoid conflicts
        package_name = get_unique_module_name("test_package")
        module_name = f"{package_name}.{next(iter(module_imports.keys()))}"

        with temporary_module(module_name):
            add_deprecated_classes(module_imports, package_name, override_classes)

            # Check that the module was added to sys.modules
            assert module_name in sys.modules
            assert isinstance(sys.modules[module_name], ModuleType)
            assert hasattr(sys.modules[module_name], "__getattr__")

    def test_add_deprecated_classes_doesnt_override_existing(self):
        """Test that add_deprecated_classes doesn't override existing modules."""
        module_name = get_unique_module_name("existing_module")
        full_module_name = f"airflow.test.{module_name}"

        # Create an existing module
        existing_module = ModuleType(full_module_name)
        existing_module.existing_attr = "existing_value"
        sys.modules[full_module_name] = existing_module

        with temporary_module(full_module_name):
            # This should not override the existing module
            add_deprecated_classes(
                {module_name: {"NewClass": "new.module.NewClass"}},
                package="airflow.test",
            )

            # The existing module should still be there
            assert sys.modules[full_module_name] == existing_module
            assert sys.modules[full_module_name].existing_attr == "existing_value"

    @pytest.mark.parametrize(
        (
            "test_case",
            "module_imports",
            "attr_name",
            "target_attr",
            "expected_target_msg",
            "override_classes",
        ),
        [
            (
                "direct_imports",
                {
                    "get_something": "target.module.get_something",
                    "another_attr": "target.module.another_attr",
                },
                "get_something",
                "get_something",
                "target.module.get_something",
                None,
            ),
            (
                "with_wildcard",
                {"specific_attr": "target.module.specific_attr", "*": "target.module"},
                "any_attribute",
                "any_attribute",
                "target.module.any_attribute",
                None,
            ),
            (
                "with_override",
                {"get_something": "target.module.get_something"},
                "get_something",
                "get_something",
                "override.module.OverrideClass",
                {"get_something": "override.module.OverrideClass"},
            ),
        ],
        ids=["direct_imports", "with_wildcard", "with_override"],
    )
    def test_current_module_deprecation(
        self, test_case, module_imports, attr_name, target_attr, expected_target_msg, override_classes
    ):
        """Test add_deprecated_classes with current module (__name__ key) functionality."""
        module_name = get_unique_module_name(f"{test_case}_module")
        full_module_name = f"airflow.test.{module_name}"

        # Create a module to modify
        test_module = ModuleType(full_module_name)
        sys.modules[full_module_name] = test_module

        with temporary_module(full_module_name):
            # Mock the target module and attribute
            mock_target_module = mock.MagicMock()
            mock_attribute = mock.MagicMock()
            setattr(mock_target_module, target_attr, mock_attribute)

            with mock.patch(
                "airflow.utils.deprecation_tools.importlib.import_module", return_value=mock_target_module
            ):
                # Prepare override parameter
                override_param = {full_module_name: override_classes} if override_classes else None

                add_deprecated_classes(
                    {full_module_name: module_imports},
                    package=full_module_name,
                    override_deprecated_classes=override_param,
                )

                # The module should now have a __getattr__ method
                assert hasattr(test_module, "__getattr__")

                # Test that accessing the deprecated attribute works
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    result = getattr(test_module, attr_name)

                    assert result == mock_attribute
                    assert len(w) == 1
                    assert issubclass(w[0].category, DeprecatedImportWarning)
                    assert f"{full_module_name}.{attr_name}" in str(w[0].message)
                    assert expected_target_msg in str(w[0].message)

    def test_add_deprecated_classes_mixed_current_and_virtual_modules(self):
        """Test add_deprecated_classes with mixed current module and virtual module imports."""
        base_module_name = get_unique_module_name("mixed_module")
        full_module_name = f"airflow.test.{base_module_name}"
        virtual_module_name = f"{base_module_name}_virtual"
        full_virtual_module_name = f"{full_module_name}.{virtual_module_name}"

        # Create a module to modify
        test_module = ModuleType(full_module_name)
        sys.modules[full_module_name] = test_module

        with temporary_module(full_module_name), temporary_module(full_virtual_module_name):
            # Mock the target modules and attributes
            mock_current_module = mock.MagicMock()
            mock_current_attr = mock.MagicMock()
            mock_current_module.current_attr = mock_current_attr

            mock_virtual_module = mock.MagicMock()
            mock_virtual_attr = mock.MagicMock()
            mock_virtual_module.VirtualClass = mock_virtual_attr

            def mock_import_module(module_name):
                if "current.module" in module_name:
                    return mock_current_module
                if "virtual.module" in module_name:
                    return mock_virtual_module
                raise ImportError(f"Module {module_name} not found")

            with mock.patch(
                "airflow.utils.deprecation_tools.importlib.import_module", side_effect=mock_import_module
            ):
                add_deprecated_classes(
                    {
                        full_module_name: {
                            "current_attr": "current.module.current_attr",
                        },
                        virtual_module_name: {
                            "VirtualClass": "virtual.module.VirtualClass",
                        },
                    },
                    package=full_module_name,
                )

                # Test current module access
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    result = test_module.current_attr

                    assert result == mock_current_attr
                    assert len(w) == 1
                    assert issubclass(w[0].category, DeprecatedImportWarning)
                    assert f"{full_module_name}.current_attr" in str(w[0].message)

                # Test virtual module access
                virtual_module = sys.modules[full_virtual_module_name]
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    result = virtual_module.VirtualClass

                    assert result == mock_virtual_attr
                    assert len(w) == 1
                    assert issubclass(w[0].category, DeprecatedImportWarning)
                    assert f"{full_virtual_module_name}.VirtualClass" in str(w[0].message)

    def test_add_deprecated_classes_current_module_not_in_sys_modules(self):
        """Test add_deprecated_classes raises error when current module not in sys.modules."""
        nonexistent_module = "nonexistent.module.name"

        with pytest.raises(ValueError, match=f"Module {nonexistent_module} not found in sys.modules"):
            add_deprecated_classes(
                {nonexistent_module: {"attr": "target.module.attr"}},
                package=nonexistent_module,
            )

    def test_add_deprecated_classes_preserves_existing_module_attributes(self):
        """Test that add_deprecated_classes preserves existing module attributes."""
        module_name = get_unique_module_name("preserve_module")
        full_module_name = f"airflow.test.{module_name}"

        # Create a module with existing attributes
        test_module = ModuleType(full_module_name)
        test_module.existing_attr = "existing_value"
        test_module.existing_function = lambda: "existing_function_result"
        sys.modules[full_module_name] = test_module

        with temporary_module(full_module_name):
            add_deprecated_classes(
                {
                    full_module_name: {
                        "deprecated_attr": "target.module.deprecated_attr",
                    }
                },
                package=full_module_name,
            )

            # Existing attributes should still be accessible
            assert test_module.existing_attr == "existing_value"
            assert test_module.existing_function() == "existing_function_result"

            # The module should have __getattr__ for deprecated attributes
            assert hasattr(test_module, "__getattr__")
