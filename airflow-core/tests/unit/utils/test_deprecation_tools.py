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

from airflow.utils.deprecation_tools import add_deprecated_classes, getattr_with_deprecation


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
                assert issubclass(w[0].category, DeprecationWarning)
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
                assert issubclass(w[0].category, DeprecationWarning)
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
                assert issubclass(w[0].category, DeprecationWarning)
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
                assert issubclass(w[0].category, DeprecationWarning)
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

    def test_add_deprecated_classes_basic(self):
        """Test basic functionality of add_deprecated_classes."""
        # Use unique package and module names to avoid conflicts
        package_name = get_unique_module_name("test_package")
        module_name = f"{package_name}.old_module"

        module_imports = {"old_module": {"OldClass": "new.module.NewClass"}}

        with temporary_module(module_name):
            add_deprecated_classes(module_imports, package_name)

            # Check that the module was added to sys.modules
            assert module_name in sys.modules
            assert isinstance(sys.modules[module_name], ModuleType)
            assert hasattr(sys.modules[module_name], "__getattr__")

    def test_add_deprecated_classes_with_wildcard(self):
        """Test add_deprecated_classes with wildcard pattern."""
        # Use unique package and module names to avoid conflicts
        package_name = get_unique_module_name("test_package")
        module_name = f"{package_name}.timezone"

        module_imports = {"timezone": {"*": "airflow.sdk.timezone"}}

        with temporary_module(module_name):
            add_deprecated_classes(module_imports, package_name)

            # Check that the module was added to sys.modules
            assert module_name in sys.modules
            assert isinstance(sys.modules[module_name], ModuleType)
            assert hasattr(sys.modules[module_name], "__getattr__")

    def test_add_deprecated_classes_with_override(self):
        """Test add_deprecated_classes with override_deprecated_classes."""
        # Use unique package and module names to avoid conflicts
        package_name = get_unique_module_name("test_package")
        module_name = f"{package_name}.old_module"

        module_imports = {"old_module": {"OldClass": "new.module.NewClass"}}

        override_deprecated_classes = {"old_module": {"OldClass": "override.module.OverrideClass"}}

        with temporary_module(module_name):
            add_deprecated_classes(module_imports, package_name, override_deprecated_classes)

            # Check that the module was added to sys.modules
            assert module_name in sys.modules
            assert isinstance(sys.modules[module_name], ModuleType)

    def test_add_deprecated_classes_doesnt_override_existing(self):
        """Test that add_deprecated_classes doesn't override existing modules."""
        # Use unique package and module names to avoid conflicts
        package_name = get_unique_module_name("test_package")
        module_name = f"{package_name}.existing_module"

        module_imports = {"existing_module": {"SomeClass": "new.module.SomeClass"}}

        with temporary_module(module_name):
            # Create a mock existing module
            existing_module = ModuleType(module_name)
            existing_module.existing_attribute = "existing_value"
            sys.modules[module_name] = existing_module

            add_deprecated_classes(module_imports, package_name)

            # Check that the existing module was not overridden
            assert sys.modules[module_name] is existing_module
            assert sys.modules[module_name].existing_attribute == "existing_value"
