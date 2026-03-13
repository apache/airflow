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

import pytest

from airflow.providers.common.compat._compat_utils import create_module_getattr


class TestCreateModuleGetattr:
    """Unit tests for the create_module_getattr utility function."""

    @pytest.mark.parametrize(
        ("name", "import_map", "is_module"),
        [
            ("BaseHook", {"BaseHook": "airflow.hooks.base"}, False),
            ("timezone", {}, True),  # Will be tested with module_map
            ("utcnow", {"utcnow": "airflow.utils.timezone"}, False),
        ],
    )
    def test_single_path_import(self, name, import_map, is_module):
        """Test basic single-path imports work correctly."""
        if name == "timezone":
            getattr_fn = create_module_getattr(import_map={}, module_map={name: "airflow.utils.timezone"})
        else:
            getattr_fn = create_module_getattr(import_map=import_map)

        result = getattr_fn(name)
        if is_module:
            # Check if it's a module
            import types

            assert isinstance(result, types.ModuleType)
        else:
            # Check if it's a class or callable
            assert isinstance(result, type) or callable(result)

    @pytest.mark.parametrize(
        ("name", "paths", "should_succeed"),
        [
            ("BaseHook", ("airflow.sdk", "airflow.hooks.base"), True),
            ("NonExistent", ("fake.module1", "fake.module2"), False),
            ("timezone", ("airflow.sdk.timezone", "airflow.utils.timezone"), True),
        ],
    )
    def test_fallback_import_mechanism(self, name, paths, should_succeed):
        """Test that fallback paths are tried in order."""
        if name == "timezone":
            getattr_fn = create_module_getattr(import_map={}, module_map={name: paths})
        else:
            getattr_fn = create_module_getattr(import_map={name: paths})

        if should_succeed:
            result = getattr_fn(name)
            assert result is not None
        else:
            with pytest.raises(ImportError, match=f"Could not import {name!r}"):
                getattr_fn(name)

    def test_rename_map_tries_new_then_old(self):
        """Test that renamed classes try new name first, then fall back to old."""
        rename_map = {
            "Asset": ("airflow.sdk", "airflow.datasets", "Dataset"),
        }
        getattr_fn = create_module_getattr(import_map={}, rename_map=rename_map)

        # Should successfully import (either Asset from airflow.sdk or Dataset from airflow.datasets)
        result = getattr_fn("Asset")
        assert result is not None
        # In Airflow 3, it's Asset; in Airflow 2, it would be Dataset
        assert result.__name__ in ("Asset", "Dataset")

    def test_module_map_imports_whole_module(self):
        """Test that module_map imports entire modules, not just attributes."""
        module_map = {"timezone": "airflow.utils.timezone"}
        getattr_fn = create_module_getattr(import_map={}, module_map=module_map)

        result = getattr_fn("timezone")
        assert hasattr(result, "utc")  # Module should have attributes
        assert hasattr(result, "utcnow")

    def test_exception_chaining_preserves_context(self):
        """Test that exception chaining with 'from' preserves original error context."""
        import_map = {"NonExistent": ("fake.module1", "fake.module2")}
        getattr_fn = create_module_getattr(import_map=import_map)

        with pytest.raises(ImportError) as exc_info:
            getattr_fn("NonExistent")

        # Verify exception has __cause__ (exception chaining)
        assert exc_info.value.__cause__ is not None

    @pytest.mark.parametrize(
        ("error_scenario", "map_config", "expected_match"),
        [
            (
                "import_error",
                {"import_map": {"Fake": ("nonexistent.mod1", "nonexistent.mod2")}},
                "Could not import 'Fake' from any of:",
            ),
            (
                "module_error",
                {"module_map": {"fake_mod": ("nonexistent.module1", "nonexistent.module2")}},
                "Could not import module 'fake_mod' from any of:",
            ),
            (
                "rename_error",
                {"rename_map": {"NewName": ("fake.new", "fake.old", "OldName")}},
                "Could not import 'NewName' from 'fake.new' or 'OldName' from 'fake.old'",
            ),
        ],
    )
    def test_error_messages_include_all_paths(self, error_scenario, map_config, expected_match):
        """Test that error messages include all attempted paths for debugging."""
        getattr_fn = create_module_getattr(
            import_map=map_config.get("import_map", {}),
            module_map=map_config.get("module_map"),
            rename_map=map_config.get("rename_map"),
        )

        keys = (
            map_config.get("import_map", {}).keys()
            or map_config.get("module_map", {}).keys()
            or map_config.get("rename_map", {}).keys()
        )
        name = next(iter(keys))

        with pytest.raises(ImportError, match=expected_match):
            getattr_fn(name)

    def test_attribute_error_for_unknown_name(self):
        """Test that accessing unknown attributes raises AttributeError with correct message."""
        getattr_fn = create_module_getattr(import_map={"BaseHook": "airflow.hooks.base"})

        with pytest.raises(AttributeError, match="module has no attribute 'UnknownClass'"):
            getattr_fn("UnknownClass")

    def test_optional_params_default_to_empty(self):
        """Test that module_map and rename_map default to empty dicts when not provided."""
        getattr_fn = create_module_getattr(import_map={"BaseHook": "airflow.hooks.base"})

        # Should work fine without module_map and rename_map
        result = getattr_fn("BaseHook")
        assert result is not None

        # Should raise AttributeError for names not in any map
        with pytest.raises(AttributeError):
            getattr_fn("NonExistent")

    def test_priority_order_rename_then_module_then_import(self):
        """Test that rename_map has priority over module_map, which has priority over import_map."""
        # If a name exists in multiple maps, rename_map should be checked first
        import_map = {"test": "airflow.hooks.base"}
        module_map = {"test": "airflow.utils.timezone"}
        rename_map = {"test": ("airflow.sdk", "airflow.datasets", "Dataset")}

        getattr_fn = create_module_getattr(
            import_map=import_map,
            module_map=module_map,
            rename_map=rename_map,
        )

        # Should use rename_map (which tries to import Asset/Dataset)
        result = getattr_fn("test")
        # Verify it came from rename_map (Asset or Dataset class, depending on Airflow version)
        assert hasattr(result, "__name__")
        assert result.__name__ in ("Asset", "Dataset")

    def test_module_not_found_error_is_caught(self):
        """Test that ModuleNotFoundError (Python 3.6+) is properly caught."""
        import_map = {"Fake": "completely.nonexistent.module.that.does.not.exist"}
        getattr_fn = create_module_getattr(import_map=import_map)

        # Should catch ModuleNotFoundError and raise ImportError
        with pytest.raises(ImportError, match="Could not import 'Fake'"):
            getattr_fn("Fake")

    @pytest.mark.parametrize(
        ("map_type", "config"),
        [
            ("import_map", {"BaseHook": "airflow.hooks.base"}),
            ("module_map", {"timezone": "airflow.utils.timezone"}),
            ("rename_map", {"Asset": ("airflow.sdk", "airflow.datasets", "Dataset")}),
        ],
    )
    def test_each_map_type_works_independently(self, map_type, config):
        """Test that each map type (import, module, rename) works correctly on its own."""
        kwargs = {"import_map": {}}
        if map_type == "import_map":
            kwargs["import_map"] = config
        elif map_type == "module_map":
            kwargs["module_map"] = config
        elif map_type == "rename_map":
            kwargs["rename_map"] = config

        getattr_fn = create_module_getattr(**kwargs)
        name = next(iter(config.keys()))

        result = getattr_fn(name)
        assert result is not None
