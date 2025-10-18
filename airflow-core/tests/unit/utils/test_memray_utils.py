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

import sys
from unittest.mock import MagicMock, Mock, patch

from airflow.configuration import AIRFLOW_HOME
from airflow.utils.memray_utils import enable_memray_trace

from tests_common.test_utils.config import conf_vars


class TestEnableMemrayTrackDecorator:
    """Test suite for enable_memray_trace decorator functionality."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.mock_function = Mock(return_value="test_result")
        self.mock_function.__name__ = "mock_function"

        # Set up memray module mock
        self.mock_memray_module = MagicMock()
        self.mock_tracker = MagicMock()
        self.mock_memray_module.Tracker.return_value = self.mock_tracker

        # Configure tracker as context manager
        self.mock_tracker.__enter__ = Mock(return_value=self.mock_tracker)
        self.mock_tracker.__exit__ = Mock(return_value=None)

        # Start patching memray module
        self.memray_patcher = patch.dict("sys.modules", {"memray": self.mock_memray_module})
        self.memray_patcher.start()

    def teardown_method(self):
        """Clean up after each test method."""
        self.memray_patcher.stop()

    def test_memray_not_used_when_tracking_disabled(self):
        """
        Verify that memray is not imported or used when enable_memray_trace is False.

        This test ensures:
        - memray module is not imported
        - memray.Tracker is never instantiated
        - Context manager methods (__enter__/__exit__) are not called
        - Original function executes normally with correct arguments
        - Return value is preserved
        """
        # Track import attempts
        import builtins

        original_import = builtins.__import__
        import_attempts = []

        def track_imports(name, *args, **kwargs):
            import_attempts.append(name)
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=track_imports):
            decorated_function = enable_memray_trace("scheduler")(self.mock_function)
            result = decorated_function("arg1", kwarg="value")

        # Verify memray was never imported
        assert "memray" not in import_attempts, "memray should not be imported when tracking is disabled"

        self.mock_memray_module.Tracker.assert_not_called()
        self.mock_tracker.__enter__.assert_not_called()
        self.mock_tracker.__exit__.assert_not_called()

        self.mock_function.assert_called_once_with("arg1", kwarg="value")
        assert result == "test_result"

    @conf_vars({("scheduler", "enable_memray_trace"): "true"})
    def test_memray_tracker_activated_when_enabled(self):
        """
        Verify that memray.Tracker is properly used when tracking is enabled.

        This test validates:
        1. Tracker instantiation with correct file path
        2. Context manager __enter__ method invocation
        3. Original function execution within tracker context
        4. Context manager __exit__ method invocation
        5. Return value preservation through decoration
        """
        decorated_function = enable_memray_trace("scheduler")(self.mock_function)
        result = decorated_function("arg1", "arg2", kwarg1="value1")

        expected_profile_path = f"{AIRFLOW_HOME}/scheduler_memory.bin"
        self.mock_memray_module.Tracker.assert_called_once_with(expected_profile_path)
        self.mock_tracker.__enter__.assert_called_once()
        self.mock_function.assert_called_once_with("arg1", "arg2", kwarg1="value1")
        self.mock_tracker.__exit__.assert_called_once()
        assert result == "test_result"

    @conf_vars({("api", "enable_memray_trace"): "true"})
    def test_function_metadata_preserved_after_decoration(self):
        """
        Verify that decorator preserves original function metadata.

        Validates preservation of:
        - Function name (__name__)
        - Docstring (__doc__)
        - Type annotations (__annotations__)
        """

        def sample_function(a: int, b: str = "default") -> str:
            """Sample function with metadata."""
            return f"{a}-{b}"

        decorated_function = enable_memray_trace("api")(sample_function)

        assert decorated_function.__name__ == "sample_function"
        assert decorated_function.__doc__ == "Sample function with metadata."
        if hasattr(sample_function, "__annotations__"):
            assert decorated_function.__annotations__ == sample_function.__annotations__


class TestEnableMemrayTrackErrorHandling:
    """Test suite for error handling in enable_memray_trace decorator."""

    def setup_method(self):
        """Set up test fixtures for error handling tests."""
        self.mock_function = Mock(return_value="test_result")
        self.mock_function.__name__ = "mock_function"

    @conf_vars({("dag_processor", "enable_memray_trace"): "true"})
    def test_graceful_fallback_on_memray_import_error(self):
        """
        Verify graceful degradation when memray module is unavailable.

        Ensures that when memray cannot be imported:
        - Original function still executes normally
        - Warning log is generated
        - Return value is preserved
        """
        with patch.dict("sys.modules"):
            if "memray" in sys.modules:
                del sys.modules["memray"]

            with patch("airflow.utils.memray_utils.log.warning") as mock_log_warn:
                decorated_function = enable_memray_trace("dag_processor")(self.mock_function)
                result = decorated_function("arg1")

                self.mock_function.assert_called_once_with("arg1")
                assert result == "test_result"
                mock_log_warn.assert_called_once()
