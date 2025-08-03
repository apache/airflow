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
"""Unit tests for the StreamCaptureManager class used in Airflow CLI tests."""

from __future__ import annotations

import sys


def test_stdout_only(stdout_capture):
    """Test capturing stdout only."""
    with stdout_capture as capture:
        print("Hello stdout")
        print("Error message", file=sys.stderr)

        # Access during context
        assert "Hello stdout" in capture.getvalue()
        assert "Error message" not in capture.getvalue()


def test_stderr_only(stderr_capture):
    """Test capturing stderr only."""
    with stderr_capture as capture:
        print("Hello stdout")
        print("Error message", file=sys.stderr)

        assert "Error message" in capture.getvalue()
        assert "Hello stdout" not in capture.getvalue()


def test_combined(combined_capture):
    """Test capturing both streams."""
    with combined_capture as capture:
        print("Hello stdout")
        print("Error message", file=sys.stderr)

        assert "Hello stdout" in capture.stdout
        assert "Error message" in capture.stderr
        assert "Hello stdout" in capture.get_combined()
        assert "Error message" in capture.get_combined()


def test_configurable(stream_capture):
    """Test with configurable capture."""
    # Capture both
    with stream_capture(stdout=True, stderr=True) as capture:
        print("stdout message")
        print("stderr message", file=sys.stderr)

        assert "stdout message" in capture.stdout
        assert "stderr message" in capture.stderr

    # Capture stderr only
    with stream_capture(stdout=False, stderr=True) as capture:
        print("stdout message")
        print("stderr message", file=sys.stderr)

        assert capture.stdout == ""
        assert "stderr message" in capture.stderr
