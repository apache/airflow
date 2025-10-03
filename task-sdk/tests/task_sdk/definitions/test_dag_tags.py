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

import pytest
from datetime import datetime

from airflow import DAG


class TestDAGTagValidation:
    """Test DAG tag length validation."""

    def test_tag_exactly_100_chars_allowed(self):
        """Test that a tag with exactly 100 characters is allowed."""
        dag = DAG(
            dag_id="test_dag",
            start_date=datetime(2021, 1, 1),
            tags=["a" * 100]
        )
        assert len(list(dag.tags)[0]) == 100

    def test_tag_101_chars_raises_exception(self):
        """Test that a tag with 101 characters raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DAG(
                dag_id="test_dag",
                start_date=datetime(2021, 1, 1),
                tags=["a" * 101]
            )
        
        error_msg = str(exc_info.value)
        assert "101 characters long" in error_msg
        assert "maximum limit of 100 characters" in error_msg

    def test_multiple_tags_one_too_long_raises_exception(self):
        """Test that multiple tags with one too long raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DAG(
                dag_id="test_dag",
                start_date=datetime(2021, 1, 1),
                tags=["short", "a" * 101, "another_short"]
            )
        
        error_msg = str(exc_info.value)
        assert "101 characters long" in error_msg
        assert "maximum limit of 100 characters" in error_msg

    def test_long_tag_preview_trimmed(self):
        """Test that very long tags are trimmed in error messages."""
        long_tag = "a" * 200
        with pytest.raises(ValueError) as exc_info:
            DAG(
                dag_id="test_dag",
                start_date=datetime(2021, 1, 1),
                tags=[long_tag]
            )
        
        error_msg = str(exc_info.value)
        # Should show first 30 chars + "..."
        assert "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa..." in error_msg
        assert "200 characters long" in error_msg
