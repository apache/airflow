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
"""Test chunking functionality."""

import pytest

from airflow.providers.hbase.hooks.hbase_strategy import HBaseStrategy


class TestChunking:
    """Test chunking functionality."""

    def test_create_chunks_normal(self):
        """Test normal chunking."""
        rows = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        chunks = HBaseStrategy._create_chunks(rows, 3)
        assert chunks == [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]

    def test_create_chunks_exact_division(self):
        """Test chunking with exact division."""
        rows = [1, 2, 3, 4, 5, 6]
        chunks = HBaseStrategy._create_chunks(rows, 2)
        assert chunks == [[1, 2], [3, 4], [5, 6]]

    def test_create_chunks_empty_list(self):
        """Test chunking empty list."""
        rows = []
        chunks = HBaseStrategy._create_chunks(rows, 3)
        assert chunks == []

    def test_create_chunks_single_item(self):
        """Test chunking single item."""
        rows = [1]
        chunks = HBaseStrategy._create_chunks(rows, 3)
        assert chunks == [[1]]

    def test_create_chunks_chunk_size_larger_than_list(self):
        """Test chunk size larger than list."""
        rows = [1, 2, 3]
        chunks = HBaseStrategy._create_chunks(rows, 10)
        assert chunks == [[1, 2, 3]]

    def test_create_chunks_chunk_size_one(self):
        """Test chunk size of 1."""
        rows = [1, 2, 3]
        chunks = HBaseStrategy._create_chunks(rows, 1)
        assert chunks == [[1], [2], [3]]

    def test_create_chunks_invalid_chunk_size_zero(self):
        """Test invalid chunk size of 0."""
        rows = [1, 2, 3]
        with pytest.raises(ValueError, match="chunk_size must be positive"):
            HBaseStrategy._create_chunks(rows, 0)

    def test_create_chunks_invalid_chunk_size_negative(self):
        """Test invalid negative chunk size."""
        rows = [1, 2, 3]
        with pytest.raises(ValueError, match="chunk_size must be positive"):
            HBaseStrategy._create_chunks(rows, -1)