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
"""Test HBase utility functions."""

from __future__ import annotations

from airflow.providers.apache.hbase.utils.data_conversion import (
    convert_batch_results_to_serializable,
    convert_bytes_to_str,
    convert_scan_results_to_serializable,
    extract_backup_id,
)


class TestDataConversion:
    """Test data conversion utilities."""

    def test_convert_bytes_to_str(self):
        """Test bytes to string conversion."""
        assert convert_bytes_to_str(b"test") == "test"
        assert convert_bytes_to_str("test") == "test"
        assert convert_bytes_to_str(b"caf\xc3\xa9", "utf-8") == "café"

    def test_convert_scan_results_to_serializable(self):
        """Test scan results conversion."""
        results = [(b"row1", {b"cf1:col1": b"value1"}), ("row2", {"cf1:col2": "value2"})]

        converted = convert_scan_results_to_serializable(results)

        assert len(converted) == 2
        assert converted[0]["row_key"] == "row1"
        assert converted[0]["cf1:col1"] == "value1"
        assert converted[1]["row_key"] == "row2"
        assert converted[1]["cf1:col2"] == "value2"

    def test_convert_batch_results_to_serializable(self):
        """Test batch results conversion."""
        results = [{b"cf1:col1": b"value1"}, {"cf1:col2": "value2"}]

        converted = convert_batch_results_to_serializable(results)

        assert len(converted) == 2
        assert converted[0]["cf1:col1"] == "value1"
        assert converted[1]["cf1:col2"] == "value2"

    def test_extract_backup_id(self):
        """Test backup ID extraction."""
        output = "Backup backup_1234567890123 completed."
        assert extract_backup_id(output) == "backup_1234567890123"

        output = "Some other output"
        assert extract_backup_id(output) is None

        output = "Multiple backup_111 and backup_222"
        assert extract_backup_id(output) == "backup_111"
