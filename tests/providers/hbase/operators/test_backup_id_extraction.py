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

"""Tests for backup_id extraction from HBase backup command output."""

from __future__ import annotations

import re

import pytest


def extract_backup_id(output: str) -> str:
    """Extract backup_id from HBase backup command output."""
    match = re.search(r'Backup (backup_\d+) started', output)
    if match:
        return match.group(1)
    raise ValueError("No backup_id found in output")


class TestBackupIdExtraction:
    """Test cases for backup_id extraction."""
    
    def test_extract_backup_id_success(self):
        """Test successful backup_id extraction."""
        output = "2025-12-19T17:50:33,416 INFO [main {}] impl.TableBackupClient: Backup backup_1766148633020 started at 1766148633416."
        expected = "backup_1766148633020"
        assert extract_backup_id(output) == expected
    
    def test_extract_backup_id_with_log_prefix(self):
        """Test extraction with Airflow log prefix."""
        output = "[2025-12-19T12:50:33.417+0000] {ssh.py:545} WARNING - 2025-12-19T17:50:33,416 INFO [main {}] impl.TableBackupClient: Backup backup_1766148633020 started at 1766148633416."
        expected = "backup_1766148633020"
        assert extract_backup_id(output) == expected
    
    def test_extract_backup_id_different_timestamp(self):
        """Test extraction with different timestamp."""
        output = "Backup backup_1234567890123 started at 1234567890123."
        expected = "backup_1234567890123"
        assert extract_backup_id(output) == expected
    
    def test_extract_backup_id_no_match(self):
        """Test extraction when no backup_id is found."""
        output = "Some random log output without backup info"
        with pytest.raises(ValueError, match="No backup_id found in output"):
            extract_backup_id(output)
    
    def test_extract_backup_id_empty_string(self):
        """Test extraction with empty string."""
        output = ""
        with pytest.raises(ValueError, match="No backup_id found in output"):
            extract_backup_id(output)