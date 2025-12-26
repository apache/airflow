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

"""Tests for HBase security features."""

import pytest

from airflow.providers.hbase.hooks.hbase import HBaseHook
from airflow.providers.hbase.hooks.hbase_strategy import SSHStrategy
from airflow.providers.ssh.hooks.ssh import SSHHook


class TestHBaseSecurityMasking:
    """Test sensitive data masking in HBase hooks."""

    def test_mask_keytab_paths(self):
        """Test masking of keytab file paths."""
        hook = HBaseHook()
        
        command = "kinit -kt /etc/security/keytabs/hbase.keytab hbase@REALM.COM"
        masked = hook._mask_sensitive_command_parts(command)
        
        assert "***KEYTAB_PATH***" in masked
        assert "/etc/security/keytabs/hbase.keytab" not in masked

    def test_mask_passwords(self):
        """Test masking of passwords in commands."""
        hook = HBaseHook()
        
        command = "hbase shell -p password=secret123"
        masked = hook._mask_sensitive_command_parts(command)
        
        assert "password=***MASKED***" in masked
        assert "secret123" not in masked

    def test_mask_tokens(self):
        """Test masking of authentication tokens."""
        hook = HBaseHook()
        
        command = "hbase shell --token=abc123def456"
        masked = hook._mask_sensitive_command_parts(command)
        
        assert "token=***MASKED***" in masked
        assert "abc123def456" not in masked

    def test_mask_java_home(self):
        """Test masking of JAVA_HOME paths."""
        hook = HBaseHook()
        
        command = "export JAVA_HOME=/usr/lib/jvm/java-8-oracle && hbase shell"
        masked = hook._mask_sensitive_command_parts(command)
        
        assert "JAVA_HOME=***MASKED***" in masked
        assert "/usr/lib/jvm/java-8-oracle" not in masked

    def test_mask_output_keytab_paths(self):
        """Test masking keytab paths in command output."""
        hook = HBaseHook()
        
        output = "Error: Could not find keytab file /home/user/.keytab"
        masked = hook._mask_sensitive_data_in_output(output)
        
        assert "***KEYTAB_PATH***" in masked
        assert "/home/user/.keytab" not in masked

    def test_mask_output_passwords(self):
        """Test masking passwords in command output."""
        hook = HBaseHook()
        
        output = "Authentication failed for password: mysecret"
        masked = hook._mask_sensitive_data_in_output(output)
        
        assert "password=***MASKED***" in masked
        assert "mysecret" not in masked

    def test_ssh_strategy_mask_keytab_paths(self):
        """Test SSH strategy masking of keytab paths."""
        # Create strategy without SSH hook initialization
        strategy = SSHStrategy("test_conn", None, None)
        
        command = "kinit -kt /opt/keytabs/service.keytab service@DOMAIN"
        masked = strategy._mask_sensitive_command_parts(command)
        
        assert "***KEYTAB_PATH***" in masked
        assert "/opt/keytabs/service.keytab" not in masked

    def test_ssh_strategy_mask_passwords(self):
        """Test SSH strategy masking of passwords."""
        # Create strategy without SSH hook initialization
        strategy = SSHStrategy("test_conn", None, None)
        
        command = "authenticate --password=topsecret"
        masked = strategy._mask_sensitive_command_parts(command)
        
        assert "password=***MASKED***" in masked
        assert "topsecret" not in masked

    def test_multiple_sensitive_items(self):
        """Test masking multiple sensitive items in one command."""
        hook = HBaseHook()
        
        command = "export JAVA_HOME=/usr/java && kinit -kt /etc/hbase.keytab user@REALM --password=secret"
        masked = hook._mask_sensitive_command_parts(command)
        
        assert "JAVA_HOME=***MASKED***" in masked
        assert "***KEYTAB_PATH***" in masked
        assert "password=***MASKED***" in masked
        assert "/usr/java" not in masked
        assert "/etc/hbase.keytab" not in masked
        assert "secret" not in masked

    def test_no_sensitive_data(self):
        """Test that normal commands are not modified."""
        hook = HBaseHook()
        
        command = "hbase shell list"
        masked = hook._mask_sensitive_command_parts(command)
        
        assert masked == command

    def test_case_insensitive_password_masking(self):
        """Test case-insensitive password masking."""
        hook = HBaseHook()
        
        command = "auth --PASSWORD=secret123"
        masked = hook._mask_sensitive_command_parts(command)
        
        assert "***MASKED***" in masked
        assert "secret123" not in masked