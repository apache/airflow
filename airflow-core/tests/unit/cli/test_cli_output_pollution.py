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

import json
import logging
import sys
from unittest import mock

import pytest

from airflow.__main__ import main

pytestmark = pytest.mark.db_test

class TestCliOutputPollution:
    def test_cli_list_dags_json_output_no_pollution(self, stdout_capture, stderr_capture):
        """
        Test that 'airflow dags list -o json' output is not polluted by logs.
        """
        root_logger = logging.getLogger()
        original_streams = {h: h.stream for h in root_logger.handlers if isinstance(h, logging.StreamHandler)}
        
        try:
            with mock.patch("sys.argv", ["airflow", "dags", "list", "-o", "json"]):
                with stdout_capture as temp_stdout, stderr_capture as temp_stderr:
                    logging.info("Pollution message")
                    
                    main()
                    
                    stdout_val = temp_stdout.getvalue().strip()
                    stderr_val = temp_stderr.getvalue().strip()
                    
            # Verify stdout is valid JSON
            try:
                data = json.loads(stdout_val)
                assert isinstance(data, list)
            except json.JSONDecodeError as e:
                pytest.fail(f"CLI output is not valid JSON: {stdout_val}\nError: {e}")
                
            # Verify that the pollution message is NOT in stdout but IS in stderr
            assert "Pollution message" not in stdout_val
            assert "Pollution message" in stderr_val
        finally:
            # Restore original streams
            for handler, stream in original_streams.items():
                handler.setStream(stream)

    def test_cli_list_dags_table_output_still_on_stdout(self, stdout_capture, stderr_capture):
        """
        Test that 'airflow dags list' (default table) still prints to stdout.
        """
        with mock.patch("sys.argv", ["airflow", "dags", "list"]):
            with stdout_capture as temp_stdout, stderr_capture as temp_stderr:
                logging.info("Regular log message")
                
                main()
                
                stdout_val = temp_stdout.getvalue().strip()
                
        # In table mode, logs might still be on stdout depending on config, 
        # but our fix ONLY redirects if output != 'table'.
        # So we just check that we didn't break normal output.
        assert "dag_id" in stdout_val
