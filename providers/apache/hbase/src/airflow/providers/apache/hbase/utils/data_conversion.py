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
"""Common utility functions for HBase provider."""

from __future__ import annotations

import re
from typing import Any


def convert_bytes_to_str(value: bytes | str, encoding: str = "utf-8") -> str:
    """
    Convert bytes to string using specified encoding.

    Args:
        value: Value to convert
        encoding: Encoding to use (default: utf-8)

    Returns:
        String value
    """
    return value.decode(encoding) if isinstance(value, bytes) else value


def convert_scan_results_to_serializable(
    results: list[tuple[str, dict[str, Any]]], encoding: str = "utf-8"
) -> list[dict[str, Any]]:
    """
    Convert scan results to JSON-serializable format.

    Args:
        results: List of (row_key, data) tuples from scan
        encoding: Encoding to use for bytes conversion

    Returns:
        List of dictionaries with string keys and values
    """
    serializable_results = []
    for row_key, data in results:
        row_dict = {"row_key": convert_bytes_to_str(row_key, encoding)}
        for col, val in data.items():
            col_str = convert_bytes_to_str(col, encoding)
            val_str = convert_bytes_to_str(val, encoding)
            row_dict[col_str] = val_str
        serializable_results.append(row_dict)
    return serializable_results


def convert_batch_results_to_serializable(
    results: list[dict[str, Any]], encoding: str = "utf-8"
) -> list[dict[str, Any]]:
    """
    Convert batch get results to JSON-serializable format.

    Args:
        results: List of data dictionaries from batch get
        encoding: Encoding to use for bytes conversion

    Returns:
        List of dictionaries with string keys and values
    """
    serializable_results = []
    for data in results:
        row_dict = {}
        for col, val in data.items():
            col_str = convert_bytes_to_str(col, encoding)
            val_str = convert_bytes_to_str(val, encoding)
            row_dict[col_str] = val_str
        serializable_results.append(row_dict)
    return serializable_results


def extract_backup_id(output: str) -> str | None:
    """
    Extract backup ID from HBase backup command output.

    Args:
        output: Command output string

    Returns:
        Backup ID (e.g., 'backup_1234567890123') or None if not found
    """
    match = re.search(r"backup_(\d+)", output)
    if match:
        return f"backup_{match.group(1)}"
    return None
