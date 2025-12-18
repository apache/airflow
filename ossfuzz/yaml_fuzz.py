#!/usr/bin/python3
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

import os
import sys

import atheris

os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow")

with atheris.instrument_imports(include=["airflow"], enable_loader_override=False):
    import airflow.utils.yaml as airflow_yaml

    try:
        from yaml.error import YAMLError
    except Exception:  # pragma: no cover - fallback for missing dependency
        YAMLError = Exception  # type: ignore[misc,assignment]


def TestInput(input_bytes: bytes):
    if len(input_bytes) > 4096:
        return

    try:
        loaded = airflow_yaml.safe_load(input_bytes)
    except (YAMLError, UnicodeDecodeError, ValueError, TypeError):
        return

    # Round-trip through dump/load to exercise dumper codepaths too.
    try:
        dumped = airflow_yaml.dump(loaded)
    except (YAMLError, ValueError, TypeError):
        return

    try:
        _ = airflow_yaml.safe_load(dumped)
    except (YAMLError, UnicodeDecodeError, ValueError, TypeError):
        return


def main():
    atheris.Setup(sys.argv, TestInput, enable_python_coverage=True)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

