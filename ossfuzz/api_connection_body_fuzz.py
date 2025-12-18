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
    from airflow.api_fastapi.core_api.datamodels.connections import ConnectionBody


def TestInput(input_bytes: bytes):
    if len(input_bytes) > 8192:
        return
    try:
        body = ConnectionBody.model_validate_json(input_bytes)
        _ = body.model_dump()
    except Exception:
        return


def main():
    atheris.Setup(sys.argv, TestInput, enable_python_coverage=True)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

