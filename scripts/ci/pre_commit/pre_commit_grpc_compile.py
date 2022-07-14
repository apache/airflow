#!/usr/bin/env python
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
import shutil
import subprocess
import sys

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )

GRPC_TOOLS_PROTOC = "grpc_tools.protoc"
PROTOC_GEN_MYPY_NAME = "protoc-gen-mypy"

if __name__ == '__main__':
    protoc_gen_mypy_path = shutil.which(PROTOC_GEN_MYPY_NAME)
    python_path = sys.executable
    command = [
        python_path,
        "-m",
        GRPC_TOOLS_PROTOC,
        f"--plugin={PROTOC_GEN_MYPY_NAME}={protoc_gen_mypy_path}",
        "--python_out",
        ".",
        "--grpc_python_out",
        ".",
        "--mypy_out",
        ".",
        "--proto_path",
        ".",
        *sys.argv[1:],
    ]
    subprocess.check_call(command)
