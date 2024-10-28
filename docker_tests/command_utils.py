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

import shlex
import subprocess


def run_command(
    cmd: list[str],
    *,
    print_output_on_error: bool = True,
    return_output: bool = False,
    check=True,
    **kwargs,
) -> str | bool:
    print(f"Running command: {' '.join(shlex.quote(c) for c in cmd)}")
    try:
        if return_output:
            return subprocess.check_output(cmd, **kwargs).decode()
        else:
            try:
                result = subprocess.run(cmd, check=check, **kwargs)
                return result.returncode == 0
            except FileNotFoundError:
                if check:
                    raise
                else:
                    return False
    except subprocess.CalledProcessError as ex:
        if print_output_on_error:
            print("========================= OUTPUT start ============================")
            print(ex.stderr)
            print(ex.stdout)
            print("========================= OUTPUT end ============================")
        raise
