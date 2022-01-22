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

import hashlib
import shlex
import subprocess
from typing import Dict, List

from airflow_breeze.console import console


def run_command(cmd: List[str], *, check: bool = True, verbose: bool = False, **kwargs):
    if verbose:
        console.print(f"[blue]$ {' '.join(shlex.quote(c) for c in cmd)}")
    try:
        return subprocess.run(cmd, check=check, **kwargs)
    except subprocess.CalledProcessError as ex:
        print("========================= OUTPUT start ============================")
        print(ex.stderr)
        print(ex.stdout)
        print("========================= OUTPUT end ============================")
        raise


def generate_md5(filename, file_size: int = 65536):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for file_chunk in iter(lambda: f.read(file_size), b""):
            hash_md5.update(file_chunk)
    return hash_md5.hexdigest()


def filter_out_none(**kwargs) -> Dict:
    for key in list(kwargs):
        if kwargs[key] is None:
            kwargs.pop(key)
    return kwargs
