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

import subprocess


def run_command(commands: list):
    result = subprocess.run(commands, check=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running command: {result.stderr}")
        exit(1)
    return result.stdout


# HEAD^1 says the "first" parent. For PR merge commits, or main commits, this is the "right" commit.
#
# In this example, 9c532b6 is the PR commit (HEAD^2), 4840892 is the head GitHub checks-out for us,
# and db121f7 is the "merge target" (HEAD^1) -- i.e. mainline

def client_codegen_diff():
    previous_mainline_commit = run_command(["git", "rev-parse", "--short", "HEAD^1"]).strip()
    print(f"Diffing openapi spec against {previous_mainline_commit}...")

    SPEC_FILE = "airflow/api_connexion/openapi/v1.yaml"

    GO_CLIENT_PATH = "clients/go/airflow"

    GO_TARGET_CLIENT_PATH = "clients/go_target_branch/airflow"

    # generate client for current patch
    run_command(["mkdir", "-p", f"{GO_CLIENT_PATH}"])

    run_command(["./clients/gen/go.sh", f"{SPEC_FILE}", f"{GO_CLIENT_PATH}"])
    # generate client for target patch
    run_command(["mkdir", "-p", f"{GO_TARGET_CLIENT_PATH}"])

    run_command(["git", "checkout", f"{previous_mainline_commit}", "--", f"{SPEC_FILE}"])
    run_command(["./clients/gen/go.sh", f"{SPEC_FILE}", f"{GO_TARGET_CLIENT_PATH}"])

    run_command(["diff", "-u", f"{GO_TARGET_CLIENT_PATH}", f"{GO_CLIENT_PATH}"])


if __name__ == "__main__":
    client_codegen_diff()
