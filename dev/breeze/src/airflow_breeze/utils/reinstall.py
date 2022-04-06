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

import subprocess
import sys
from pathlib import Path

from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import console


def reinstall_breeze(breeze_sources: Path):
    subprocess.check_call(["pipx", "install", "-e", breeze_sources, "--force"])


def ask_to_reinstall(breeze_sources: Path):
    """
    Ask the user to reinstall Breeze (and do so if confirmed).
    :param breeze_sources: breeze sources to reinstall Breeze from.
    """
    console.print(
        f"\n[bright_yellow]WARNING! Breeze dependencies changed since the installation![/]\n\n"
        f"[bright_yellow]This might cause various problems!![/]\n\n"
        f"If you experience problems - reinstall Breeze with:\n\n"
        f"    pipx install -e '{breeze_sources}' --force\n\n"
    )
    answer = user_confirm("Do you want to reinstall Breeze now?", timeout=3, default_answer=Answer.NO)
    if answer == Answer.YES:
        reinstall_breeze(breeze_sources)
    elif answer == Answer.QUIT:
        sys.exit(1)
