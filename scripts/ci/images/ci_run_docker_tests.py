#!/usr/bin/env python3
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

import argparse
import shlex
import subprocess
import sys
from pathlib import Path
from typing import List

AIRFLOW_SOURCE = Path(__file__).resolve().parent.parent.parent
BUILD_CACHE_DIR = AIRFLOW_SOURCE / ".build"

CBLUE = '\033[94m'
CEND = '\033[0m'


def get_parser():
    parser = argparse.ArgumentParser(
        prog="ci_run_docker_tests",
        description="Running Docker tests using pytest",
        epilog="Unknown arguments are passed unchanged to Pytest.",
    )
    parser.add_argument(
        "--interactive",
        "-i",
        action='store_true',
        help="Activates virtual environment ready to run tests and drops you in",
    )
    parser.add_argument("--initialize", action="store_true", help="Initialize virtual environment and exit")
    parser.add_argument("pytestopts", nargs=argparse.REMAINDER, help="Tests to run")
    return parser


def run_verbose(cmd: List[str], *, check=True, **kwargs):
    print(f"{CBLUE}$ {' '.join(shlex.quote(c) for c in cmd)}{CEND}")
    subprocess.run(cmd, check=check, **kwargs)


def create_virtualenv():
    virtualenv_path = (
        BUILD_CACHE_DIR / ".docker_venv" / f"host_python_{sys.version_info[0]}.{sys.version_info[1]}"
    )
    virtualenv_path.parent.mkdir(parents=True, exist_ok=True)
    if not virtualenv_path.exists():
        print("Creating virtualenv environment")
        run_verbose([sys.executable, "-m", "venv", str(virtualenv_path)])

    python_bin = virtualenv_path / "bin" / "python"
    run_verbose([str(python_bin), "-m", "pip", "install", "pytest", "pytest-xdist", "requests"])
    return python_bin


def main():
    parser = get_parser()
    args = parser.parse_args()

    python_bin = create_virtualenv()

    if args.initialize:
        return
    if args.interactive:
        activate_bin = python_bin.parent / "activate"
        bash_trampoline = f"source {shlex.quote(str(activate_bin))}"
        print("To enter virtual environment, run:")
        print(f"    {bash_trampoline}")
        return

    extra_pytest_args = (
        args.pytestopts[1:] if args.pytestopts and args.pytestopts[0] == "--" else args.pytestopts
    )
    if not extra_pytest_args:
        raise SystemExit("You must select the tests to run.")

    pytest_args = ("-n", "auto", "--color=yes")

    run_verbose([str(python_bin), "-m", "pytest", *pytest_args, *extra_pytest_args])


if __name__ == "__main__":
    main()
