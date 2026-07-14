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
from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH

# NOTE!. This script is executed from a node environment created by prek hook, and this environment
# Cannot have additional Python dependencies installed. We should not import any of the libraries
# here that are not available in stdlib! You should not import common_prek_utils.py here because
# they are importing rich library which is not available in the node environment.

PROVIDERS_ROOT = AIRFLOW_ROOT_PATH / "providers"
PROVIDERS_PATHS = {
    "fab": {
        "root": PROVIDERS_ROOT / "fab",
        "www": PROVIDERS_ROOT / "fab" / "src" / "airflow" / "providers" / "fab" / "www",
        "dist": PROVIDERS_ROOT / "fab" / "src" / "airflow" / "providers" / "fab" / "www" / "static" / "dist",
        "hash": PROVIDERS_ROOT / "fab" / "www-hash.txt",
    },
    "edge": {
        "root": PROVIDERS_ROOT / "edge3",
        "www": PROVIDERS_ROOT / "edge3" / "src" / "airflow" / "providers" / "edge3" / "plugins" / "www",
        "dist": PROVIDERS_ROOT
        / "edge3"
        / "src"
        / "airflow"
        / "providers"
        / "edge3"
        / "plugins"
        / "www"
        / "dist",
        "hash": PROVIDERS_ROOT / "edge3" / "www-hash.txt",
    },
    "ai": {
        "root": PROVIDERS_ROOT / "common" / "ai",
        "www": PROVIDERS_ROOT
        / "common"
        / "ai"
        / "src"
        / "airflow"
        / "providers"
        / "common"
        / "ai"
        / "plugins"
        / "www",
        "dist": PROVIDERS_ROOT
        / "common"
        / "ai"
        / "src"
        / "airflow"
        / "providers"
        / "common"
        / "ai"
        / "plugins"
        / "www"
        / "dist",
        "hash": PROVIDERS_ROOT / "common" / "ai" / "www-hash.txt",
        # common.ai ships two independent plugin bundles (HITL Review under
        # ``www`` and AI Trace under ``ai_trace_www``); each is compiled with the
        # same pnpm install + build and must be packaged, so extra bundles are
        # listed here and built alongside the primary one.
        "extra_bundles": [
            {
                "www": PROVIDERS_ROOT
                / "common"
                / "ai"
                / "src"
                / "airflow"
                / "providers"
                / "common"
                / "ai"
                / "plugins"
                / "ai_trace_www",
                "dist": PROVIDERS_ROOT
                / "common"
                / "ai"
                / "src"
                / "airflow"
                / "providers"
                / "common"
                / "ai"
                / "plugins"
                / "ai_trace_www"
                / "dist",
            },
        ],
    },
}


def get_directory_hash(directory: Path, skip_path_regexps: list[str]) -> str:
    files = sorted(directory.rglob("*"))
    for skip_path_regexp in skip_path_regexps:
        matcher = re.compile(skip_path_regexp)
        files = [file for file in files if not matcher.match(os.fspath(file.resolve()))]
    sha = hashlib.sha256()
    for file in files:
        if file.is_file() and not file.name.startswith("."):
            sha.update(file.read_bytes())
    return sha.hexdigest()


if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

INTERNAL_SERVER_ERROR = "500 Internal Server Error"

SKIP_PATH_REGEXPS = [".*/node_modules.*", ".*/.pnpm-store.*"]


def _build_bundle(provider_name: str, www_directory: Path, dist_directory: Path) -> None:
    """Run ``pnpm install --frozen-lockfile`` + ``pnpm build`` for one UI bundle."""
    if not dist_directory.exists():
        shutil.rmtree(dist_directory, ignore_errors=True)
    env = os.environ.copy()
    env["FORCE_COLOR"] = "true"
    for try_num in range(3):
        print(
            f"### Trying to install {provider_name} deps in {www_directory.name}: attempt {try_num + 1} ###"
        )
        result = subprocess.run(
            ["pnpm", "install", "--frozen-lockfile"],
            cwd=os.fspath(www_directory),
            text=True,
            check=False,
            capture_output=True,
        )
        if result.returncode == 0:
            break
        if try_num == 2 or INTERNAL_SERVER_ERROR not in result.stderr + result.stdout:
            print(result.stdout + "\n" + result.stderr)
            sys.exit(result.returncode)
    subprocess.check_call(["pnpm", "build"], cwd=os.fspath(www_directory), env=env)


def compile_assets(provider_name: str):
    if provider_name not in PROVIDERS_PATHS:
        raise ValueError(
            f"Provider '{provider_name}' is not supported. Supported providers: {list(PROVIDERS_PATHS.keys())}"
        )
    provider_paths = PROVIDERS_PATHS[provider_name]
    # A provider may ship more than one bundle (e.g. common.ai has HITL Review
    # and AI Trace); build them all. The change hash covers every source dir so
    # a change in any bundle triggers a rebuild of the set.
    bundles = [{"www": provider_paths["www"], "dist": provider_paths["dist"]}]
    bundles += provider_paths.get("extra_bundles", [])
    provider_paths["hash"].parent.mkdir(exist_ok=True, parents=True)

    all_dists_exist = all(b["dist"].exists() for b in bundles)
    new_hash = "".join(get_directory_hash(b["www"], skip_path_regexps=SKIP_PATH_REGEXPS) for b in bundles)
    if all_dists_exist:
        old_hash = provider_paths["hash"].read_text().strip() if provider_paths["hash"].exists() else ""
        if new_hash == old_hash:
            print(f"The {provider_name!r} UI sources have not changed! Skip regeneration.")
            return
        print(f"The {provider_name!r} UI sources changed, regenerating all bundles.")
        print("Old hash: " + old_hash)
        print("New hash: " + new_hash)

    for bundle in bundles:
        _build_bundle(provider_name, bundle["www"], bundle["dist"])
    new_hash = "".join(get_directory_hash(b["www"], skip_path_regexps=SKIP_PATH_REGEXPS) for b in bundles)
    provider_paths["hash"].write_text(new_hash + "\n")
    print(f"Assets compiled successfully. New hash: {new_hash}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compile provider assets for the specified provider.")
    parser.add_argument(
        "provider",
        type=str,
        help="The name of the provider whose assets should be compiled.",
        choices=list(PROVIDERS_PATHS.keys()),
    )
    args = parser.parse_args()
    compile_assets(args.provider)
