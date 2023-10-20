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
"""
Builds provider documentation
"""
import argparse
import os
import subprocess

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Builds provider documenation.")
    parser.add_argument(
        "--providers",
        dest="providers",
        action="store",
        required=True,
        nargs='+',
        help="List of provider ids",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    provider_filters = []
    for provider in args.providers:
        provider_filters.extend(["--package-filter", f"apache-airflow-providers-{provider.replace('.', '-')}"])

    try:
        subprocess.run([".breeze", "build-docs", "--clean-build"] + provider_filters, check=True)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to build provider docs with .breeze: {e}")

    os.chdir(os.environ["AIRFLOW_SITE_DIRECTORY"])
