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
import os
import sys
from pathlib import Path

import jinja2

# isort:off (needed to workaround isort bug)
from docs.exts.provider_yaml_utils import load_package_data

# isort:on (needed to workaround isort bug)

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
DOCS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir, os.pardir))
BUILD_DIR = os.path.abspath(os.path.join(DOCS_DIR, "_build"))
ALL_PROVIDER_YAMLS_WITH_SUSPENDED = load_package_data(include_suspended=True)


def _get_jinja_env():
    loader = jinja2.FileSystemLoader(CURRENT_DIR, followlinks=True)
    env = jinja2.Environment(loader=loader, undefined=jinja2.StrictUndefined)
    return env


def _render_template(template_name, **kwargs):
    return _get_jinja_env().get_template(template_name).render(**kwargs)


def _render_content():
    providers = []
    provider_yamls = {p["package-name"]: p for p in ALL_PROVIDER_YAMLS_WITH_SUSPENDED}
    for path in sorted(Path(BUILD_DIR).glob("docs/apache-airflow-providers-*/")):
        package_name = path.name
        try:
            providers.append(provider_yamls[package_name])
        except KeyError:
            print(
                f"WARNING! Could not find provider.yaml file for package: {package_name}"
            )

    content = _render_template("dev_index_template.html.jinja2", providers=providers)
    return content


def generate_index(out_file: str) -> None:
    """
    Generates an index for development documentation.

    :param out_file: The path where the index should be stored
    """
    content = _render_content()
    with open(out_file, "w") as output_file:
        output_file.write(content)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "outfile", nargs="?", type=argparse.FileType("w"), default=sys.stdout
    )
    args = parser.parse_args()
    args.outfile.write(_render_content())
