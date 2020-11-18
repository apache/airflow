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
import os
import sys
from glob import glob

import jinja2

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
DOCS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir, os.pardir))
BUILD_DIR = os.path.abspath(os.path.join(DOCS_DIR, '_build'))


def _get_jinja_env():
    loader = jinja2.FileSystemLoader(CURRENT_DIR, followlinks=True)
    env = jinja2.Environment(loader=loader, undefined=jinja2.StrictUndefined)
    return env


def _render_template(template_name, **kwargs):
    return _get_jinja_env().get_template(template_name).render(**kwargs)


def _render_content():
    provider_packages = [
        os.path.basename(os.path.dirname(p)) for p in glob(f"{BUILD_DIR}/docs/apache-airflow-providers-*/")
    ]
    content = _render_template('dev_index_template.html.jinja2', provider_packages=sorted(provider_packages))
    return content


def generate_index(out_file: str):
    content = _render_content()
    with open(out_file, "w") as output_file:
        output_file.write(content)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
    args = parser.parse_args()
    args.outfile.write(_render_content())
