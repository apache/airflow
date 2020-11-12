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

import json
import os
from typing import Any, Dict

import jsonschema
import yaml
from sphinx.application import Sphinx

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
PROVIDER_DATA_PATH = os.path.join(ROOT_DIR, "dev", "package-data", "providers.yaml")
PROVIDER_DATA_SCHEMA_PATH = f"{PROVIDER_DATA_PATH}.schema.json"


def _load_schema() -> Dict[str, Any]:
    with open(PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        content = json.load(schema_file)
    return content


def _load_package_data():
    schema = _load_schema()
    with open(PROVIDER_DATA_PATH) as yaml_file:
        content = yaml.safe_load(yaml_file)
    jsonschema.validate(content, schema=schema)
    return content


def _on_config_inited(app, config):
    del app
    jinja_context = getattr(config, 'jinja_contexts', None) or {}

    jinja_context['providers_ctx'] = {'package_data': _load_package_data()}

    config.jinja_contexts = jinja_context


def setup(app: Sphinx):
    """Setup plugin"""
    app.setup_extension('sphinxcontrib.jinja')
    app.connect("config-inited", _on_config_inited)

    return {'parallel_read_safe': True, 'parallel_write_safe': True}
