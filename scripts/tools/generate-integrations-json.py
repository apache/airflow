#!/usr/bin/env python
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
import re
import shutil

# pylint: disable=no-name-in-module
from docs.exts.provider_yaml_utils import load_package_data

# pylint: enable=no-name-in-module

AIRFLOW_SITE_DIR = os.environ.get('AIRFLOW_SITE_DIRECTORY')
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
DOCS_DIR = os.path.join(ROOT_DIR, 'docs')

if __name__ != "__main__":
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        "To run this script, run the ./generate-integrations-json.py command"
    )

if not (
    AIRFLOW_SITE_DIR
    and os.path.isdir(AIRFLOW_SITE_DIR)
    and os.path.isdir(os.path.join(AIRFLOW_SITE_DIR, 'docs-archive'))
):
    raise SystemExit(
        'Before using this script, set the environment variable AIRFLOW_SITE_DIRECTORY. This variable '
        'should contain the path to the airflow-site repository directory. '
        '${AIRFLOW_SITE_DIRECTORY}/docs-archive must exists.'
    )

ALL_PROVIDER_YAMLS = load_package_data()

result_integrations = []
for provider_info in ALL_PROVIDER_YAMLS:
    for integration in provider_info.get('integrations', []):
        doc_url = integration.get("how-to-guide")
        if doc_url:
            doc_url = doc_url[0].strip()
            doc_url = re.sub(f'/{provider_info["package-name"]}/', r"\g<0>stable/", doc_url)
            doc_url = re.sub(r'\.rst', '.html', doc_url)
        else:
            doc_url = f"/docs/{provider_info['package-name'].lower()}/stable/index.html"
        logo = integration.get("logo")

        result = {
            'name': integration['integration-name'],
            'url': doc_url,
        }
        if logo:
            result['logo'] = logo
        result_integrations.append(result)

result_integrations = sorted(result_integrations, key=lambda x: x['name'])
with open(os.path.join(AIRFLOW_SITE_DIR, 'landing-pages/site/static/integrations.json'), 'w') as f:
    f.write(
        json.dumps(
            result_integrations,
            indent=4,
        )
    )

shutil.copytree(
    src=os.path.join(DOCS_DIR, 'integration-logos'),
    dst=os.path.join(AIRFLOW_SITE_DIR, 'landing-pages/site/static/integration-logos'),
    dirs_exist_ok=True,
)
