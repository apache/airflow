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

import logging
import os
import subprocess
from typing import Dict, List, Optional

import pytest
import yaml

log = logging.getLogger(__name__)

CHART_FOLDER = os.path.normpath(os.path.join(__file__, os.pardir, os.pardir))


def run_cmd(cmd, **kwargs):
    """
    Helper method to run a command, and include any output in case of error.
    """
    try:
        log.debug('Running %s', cmd)
        return subprocess.check_output(
            cmd,
            stderr=subprocess.STDOUT,
            **kwargs
        )
    except subprocess.CalledProcessError as e:
        raise AssertionError(f'Command {cmd} failed with: {e.output.decode("utf-8")}')


@pytest.fixture(scope="session", autouse=True)
def initalize_chart():
    """
    Get the helm chart ready to install/render.

    This is run for us automatically by pytest once per test-session
    """
    run_cmd(['helm', 'repo', 'add', 'stable', 'https://kubernetes-charts.storage.googleapis.com/'])
    run_cmd(['helm', 'dep', 'update', CHART_FOLDER])


@pytest.fixture
def render_chart(request):
    """
    Pytest fixture to render the helm chart with the given values, and return
    rendered templates as a list of  python dicts

    >>> resources = render_chart("TEST-BASIC", {"chart": {'metadata': 'AA'}})
    >>> kind_and_name = [ (resource['kind'], resource['metadata']['name']) for resource in resources
    >>> ('Deployment', 'TEST-BASIC-scheduler') in kind_and_name
    True
    """
    # If we set DEFAULT_TEMPLATES on the Test class, use that as the default value for templates
    default_templates = getattr(request.cls, "DEFAULT_TEMPLATES", None)

    def _do_render(
        *,
        name: str = "test-release",
        templates: Optional[List[str]] = None,
        values: Optional[Dict] = None,
    ):
        content = yaml.dump(values or {})

        cmd = [
            'helm',
            'template',
        ]

        for template in (templates or default_templates or []):
            cmd += ['--show-only', f'templates/{template}']

        cmd += [
            name,
            CHART_FOLDER,
            '--values',
            '-',
        ]

        templates = run_cmd(cmd, input=content.encode('utf-8'))
        return list(filter(None, yaml.safe_load_all(templates)))  # type: ignore

    return _do_render
