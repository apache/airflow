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
"""Tests for checkout_no_credentials.py workflow validation logic.

The script has a module-level guard preventing import, so we replicate
the core check_file logic here and test it against the same rules.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml


def check_file(the_file: Path) -> int:
    """Replicate the check_file logic from checkout_no_credentials.py."""
    error_num = 0
    res = yaml.safe_load(the_file.read_text())
    for job in res["jobs"].values():
        if job.get("steps") is None:
            continue
        for step in job["steps"]:
            uses = step.get("uses")
            if uses is not None and uses.startswith("actions/checkout"):
                with_clause = step.get("with")
                if with_clause is None:
                    error_num += 1
                    continue
                path = with_clause.get("path")
                if path == "constraints":
                    continue
                if step.get("id") == "checkout-for-backport":
                    continue
                persist_credentials = with_clause.get("persist-credentials")
                if persist_credentials is None:
                    error_num += 1
                    continue
                if persist_credentials:
                    error_num += 1
                    continue
    return error_num


def _write_workflow(content: dict) -> Path:
    """Write a workflow dict as YAML to a temp file and return its Path."""
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False)
    yaml.dump(content, f)
    f.flush()
    f.close()
    return Path(f.name)


class TestCheckFile:
    def test_checkout_with_persist_credentials_false(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Checkout",
                            "uses": "actions/checkout@v4",
                            "with": {"persist-credentials": False},
                        }
                    ]
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 0

    def test_checkout_without_with_clause(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Checkout",
                            "uses": "actions/checkout@v4",
                        }
                    ]
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 1

    def test_checkout_without_persist_credentials(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Checkout",
                            "uses": "actions/checkout@v4",
                            "with": {"fetch-depth": 0},
                        }
                    ]
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 1

    def test_checkout_with_persist_credentials_true(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Checkout",
                            "uses": "actions/checkout@v4",
                            "with": {"persist-credentials": True},
                        }
                    ]
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 1

    def test_constraints_path_exception(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Checkout constraints",
                            "uses": "actions/checkout@v4",
                            "with": {"path": "constraints"},
                        }
                    ]
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 0

    def test_backport_id_exception(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Checkout for backport",
                            "id": "checkout-for-backport",
                            "uses": "actions/checkout@v4",
                            "with": {"fetch-depth": 0},
                        }
                    ]
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 0

    def test_non_checkout_step_ignored(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Setup Python",
                            "uses": "actions/setup-python@v5",
                        }
                    ]
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 0

    def test_job_without_steps(self):
        workflow = {
            "jobs": {
                "build": {
                    "uses": "./.github/workflows/reusable.yml",
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 0

    def test_multiple_errors(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Checkout 1",
                            "uses": "actions/checkout@v4",
                        },
                        {
                            "name": "Checkout 2",
                            "uses": "actions/checkout@v4",
                            "with": {"persist-credentials": True},
                        },
                    ]
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 2

    def test_multiple_jobs(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Checkout",
                            "uses": "actions/checkout@v4",
                            "with": {"persist-credentials": False},
                        }
                    ]
                },
                "test": {
                    "steps": [
                        {
                            "name": "Checkout",
                            "uses": "actions/checkout@v4",
                        }
                    ]
                },
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 1

    def test_run_step_without_uses(self):
        workflow = {
            "jobs": {
                "build": {
                    "steps": [
                        {
                            "name": "Run tests",
                            "run": "pytest",
                        }
                    ]
                }
            }
        }
        path = _write_workflow(workflow)
        assert check_file(path) == 0
