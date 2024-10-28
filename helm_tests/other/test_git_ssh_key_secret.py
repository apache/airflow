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

import jmespath

from tests.charts.helm_template_generator import render_chart


class TestGitSSHKeySecret:
    """Tests git-ssh secret."""

    def test_create_git_ssh_key_secret(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "sshKey": "cm9tIGlzIHRoZSBraW5n",
                    },
                    "persistence": {"enabled": True},
                }
            },
            show_only=["templates/secrets/git-ssh-key-secret.yaml"],
        )

        assert "release-name-ssh-secret" == jmespath.search("metadata.name", docs[0])
        assert "Y205dElHbHpJSFJvWlNCcmFXNW4=" == jmespath.search(
            "data.gitSshKey", docs[0]
        )
