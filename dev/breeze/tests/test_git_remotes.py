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

import pytest

from airflow_breeze.utils.git_remotes import (
    Remote,
    find_remote,
    find_remote_for_repo,
    get_github_repo,
    parse_remotes,
)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param("https://github.com/apache/airflow.git", "apache/airflow", id="https"),
        pytest.param("https://github.com/apache/airflow", "apache/airflow", id="https-no-suffix"),
        pytest.param(
            "https://token@github.com/someone/airflow.git", "someone/airflow", id="https-credentials"
        ),
        pytest.param("git@github.com:someone/my-airflow.git", "someone/my-airflow", id="ssh"),
        pytest.param("ssh://git@github.com/someone/airflow.git", "someone/airflow", id="ssh-url"),
        pytest.param("http://github.com/apache/airflow.git", "apache/airflow", id="http"),
        pytest.param("https://www.github.com/apache/airflow", "apache/airflow", id="www"),
        pytest.param("https://GitHub.com/apache/airflow.git", "apache/airflow", id="upper-case-host"),
        pytest.param(
            "ssh://git@github.com:22/someone/airflow.git", "someone/airflow", id="ssh-url-with-port"
        ),
        pytest.param(
            "git@github-personal:apache/airflow.git", "apache/airflow", id="ssh-host-alias-from-ssh-config"
        ),
        pytest.param("github-work:someone/airflow.git", "someone/airflow", id="ssh-host-alias-without-user"),
        pytest.param(
            "ssh://git@github-personal:2222/apache/airflow.git", "apache/airflow", id="ssh-url-alias"
        ),
        pytest.param("git@gitlab.com:someone/airflow.git", None, id="ssh-not-github"),
        pytest.param("git@github.corp.example:apache/airflow.git", None, id="ssh-github-enterprise-host"),
        pytest.param("https://gitlab.com/someone/airflow.git", None, id="not-github"),
        pytest.param("/local/path/airflow", None, id="local-path"),
    ],
)
def test_get_github_repo(url, expected):
    assert get_github_repo(url) == expected


REMOTE_V = """\
origin\tgit@github.com:someone/airflow.git (fetch)
origin\tgit@github.com:someone/airflow.git (push)
upstream\thttps://github.com/apache/airflow.git (fetch)
upstream\thttps://github.com/apache/airflow.git (push)
site\thttps://github.com/apache/airflow-site.git (fetch)
site\thttps://github.com/apache/airflow-site.git (push)
"""


def test_parse_remotes():
    assert parse_remotes(REMOTE_V) == [
        Remote("origin", "git@github.com:someone/airflow.git", "someone/airflow"),
        Remote("upstream", "https://github.com/apache/airflow.git", "apache/airflow"),
        Remote("site", "https://github.com/apache/airflow-site.git", "apache/airflow-site"),
    ]


def test_find_remote_for_repo_matches_the_exact_repository():
    remotes = parse_remotes("site\thttps://github.com/apache/airflow-site.git (fetch)\n" + REMOTE_V)

    assert find_remote_for_repo(remotes, "Apache/Airflow") == remotes[2]
    assert find_remote_for_repo(remotes, "other/repo") is None


def test_find_remote():
    remotes = parse_remotes(REMOTE_V)

    assert find_remote(remotes, "origin") == remotes[0]
    assert find_remote(remotes, "missing") is None
