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

from airflow.upgrade.rules.aaa_airflow_version_check import VersionCheckRule


class TestVersionCheckRule:
    def test_new_airflow_version_available(self, requests_mock):
        requests_mock.get(
            "https://pypi.org/pypi/apache-airflow/json",
            json={
                "releases": {
                    # The values here don't matter
                    "1.10.22b1": None,
                    "1.10.20": None,
                    "1.10.21": None,
                }
            },
        )
        requests_mock.get(
            "https://pypi.org/pypi/apache-airflow-upgrade-check/json",
            json={
                "releases": {
                    "0.0.0": None,
                }
            },
        )

        results = list(VersionCheckRule().check())
        assert len(results) == 1, results

        msg = results[0]
        assert " 1.10.21 " in msg
        assert "apache-airflow" in msg
        assert "apache-airflow-upgrade-check" not in msg

    def test_new_airflow_upgrade_check_version_available(self, requests_mock):
        requests_mock.get(
            "https://pypi.org/pypi/apache-airflow/json",
            json={
                "releases": {
                    # The values here don't matter
                    "1.10.0": None,
                }
            },
        )
        requests_mock.get(
            "https://pypi.org/pypi/apache-airflow-upgrade-check/json",
            json={
                "releases": {
                    "99.0.0": None,
                }
            },
        )

        results = list(VersionCheckRule().check())
        assert len(results) == 1, results

        msg = results[0]
        assert " 99.0.0 " in msg
        assert "apache-airflow-upgrade-check" in msg
