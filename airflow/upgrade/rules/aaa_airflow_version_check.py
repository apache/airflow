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

# This module starts with `aaa_` so that it is sorted first alphabetically, but is still a valid python module
# name (starting with digitis is not valid)

from __future__ import absolute_import

from packaging.version import Version
import requests

from airflow.upgrade.rules.base_rule import BaseRule

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata


class VersionCheckRule(BaseRule):

    title = "Check for latest versions of apache-airflow and checker"

    description = """\
Check that the latest version of apache-airflow-upgrade-check is installed, and
that you are on the latest 1.10.x release of apache-airflow."""

    def pypi_releases(self, distname):
        """
        Get all the non-dev releases of a dist from PyPI
        """

        resp = requests.get("https://pypi.org/pypi/{}/json".format(distname))
        resp.raise_for_status()

        for rel_string in resp.json()["releases"].keys():
            ver = Version(rel_string)
            if ver.is_devrelease or ver.is_prerelease:
                continue
            yield ver

    def check(self):

        current_airflow_version = Version(__import__("airflow").__version__)
        try:
            upgrade_check_ver = Version(
                importlib_metadata.distribution("apache-airflow-upgrade-check").version,
            )
        except importlib_metadata.PackageNotFoundError:
            upgrade_check_ver = Version("0.0.0")

        try:
            latest_airflow_v1_release = sorted(
                filter(lambda v: v.major == 1, self.pypi_releases("apache-airflow"))
            )[-1]

            if current_airflow_version < latest_airflow_v1_release:
                yield (
                    "There is a more recent version of apache-airflow. Please upgrade to {} and re-run this"
                    " script"
                ).format(latest_airflow_v1_release)

            latest_upgrade_check_release = sorted(
                self.pypi_releases("apache-airflow-upgrade-check")
            )[-1]

            if upgrade_check_ver < latest_upgrade_check_release:
                yield (
                    "There is a more recent version of apache-airflow-upgrade-check. Please upgrade to {}"
                    " and re-run this script"
                ).format(latest_upgrade_check_release)
        except Exception as e:
            yield "Unable to go ask PyPI.org for latest release information: " + str(e)
