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

import semver


def object_exists(path: str):
    """Returns true if importable python object is there."""
    from airflow.utils.module_loading import import_string

    try:
        import_string(path)
        return True
    except ImportError:
        return False


def get_provider_version(provider_name):
    """
    Returns provider version given provider package name.

    Example::
        if provider_version('apache-airflow-providers-cncf-kubernetes') >= (6, 0):
            raise Exception(
                "You must now remove `get_kube_client` from PodManager "
                "and make kube_client a required argument."
            )
    """
    from airflow.providers_manager import ProvidersManager

    info = ProvidersManager().providers[provider_name]
    return semver.VersionInfo.parse(info.version)


def get_provider_min_airflow_version(provider_name):
    from airflow.providers_manager import ProvidersManager

    p = ProvidersManager()
    deps = p.providers[provider_name].data["dependencies"]
    airflow_dep = [x for x in deps if x.startswith("apache-airflow")][0]
    min_airflow_version = tuple(map(int, airflow_dep.split(">=")[1].split(".")))
    return min_airflow_version
