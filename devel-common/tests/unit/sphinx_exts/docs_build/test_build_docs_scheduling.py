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

from unittest import mock

from docs import build_docs


@mock.patch.object(build_docs, "_estimated_build_weight", autospec=True)
def test_sort_heaviest_first_orders_by_weight_then_name(mock_weight):
    weights = {"google": 3000, "amazon": 1500, "ftp": 20, "ssh": 20, "docker-stack": 5}
    mock_weight.side_effect = weights.__getitem__

    ordered = build_docs.sort_heaviest_first(["ssh", "docker-stack", "google", "ftp", "amazon"])

    assert ordered == ["google", "amazon", "ftp", "ssh", "docker-stack"]


def test_estimated_build_weight_ranks_real_packages_sensibly():
    google = build_docs._estimated_build_weight("apache-airflow-providers-google")
    core = build_docs._estimated_build_weight("apache-airflow")
    ftp = build_docs._estimated_build_weight("apache-airflow-providers-ftp")

    assert google > ftp > 0
    assert core > ftp
    assert build_docs._estimated_build_weight("docker-stack") == 0
