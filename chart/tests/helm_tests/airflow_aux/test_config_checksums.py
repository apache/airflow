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

from chart_utils.helm_template_generator import render_chart

# The checksum/* annotations exist so that a configuration change restarts the component that
# reads it. Hashing the whole rendered manifest also pulled in metadata.labels, which carry
# helm.sh/chart, so the checksums moved on a chart version bump and every component restarted
# even though nothing it reads had changed.

# A label added through the `labels` value lands in the same metadata.labels block as
# chart: "airflow-<version>", so it stands in for the chart version, which is not settable
# through values.
#
# jwtSecret and the metadata connection password are generated when not supplied, which would
# make any two renders differ for reasons unrelated to the configuration.
STABLE_VALUES = {
    "jwtSecret": "a-fixed-jwt-secret",
    "metadataConnection": {"pass": "a-fixed-password"},
}

CHECKSUM_BEARING_TEMPLATES = [
    "templates/api-server/api-server-deployment.yaml",
    "templates/scheduler/scheduler-deployment.yaml",
    "templates/workers/worker-deployment.yaml",
    "templates/triggerer/triggerer-deployment.yaml",
    "templates/dag-processor/dag-processor-deployment.yaml",
    "templates/statsd/statsd-deployment.yaml",
]


def _checksums(values, show_only):
    """Every checksum/* pod annotation rendered by the given templates."""
    checksums = {}
    for template in show_only:
        for doc in render_chart(values=values, show_only=[template]):
            annotations = doc["spec"]["template"]["metadata"].get("annotations", {})
            for key, value in annotations.items():
                if key.startswith("checksum/"):
                    checksums[f"{template}:{key}"] = value
    return checksums


class TestConfigChecksums:
    """The checksum/* annotations must track configuration, not the chart version."""

    def test_checksums_ignore_object_labels(self):
        before = _checksums(STABLE_VALUES, CHECKSUM_BEARING_TEMPLATES)
        assert before, "expected at least one checksum/* annotation"

        after = _checksums(
            {**STABLE_VALUES, "labels": {"probe": "xyz"}},
            CHECKSUM_BEARING_TEMPLATES,
        )

        assert before == after

    def test_airflow_config_checksum_tracks_a_config_change(self):
        template = ["templates/scheduler/scheduler-deployment.yaml"]
        before = _checksums(STABLE_VALUES, template)
        after = _checksums(
            {**STABLE_VALUES, "config": {"core": {"parallelism": 99}}},
            template,
        )

        key = "templates/scheduler/scheduler-deployment.yaml:checksum/airflow-config"
        assert before[key] != after[key]
