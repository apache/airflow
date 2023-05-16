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

import subprocess
import tempfile

NAMESPACE_TEMPLATE = """
kind: Namespace
apiVersion: v1
metadata:
  name: aws-observability
  labels:
    aws-observability: enabled
"""

CONFIGMAP_TEMPLATE = """
kind: ConfigMap
apiVersion: v1
metadata:
  name: aws-logging
  namespace: aws-observability
data:
  flb_log_cw: "false"  # Set to true to ship Fluent Bit process logs to CloudWatch.
  filters.conf: |
    [FILTER]
        Name parser
        Match *
        Key_name log
        Parser crio
    [FILTER]
        Name kubernetes
        Match kube.*
        Merge_Log On
        Keep_Log Off
        Buffer_Size 0
        Kube_Meta_Cache_TTL 300s
  output.conf: |
    [OUTPUT]
        Name cloudwatch_logs
        Match   kube.*
        region {region}
        log_group_name {log_group_name}
        log_stream_prefix {log_stream_prefix}
        log_retention_days {log_retention_days}
        auto_create_group {auto_create_group}
  parsers.conf: |
    [PARSER]
        Name crio
        Format Regex
        Regex ^(?<time>[^ ]+) (?<stream>stdout|stderr) (?<logtag>P|F) (?<log>.*)$
        Time_Key    time
        Time_Format %Y-%m-%dT%H:%M:%S.%L%z
"""


def enable_fargate_logging(
    *,
    region: str,
    log_group_name: str,
    log_stream_prefix: str = "",
    log_retention_days: int = 60,
    auto_create_group: bool = True,
) -> subprocess.CompletedProcess:

    with tempfile.NamedTemporaryFile(mode="w") as namespace:
        namespace.write(NAMESPACE_TEMPLATE)
        namespace.flush()

        result = subprocess.run(
            ["kubectl", "apply", "-f", namespace],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        if result.returncode != 0:
            # If the process did not complete successfully, return the stdout string. No point continuing.
            return result

    configmap_text = CONFIGMAP_TEMPLATE.format(
        region=region,
        log_group_name=log_group_name,
        log_stream_prefix=log_stream_prefix,
        log_retention_days=log_retention_days,
        auto_create_group=auto_create_group,
    )

    with tempfile.NamedTemporaryFile(mode="w") as configmap:
        configmap.write(configmap_text)
        configmap.flush()

        return subprocess.run(
            ["kubectl", "apply", "-f", configmap],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
