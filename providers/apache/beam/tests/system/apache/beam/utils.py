#
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
"""
Example Utils for Apache Beam operator example DAG's
"""

from __future__ import annotations

import os
from datetime import datetime
from urllib.parse import urlsplit

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCS_INPUT = os.environ.get("APACHE_BEAM_PYTHON", "gs://INVALID BUCKET NAME/shakespeare/kinglear.txt")
GCS_TMP = os.environ.get("APACHE_BEAM_GCS_TMP", "gs://INVALID BUCKET NAME/temp/")
GCS_STAGING = os.environ.get("APACHE_BEAM_GCS_STAGING", "gs://INVALID BUCKET NAME/staging/")
GCS_OUTPUT = os.environ.get("APACHE_BEAM_GCS_OUTPUT", "gs://INVALID BUCKET NAME/output")
GCS_PYTHON = os.environ.get("APACHE_BEAM_PYTHON", "gs://INVALID BUCKET NAME/wordcount_debugging.py")
GCS_PYTHON_DATAFLOW_ASYNC = os.environ.get(
    "APACHE_BEAM_PYTHON_DATAFLOW_ASYNC", "gs://INVALID BUCKET NAME/wordcount_debugging.py"
)
GCS_GO = os.environ.get("APACHE_BEAM_GO", "gs://INVALID BUCKET NAME/wordcount_debugging.go")
GCS_GO_DATAFLOW_ASYNC = os.environ.get(
    "APACHE_BEAM_GO_DATAFLOW_ASYNC", "gs://INVALID BUCKET NAME/wordcount_debugging.go"
)
GCS_JAR_DIRECT_RUNNER = os.environ.get(
    "APACHE_BEAM_DIRECT_RUNNER_JAR",
    "gs://INVALID BUCKET NAME/tests/dataflow-templates-bundled-java=11-beam-v2.25.0-DirectRunner.jar",
)
GCS_JAR_DATAFLOW_RUNNER = os.environ.get(
    "APACHE_BEAM_DATAFLOW_RUNNER_JAR", "gs://INVALID BUCKET NAME/word-count-beam-bundled-0.1.jar"
)
GCS_JAR_SPARK_RUNNER = os.environ.get(
    "APACHE_BEAM_SPARK_RUNNER_JAR",
    "gs://INVALID BUCKET NAME/tests/dataflow-templates-bundled-java=11-beam-v2.25.0-SparkRunner.jar",
)
GCS_JAR_FLINK_RUNNER = os.environ.get(
    "APACHE_BEAM_FLINK_RUNNER_JAR",
    "gs://INVALID BUCKET NAME/tests/dataflow-templates-bundled-java=11-beam-v2.25.0-FlinkRunner.jar",
)

GCS_JAR_DIRECT_RUNNER_PARTS = urlsplit(GCS_JAR_DIRECT_RUNNER)
GCS_JAR_DIRECT_RUNNER_BUCKET_NAME = GCS_JAR_DIRECT_RUNNER_PARTS.netloc
GCS_JAR_DIRECT_RUNNER_OBJECT_NAME = GCS_JAR_DIRECT_RUNNER_PARTS.path[1:]
GCS_JAR_DATAFLOW_RUNNER_PARTS = urlsplit(GCS_JAR_DATAFLOW_RUNNER)
GCS_JAR_DATAFLOW_RUNNER_BUCKET_NAME = GCS_JAR_DATAFLOW_RUNNER_PARTS.netloc
GCS_JAR_DATAFLOW_RUNNER_OBJECT_NAME = GCS_JAR_DATAFLOW_RUNNER_PARTS.path[1:]
GCS_JAR_SPARK_RUNNER_PARTS = urlsplit(GCS_JAR_SPARK_RUNNER)
GCS_JAR_SPARK_RUNNER_BUCKET_NAME = GCS_JAR_SPARK_RUNNER_PARTS.netloc
GCS_JAR_SPARK_RUNNER_OBJECT_NAME = GCS_JAR_SPARK_RUNNER_PARTS.path[1:]
GCS_JAR_FLINK_RUNNER_PARTS = urlsplit(GCS_JAR_FLINK_RUNNER)
GCS_JAR_FLINK_RUNNER_BUCKET_NAME = GCS_JAR_FLINK_RUNNER_PARTS.netloc
GCS_JAR_FLINK_RUNNER_OBJECT_NAME = GCS_JAR_FLINK_RUNNER_PARTS.path[1:]


DEFAULT_ARGS = {
    "default_pipeline_options": {"output": "/tmp/example_beam"},
    "trigger_rule": TriggerRule.ALL_DONE,
}
START_DATE = datetime(2021, 1, 1)
