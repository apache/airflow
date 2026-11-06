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
"""Shared configuration constants for the OpenLineage system-test DAGs."""

from __future__ import annotations

import os
from datetime import timedelta

# Defensive per-DagRun timeout so a hung/misbehaving DAG fails fast instead of running forever.
# Overridable via env var — the e2e harness runs every DAG in the same deployment and wants a
# tighter value than a single real system-test run needs.
DEFAULT_DAGRUN_TIMEOUT = timedelta(minutes=int(os.getenv("SYSTEM_TESTS_OL_DAGRUN_TIMEOUT_MINUTES", "10")))

# The e2e harness runs every Dag at once, so their tasks compete for the worker slots, which go to the highest
# priority first. By default that is the task with the most tasks downstream, e.g. the first of a long chain, while
# the Dags that wait for other Dag runs, loop or retry (their tasks have few downstream) take the longest to finish.
# Those Dags and the Dags they trigger use this weight, so their tasks are never queued behind the others.
CRITICAL_PATH_PRIORITY_WEIGHT = 100
