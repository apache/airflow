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

"""Local probes for a processor that does not have metadata database credentials."""

from __future__ import annotations

import json
import os
from pathlib import Path
from time import monotonic

from airflow.configuration import conf


def write_processor_health(last_api_heartbeat: float) -> None:
    path = Path(conf.get("dag_processor", "health_check_file"))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}")
    temporary.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "loop": monotonic(),
                "api": last_api_heartbeat,
            }
        )
    )
    temporary.replace(path)


def check_processor_health(*, readiness: bool = False) -> None:
    try:
        record = json.loads(Path(conf.get("dag_processor", "health_check_file")).read_text())
        os.kill(record["pid"], 0)
        now = monotonic()
        maximum_age = conf.getint("dag_processor", "health_check_threshold")
        fields = ("loop", "api") if readiness else ("loop",)
        if any(not 0 <= now - record[field] <= maximum_age for field in fields):
            raise ValueError("Processor heartbeat has expired")
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SystemExit("Dag processor is not healthy") from error
