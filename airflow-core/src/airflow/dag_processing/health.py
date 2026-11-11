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

import structlog

from airflow.configuration import conf

log = structlog.get_logger(__name__)

_HEALTH_WRITE_WARNING_INTERVAL = 60.0
_next_health_write_warnings: dict[Path, float] = {}


def write_processor_health(last_api_heartbeat: float) -> None:
    path = Path(conf.get("dag_processor", "health_check_file"))
    try:
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
    except OSError as error:
        now = monotonic()
        next_warning = _next_health_write_warnings.get(path)
        if next_warning is None or now >= next_warning:
            log.warning(
                "Unable to write [dag_processor] health_check_file",
                path=str(path),
                error=str(error),
            )
            _next_health_write_warnings[path] = now + _HEALTH_WRITE_WARNING_INTERVAL
    else:
        _next_health_write_warnings.pop(path, None)


def check_processor_health(*, readiness: bool = False) -> None:
    try:
        record = json.loads(Path(conf.get("dag_processor", "health_check_file")).read_text())
        os.kill(record["pid"], 0)
        now = monotonic()
        maximum_age = conf.getint("dag_processor", "health_check_threshold")
        fields = ("loop", "api") if readiness else ("loop",)
        for field in fields:
            age = now - record[field]
            if not 0 <= age <= maximum_age:
                raise ValueError(
                    f"Processor {field} heartbeat age {age:.1f}s is outside 0..{maximum_age}s "
                    "([dag_processor] health_check_threshold)"
                )
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SystemExit(f"Dag processor is not healthy: {error}") from error
