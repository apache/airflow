#!/usr/bin/env bash

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

set -euo pipefail

readonly DIRECTORY="${AIRFLOW_HOME:-/usr/local/airflow}"
# Allow retention to be specified in either days (legacy) or minutes.
# Minutes take precedence when set to a positive integer.
readonly RETENTION_DAYS="${AIRFLOW__LOG_RETENTION_DAYS:-15}"
readonly RETENTION_MINUTES="${AIRFLOW__LOG_RETENTION_MINUTES:-}"
readonly FREQUENCY="${AIRFLOW__LOG_CLEANUP_FREQUENCY_MINUTES:-15}"

trap "exit" INT TERM

readonly EVERY=$((FREQUENCY*60))

echo "Cleaning logs every $EVERY seconds"

while true; do
  if [[ -n "${RETENTION_MINUTES}" && "${RETENTION_MINUTES}" -ge 0 ]]; then
    echo "Trimming airflow logs older than ${RETENTION_MINUTES} minutes."
    find "${DIRECTORY}"/logs \
      -type d -name 'lost+found' -prune -o \
      -type f -mmin +"${RETENTION_MINUTES}" -name '*.log' -print0 | \
      xargs -0 rm -f || true
  else
    echo "Trimming airflow logs to ${RETENTION_DAYS} days."
    find "${DIRECTORY}"/logs \
      -type d -name 'lost+found' -prune -o \
      -type f -mtime +"${RETENTION_DAYS}" -name '*.log' -print0 | \
      xargs -0 rm -f || true
  fi

  find "${DIRECTORY}"/logs -type d -empty -delete || true

  seconds=$(( $(date -u +%s) % EVERY))
  (( seconds < 1 )) || sleep $((EVERY - seconds - 1))
  sleep 1
done
