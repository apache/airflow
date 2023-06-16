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
readonly RETENTION="${AIRFLOW__LOG_RETENTION_DAYS:-15}"
readonly DELETE_LOGS_FOLDERS="${AIRFLOW__LOG_DELETE_FOLDERS:-true}"

trap "exit" INT TERM

readonly EVERY=$((15*60))

echo "Running the improved cleaning logs..."
echo "Cleaning logs every $EVERY seconds"

while true; do
  if [ "${DELETE_LOGS_FOLDERS}" = true ]
  then
    echo "Trimming airflow logs folders to ${RETENTION} days."
    find "${DIRECTORY}"/logs \
      -type d -name 'lost+found' -prune -o \
      -type d -mtime +"${RETENTION}" -print0 | \
      xargs -0 rm -rf
  else
    echo "Trimming airflow logs files to ${RETENTION} days."
    find "${DIRECTORY}"/logs \
      -type d -name 'lost+found' -prune -o \
      -type f -mtime +"${RETENTION}" -name '*.log' -print0 | \
      xargs -0 rm -f
  fi

  seconds=$(( $(date -u +%s) % EVERY))
  (( seconds < 1 )) || sleep $((EVERY - seconds - 1))
  sleep 1
done
