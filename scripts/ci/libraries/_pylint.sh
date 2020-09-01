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

# In case of the pylint checks we filter out some files which are still in pylint_todo.txt list
function pylint::filter_out_files_from_pylint_todo_list() {
  FILTERED_FILES=()
  set +e
  for FILE in "$@"
  do
      if [[ ${FILE} == "airflow/migrations/versions/"* ]]; then
          # Skip all generated migration scripts
          continue
      fi
      if ! grep -x "./${FILE}" <"${AIRFLOW_SOURCES}/scripts/ci/pylint_todo.txt" >/dev/null; then
          FILTERED_FILES+=("${FILE}")
      fi
  done
  set -e
  export FILTERED_FILES
}
