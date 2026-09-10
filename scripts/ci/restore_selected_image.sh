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

selection="${RUNNER_TEMP}/selected-image.json"
if ! uv run --project dev/breeze python -m airflow_breeze.utils.image_artifacts restore-selection \
  --kind "${IMAGE_KIND}" --python "${IMAGE_PYTHON}" --platform "${IMAGE_PLATFORM}" \
  --repository "${GITHUB_REPOSITORY}" --run-id "${GITHUB_RUN_ID}" \
  --run-attempt "${GITHUB_RUN_ATTEMPT}" --output "${selection}" --output-directory /mnt \
  --require-selection; then
  echo "::error::The selected image could not be downloaded or verified after bounded retries."
  echo "::error::Re-run all jobs so the image producer can select or build an available image."
  exit 1
fi
hit="$(jq -r '.hit' "${selection}")"
echo "hit=${hit}" >> "${GITHUB_OUTPUT}"
if [[ "${hit}" == "true" && "${IMAGE_KIND}" == "ci" && \
  "$(jq -r '.scope' "${selection}")" != "current-run" ]]; then
  echo "MOUNT_SOURCES=selected" >> "${GITHUB_ENV}"
fi
