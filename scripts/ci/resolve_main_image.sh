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

mkdir -p "${RUNNER_TEMP}/image-artifacts/${IMAGE_KIND}"
selection_name="selected-${IMAGE_KIND}-${IMAGE_PYTHON}-${IMAGE_PLATFORM#linux/}-${GITHUB_RUN_ATTEMPT}"
echo "built-image-name=${selection_name/selected-/built-}" >> "${GITHUB_OUTPUT}"
echo "selection-name=${selection_name}" \
  >> "${GITHUB_OUTPUT}"
if [[ "${IMAGE_REUSE_DISABLED}" == "true" && "${IMAGE_PUBLISH}" != "true" ]]; then
  echo "hit=false" >> "${GITHUB_OUTPUT}"
  exit 0
fi
fingerprint="${RUNNER_TEMP}/image-artifacts/${IMAGE_KIND}/fingerprint.json"
selection="${RUNNER_TEMP}/image-artifacts/${IMAGE_KIND}/selection.json"
if [[ -n "${IMAGE_BASE:-}" ]]; then
  base_digest="${IMAGE_BASE#debian@}"
elif ! base_digest="$(docker buildx imagetools inspect debian:bookworm-slim \
  --format '{{.Manifest.Digest}}')"; then
  if [[ "${IMAGE_PUBLISH}" == "true" ]]; then
    exit 1
  fi
  echo "::notice::Could not resolve base image; building normally."
  echo "hit=false" >> "${GITHUB_OUTPUT}"
  exit 0
fi
if [[ ! "${base_digest}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  if [[ "${IMAGE_PUBLISH}" == "true" ]]; then
    echo "::error::Registry returned an invalid base image digest."
    exit 1
  fi
  echo "::notice::Registry returned an invalid base identity; building normally."
  echo "hit=false" >> "${GITHUB_OUTPUT}"
  exit 0
fi
echo "base-image=debian@${base_digest}" >> "${GITHUB_OUTPUT}"
constraints=()
if [[ -n "${IMAGE_CONSTRAINTS_FILE}" ]]; then
  constraints+=(--constraints-file "${IMAGE_CONSTRAINTS_FILE}")
fi
uv run --project dev/breeze python -m airflow_breeze.utils.image_artifacts fingerprint \
  --kind "${IMAGE_KIND}" --python "${IMAGE_PYTHON}" --platform "${IMAGE_PLATFORM}" \
  --base-image-digest "${base_digest}" --build-arg DEBIAN_VERSION=bookworm \
  --build-arg INSTALL_MYSQL_CLIENT_TYPE=mariadb --build-arg USE_UV=true \
  --build-arg UPGRADE_TO_NEWER_DEPENDENCIES=false --build-arg DEFAULT_BRANCH=main \
  --build-arg DEFAULT_CONSTRAINTS_BRANCH=constraints-main "${constraints[@]}" \
  --output "${fingerprint}"
flags=()
if [[ "${IMAGE_REUSE_DISABLED}" == "true" ]]; then
  flags+=(--disabled)
fi
uv run --project dev/breeze python -m airflow_breeze.utils.image_artifacts resolve \
  --fingerprint-file "${fingerprint}" --repository apache/airflow \
  --output "${selection}" "${flags[@]}"
echo "hit=$(jq -r '.hit' "${selection}")" >> "${GITHUB_OUTPUT}"
artifact_name="$(jq -r '.["artifact-name"]' "${fingerprint}")"
echo "artifact-name=${artifact_name}" >> "${GITHUB_OUTPUT}"
if [[ "${IMAGE_PUBLISH}" == "true" ]]; then
  publication="${RUNNER_TEMP}/image-artifacts/${IMAGE_KIND}/publication.json"
  uv run --project dev/breeze python -m airflow_breeze.utils.image_artifacts publication-exists \
    --artifact-name "${artifact_name}" --repository "${GITHUB_REPOSITORY}" \
    --run-id "${GITHUB_RUN_ID}" --output "${publication}"
  echo "publication-exists=$(jq -r '.exists' "${publication}")" >> "${GITHUB_OUTPUT}"
fi
