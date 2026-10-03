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
#
# dbt Core runner executed inside a single Kubernetes pod. It mirrors a dbt
# Cloud job: clone -> dbt deps -> dbt commands -> upload artifacts.
# The upload runs on BOTH success and failure via an EXIT trap. Everything is
# driven by environment variables injected by DbtKubernetesRunOperator; there
# are no positional arguments.
set -uo pipefail

: "${DBT_PROJECT_DIR:=/dbt}"

# --- storage helpers ---------------------------------------------------------
# Upload local file to s3:// or gs://.
_storage_upload() {
  case "$2" in
    s3://*) aws s3 cp "$1" "$2" --quiet ;;
    gs://*) gsutil cp "$1" "$2" ;;
  esac
}
# Download from s3:// or gs:// to a local file. Returns non-zero if not found.
_storage_download() {
  case "$1" in
    s3://*) aws s3 cp "$1" "$2" --quiet 2>/dev/null ;;
    gs://*) gsutil cp "$1" "$2" 2>/dev/null ;;
  esac
}

# --- artifact upload: runs on every exit (success or failure) ----------------
_upload_artifacts() {
  rc=$?
  set +x  # never trace credentials

  if [ -n "${ARTIFACT_DEST:-}" ]; then
    echo "::group::Upload Artifacts"
    if [ -d "${DBT_PROJECT_DIR}/target" ]; then
      echo "Uploading artifacts to '${ARTIFACT_DEST}' ..."
      _upload_max_retries=3
      _upload_retry_delay=5
      _upload_ok=0
      for _attempt in $(seq 1 "${_upload_max_retries}"); do
        case "${ARTIFACT_DEST}" in
          s3://*) aws s3 sync "${DBT_PROJECT_DIR}/target" "${ARTIFACT_DEST}" --only-show-errors && _upload_ok=1 && break ;;
          gs://*) gsutil -m rsync -r "${DBT_PROJECT_DIR}/target" "${ARTIFACT_DEST}" && _upload_ok=1 && break ;;
          *) echo "WARNING: unsupported ARTIFACT_DEST '${ARTIFACT_DEST}'"; break ;;
        esac
        echo "Attempt ${_attempt}/${_upload_max_retries} failed, retrying in ${_upload_retry_delay}s..."
        sleep "${_upload_retry_delay}"
      done
      [ "${_upload_ok}" -eq 1 ] && echo "Artifacts uploaded successfully." \
        || echo "WARNING: artifact upload failed after ${_upload_max_retries} attempts."
    else
      echo "WARNING: No target/ directory found, skipping artifact upload."
    fi
    echo "::endgroup::"
  fi

  return "${rc}"
}
trap _upload_artifacts EXIT

# --- credentials -------------------------------------------------------------
if [ -n "${GOOGLE_APPLICATION_CREDENTIALS_JSON:-}" ]; then
  printf '%s' "${GOOGLE_APPLICATION_CREDENTIALS_JSON}" > /tmp/gcp-key.json
  export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-key.json
fi

# --- 1) clone (with git cache fallback) --------------------------------------
if [ -n "${GIT_REPO_URL:-}" ]; then
  echo "::group::Git Clone"
  echo "Cloning ${GIT_REPO_URL} @ ${GIT_BRANCH:-main}..."
  _auth=""
  [ -n "${GIT_TOKEN:-}" ] && _auth="${GIT_TOKEN}@"
  _max_retries=3
  _retry_delay=10
  _clone_ok=0
  for _attempt in $(seq 1 "${_max_retries}"); do
    rm -rf "${DBT_PROJECT_DIR}"
    git clone --branch "${GIT_BRANCH:-main}" --single-branch --depth 1 \
      "https://${_auth}${GIT_REPO_URL}" "${DBT_PROJECT_DIR}" && { _clone_ok=1; break; }
    echo "Attempt ${_attempt}/${_max_retries} failed, retrying in ${_retry_delay}s..."
    [ "${_attempt}" -lt "${_max_retries}" ] && sleep "${_retry_delay}"
  done
  unset _auth GIT_TOKEN

  if [ "${_clone_ok}" -eq 1 ]; then
    echo "Git clone completed successfully."
    # Always update the cache on a successful clone so it stays current.
    if [ -n "${GIT_CACHE_DEST:-}" ]; then
      echo "Updating git cache → ${GIT_CACHE_DEST}"
      tar -czf /tmp/_repo_cache.tar.gz -C "${DBT_PROJECT_DIR}" . \
        && _storage_upload /tmp/_repo_cache.tar.gz "${GIT_CACHE_DEST}" \
        && echo "Cache updated." \
        || echo "WARNING: cache update failed (non-fatal)."
      rm -f /tmp/_repo_cache.tar.gz
    fi
  else
    # All clone attempts failed — fall back to cached repo if available.
    if [ -n "${GIT_CACHE_DEST:-}" ]; then
      echo "Git clone failed. Trying cache fallback → ${GIT_CACHE_DEST}"
      if _storage_download "${GIT_CACHE_DEST}" /tmp/_repo_cache.tar.gz; then
        rm -rf "${DBT_PROJECT_DIR}" && mkdir -p "${DBT_PROJECT_DIR}"
        tar -xzf /tmp/_repo_cache.tar.gz -C "${DBT_PROJECT_DIR}"
        rm -f /tmp/_repo_cache.tar.gz
        echo "Using cached repository (git cache hit)."
      else
        echo "Git clone failed and no cache available at ${GIT_CACHE_DEST}."
        echo "::endgroup::"; exit 1
      fi
    else
      echo "Git clone failed after ${_max_retries} attempts."
      echo "::endgroup::"; exit 1
    fi
  fi
  echo "::endgroup::"
fi
cd "${DBT_PROJECT_DIR}" || exit $?

# --- 3) dbt deps -------------------------------------------------------------
if [ -n "${INSTALL_DEPS:-}" ]; then
  echo "::group::Install dbt Packages"
  ${CMD_PREFIX:-} dbt deps || { echo "::endgroup::"; exit $?; }
  echo "dbt packages installed successfully."
  echo "::endgroup::"
fi

# --- 4) run dbt commands sequentially ----------------------------------------
_total=$(printf '%s' "${DBT_STEPS:-}" | grep -c '[^[:space:]]' || true)
_idx=0
rc=0
while IFS= read -r _step; do
  [ -z "${_step}" ] && continue
  _idx=$(( _idx + 1 ))
  echo "::group::[Step ${_idx}/${_total}] ${_step}"
  eval "${CMD_PREFIX:-} ${_step}" || { rc=$?; echo "::endgroup::"; echo "Step failed (exit ${rc}): ${_step}"; break; }
  echo "Step ${_idx}/${_total} completed successfully."
  echo "::endgroup::"
done <<EOF
${DBT_STEPS:-}
EOF
exit "${rc}"
