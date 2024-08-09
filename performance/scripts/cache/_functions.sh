#!/usr/bin/env bash
# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# This software is provided as-is,
# without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.

# Functions used to cache the ${HOME}/.cache folder between the runs
# It currently uses GCS storage bucket to store it

# Initializes the script
function cache::initialize() {
    set -eao pipefail

    ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../" && pwd )"
    readonly ROOT_DIR

    # shellcheck source=scripts/_common_values.sh
    source "${ROOT_DIR}/scripts/_common_values.sh"

    CACHE_FILE="/cache/cache.tgz"
    readonly CACHE_FILE

    SHA_FILES="/cache/sha_files_txt"
    readonly SHA_FILES

    SHA256SUM_FILE="/cache/sha256sum.txt"
    readonly SHA256SUM_FILE

    # Prefix of the cache URL. You should set it to the URL your bucket (GCS/S3 etc.) if you want to
    # Speed up the CI builds you have to give your CI service account should permission to write to the
    # storage pointed at with the URL.
    # Note: If you do not set the cache, the build will continue to work but it will be slower
    # The cache is crated with cache-<SHA_OF_SELECTED_SOURCE_FILES>.tgz name
    CACHE_URL_PREFIX=${CACHE_URL_PREFIX:="gs://polidea-airflow-gepard-cloud-build-cache/"}
    readonly CACHE_URL_PREFIX

    # Name of the Cache tool use. You should set it to the name of your util to copy the files from/to
    # the cache. This might be gsutil or s3. The tool will be called with:
    # `<CACHE_UTILITY> cp URL FILE` to download cache and <CACHE_UTILITY> cp FILE URL
    CACHE_UTILITY=${CACHE_UTILITY:="gsutil"}
    readonly CACHE_UTILITY

    cd "${ROOT_DIR}"
}

function cache::calculate_cache_sha() {
    # Calculate SHA of the combined files and folders that might cause cache rebuilding
    find "scripts/pre-commit" -type f -exec sha256sum {} \; | LC_ALL="C" sort > "${SHA_FILES}"
    sha256sum .pre-commit-config.yaml >> "${SHA_FILES}"
    cat "${SHA_FILES}"
    sha256sum "${SHA_FILES}" | cut -d" " -f1 > "${SHA256SUM_FILE}"

}

function cache::retrieve_cache_url() {
    CACHE_URL="${CACHE_URL_PREFIX}cache-$(cat "${SHA256SUM_FILE}").tgz"
    readonly CACHE_URL
}

# Download and unpack cache for this SHA if it exists. It succeeds even if the file does
# not exist in the cache but warning is printed
function cache::download_cache() {
    echo
    echo "Attempting to download pre-commit cache from ${CACHE_URL}"
    echo

    (${CACHE_UTILITY} cp "${CACHE_URL}" "${CACHE_FILE}" \
        && tar -C "${HOME}" -xzf "${CACHE_FILE}") || true
}

# Cache should be immutable. If we already downloaded it, we should not re-upload it
# This method succeeds even if there is no permission to write to the bucket
function cache::upload_cache() {
    if [[ ! -f "${CACHE_FILE}" ]]; then
        echo
        echo "Storing pre-commit cache in ${CACHE_URL}"
        echo
        (tar -C "${HOME}" -czf "${CACHE_FILE}" .cache && \
            ${CACHE_UTILITY} cp "${CACHE_FILE}" "${CACHE_URL}") || true
    fi
}
