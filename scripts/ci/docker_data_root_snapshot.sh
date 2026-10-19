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

# Moves the CI image between jobs as a copy of the Docker data directory that holds it, so a job
# restores it with one extraction instead of `docker image load` unpacking and checksumming every
# layer again.
#
#   docker_data_root_snapshot.sh create SNAPSHOT_FILE
#       Creates the snapshot after the builder publishes its caches. SNAPSHOT_FILE.meta reads
#       "unavailable" until the archive is complete, so a failed rerun retires an earlier snapshot.
#   docker_data_root_snapshot.sh check SNAPSHOT_FILE
#       Checks SNAPSHOT_FILE.meta against this checkout and daemon before the archive is downloaded.
#   docker_data_root_snapshot.sh restore SNAPSHOT_FILE
#       Restores the image. Incompatible snapshots leave the daemon untouched. Failed or overdue
#       materialization cleans partial image state and restarts Docker for the normal stash path.
set -euo pipefail

DATA_ROOT="/var/lib/docker"
# overlay2 records opaque directories in trusted.* attributes and file capabilities in security.*;
# GNU tar extracts only user.* attributes unless told otherwise.
TAR_ATTRIBUTES=(--xattrs "--xattrs-include=*" --acls --numeric-owner)
# Every blocking step is bounded, so a stalled restore fails fast instead of holding the job.
RESTORE_TIMEOUT_SECONDS="${SNAPSHOT_RESTORE_TIMEOUT_SECONDS:-300}"
DAEMON_TIMEOUT_SECONDS=60
DAEMON_STOPPED=false
RESTORE_IN_PROGRESS=false
RESTORE_FILE=""
IMAGE_ID=""
EXPECTED_IMAGE=""

function cleanup_daemon() {
    local result=$?
    trap - EXIT
    if [[ -n "${RESTORE_FILE}" ]]; then
        rm -f "${RESTORE_FILE}" "${RESTORE_FILE}.meta"
    fi
    if [[ "${RESTORE_IN_PROGRESS}" == true ]]; then
        stop_daemon
        remove_image_store
        start_daemon
    elif [[ "${DAEMON_STOPPED}" == true ]]; then
        start_daemon
    fi
    exit "${result}"
}
trap cleanup_daemon EXIT

function check_supported_daemon() {
    local daemon=$1
    local _ driver root
    read -r _ driver _ root <<< "${daemon}"
    if [[ "${driver}" != overlay2 || "${root}" != "${DATA_ROOT}" ]]; then
        echo "::warning::Unsupported Docker image store; skipping snapshot: ${daemon}"
        exit 3
    fi
}

# A data directory is only readable by the daemon version and storage driver that wrote it.
function daemon_fingerprint() {
    docker info --format '{{.ServerVersion}} {{.Driver}} {{.Architecture}} {{.DockerRootDir}}'
}

function stop_daemon() {
    DAEMON_STOPPED=true
    sudo timeout "${DAEMON_TIMEOUT_SECONDS}" systemctl stop docker.socket docker
}

function start_daemon() {
    sudo timeout "${DAEMON_TIMEOUT_SECONDS}" systemctl start docker
    DAEMON_STOPPED=false
}

# Identifies the image by its ID and by the file tree its layers combine into: a restore that loses
# a whiteout or an opaque-directory marker changes the tree while the ID stays the same. The listing
# is sorted on the host, since sorting it in the container would add sort's temporary files to it.
function image_fingerprint() {
    local image=$1
    local image_id tree
    image_id="$(docker image inspect --format '{{.Id}}' "${image}")" || return 1
    tree="$(timeout "${DAEMON_TIMEOUT_SECONDS}" docker run --rm --pull never --network none \
        --entrypoint find "${image}" / -xdev -printf '%p %y %m\n' | LC_ALL=C sort | sha256sum)" \
        || return 1
    echo "${image_id} ${tree%% *}"
}

function remove_image_store() {
    sudo rm -rf "${DATA_ROOT}/image" "${DATA_ROOT}/overlay2"
}

function create_snapshot() {
    local snapshot_file="${1}"
    local daemon image_id revision fingerprint image_fingerprint_id image_tree
    rm -f "${snapshot_file}"
    echo "unavailable" > "${snapshot_file}.meta"
    daemon="$(daemon_fingerprint)"
    check_supported_daemon "${daemon}"
    image_id="$(docker images --quiet --filter 'label=org.apache.airflow.image=airflow-ci' | sort -u)"
    if [[ -z "${image_id}" || "${image_id}" == *$'\n'* ]]; then
        echo "Expected exactly one CI image in the daemon, found: '${image_id}'" >&2
        exit 1
    fi
    revision="$(git rev-parse HEAD)"
    fingerprint="$(image_fingerprint "${image_id}")"
    read -r image_fingerprint_id image_tree <<< "${fingerprint}"
    docker ps --all --quiet | xargs --no-run-if-empty docker rm --force >/dev/null
    docker images --quiet | sort -u | {
        grep --invert-match --fixed-strings --line-regexp "${image_id}" || true
    } | xargs --no-run-if-empty docker rmi --force >/dev/null
    docker builder prune --all --force >/dev/null
    stop_daemon
    sudo tar --directory "${DATA_ROOT}" "${TAR_ATTRIBUTES[@]}" --create --file - image overlay2 \
        | zstd -3 -T0 --quiet --force -o "${snapshot_file}"
    printf '%s %s %s %s\n' "${daemon}" "${image_fingerprint_id}" "${revision}" "${image_tree}" \
        > "${snapshot_file}.meta"
    start_daemon
}

# Exits without touching the daemon unless the snapshot was made from this checkout by a daemon
# like this one, and this daemon holds nothing the restore would drop.
function check_snapshot() {
    local snapshot_file="${1}"
    local -a meta
    local snapshot_fingerprint daemon existing_images existing_containers
    if [[ ! -f "${snapshot_file}.meta" ]]; then
        echo "No snapshot at ${snapshot_file}"
        exit 2
    fi
    read -r -a meta < "${snapshot_file}.meta"
    if [[ ${#meta[@]} != 7 ]]; then
        echo "Snapshot unavailable or its metadata is invalid"
        exit 3
    fi
    if [[ "${meta[5]}" != "$(git rev-parse HEAD)" ]]; then
        echo "Snapshot belongs to a different checkout revision"
        exit 3
    fi
    snapshot_fingerprint="${meta[*]:0:4}"
    daemon="$(daemon_fingerprint)"
    check_supported_daemon "${daemon}"
    if [[ "${snapshot_fingerprint}" != "${daemon}" ]]; then
        echo "::warning::Snapshot daemon fingerprint mismatch: '${snapshot_fingerprint}' != '${daemon}'"
        exit 3
    fi
    existing_images="$(docker images --all --quiet)"
    existing_containers="$(docker ps --all --quiet)"
    if [[ -n "${existing_images}" || -n "${existing_containers}" ]]; then
        echo "The daemon already holds images, which restoring the snapshot would drop"
        exit 3
    fi
    IMAGE_ID="${meta[4]}"
    EXPECTED_IMAGE="${meta[4]} ${meta[6]}"
}

function restore_snapshot() {
    local snapshot_file="${1}"
    local restored_image
    RESTORE_FILE="${snapshot_file}"
    if [[ ! -f "${snapshot_file}" ]]; then
        echo "No snapshot at ${snapshot_file}"
        exit 2
    fi
    check_snapshot "${snapshot_file}"
    stop_daemon
    RESTORE_IN_PROGRESS=true
    remove_image_store
    timeout "${RESTORE_TIMEOUT_SECONDS}" zstd -d -T0 --quiet --stdout "${snapshot_file}" \
        | sudo timeout "${RESTORE_TIMEOUT_SECONDS}" \
            tar --directory "${DATA_ROOT}" "${TAR_ATTRIBUTES[@]}" --extract --file -
    start_daemon
    if ! restored_image="$(image_fingerprint "${IMAGE_ID}")" \
            || [[ "${restored_image}" != "${EXPECTED_IMAGE}" ]]; then
        echo "The restored image does not match the snapshot: '${restored_image:-}' != '${EXPECTED_IMAGE}'"
        exit 4
    fi
    RESTORE_IN_PROGRESS=false
}

case "${1:-}" in
    create)
        create_snapshot "${2}"
        ;;
    check)
        check_snapshot "${2}"
        ;;
    restore)
        restore_snapshot "${2}"
        ;;
    *)
        echo "Usage: ${0} {create|check|restore} SNAPSHOT_FILE" >&2
        exit 1
        ;;
esac
