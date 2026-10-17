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
#       Creates the snapshot after the builder publishes its caches.
#   docker_data_root_snapshot.sh restore SNAPSHOT_FILE
#       Restores the image. Incompatible snapshots leave the daemon untouched. Failed
#       materialization cleans partial image state and restarts Docker for the normal stash path.
set -euo pipefail

DATA_ROOT="/var/lib/docker"
DAEMON_STOPPED=false
RESTORE_IN_PROGRESS=false
RESTORE_FILE=""

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
    sudo systemctl stop docker.socket docker
}

function start_daemon() {
    sudo systemctl start docker
    DAEMON_STOPPED=false
}

function remove_image_store() {
    sudo rm -rf "${DATA_ROOT}/image" "${DATA_ROOT}/overlay2"
}

function create_snapshot() {
    local snapshot_file="${1}"
    local daemon image_id
    daemon="$(daemon_fingerprint)"
    check_supported_daemon "${daemon}"
    image_id="$(docker images --quiet --filter 'label=org.apache.airflow.image=airflow-ci' | sort -u)"
    if [[ -z "${image_id}" || "${image_id}" == *$'\n'* ]]; then
        echo "Expected exactly one CI image in the daemon, found: '${image_id}'" >&2
        exit 1
    fi
    docker ps --all --quiet | xargs --no-run-if-empty docker rm --force >/dev/null
    docker images --quiet | sort -u | {
        grep --invert-match --fixed-strings --line-regexp "${image_id}" || true
    } | xargs --no-run-if-empty docker rmi --force >/dev/null
    docker builder prune --all --force >/dev/null
    printf '%s %s %s\n' "${daemon}" "${image_id}" "$(git rev-parse HEAD)" \
        > "${snapshot_file}.meta"
    stop_daemon
    sudo tar --directory "${DATA_ROOT}" --xattrs --acls --numeric-owner \
        --create --file - image overlay2 \
        | zstd -3 -T0 --quiet --force -o "${snapshot_file}"
    start_daemon
}

function restore_snapshot() {
    local snapshot_file="${1}"
    RESTORE_FILE="${snapshot_file}"
    local -a meta
    local image_id snapshot_fingerprint daemon existing_images existing_containers
    if [[ ! -f "${snapshot_file}" || ! -f "${snapshot_file}.meta" ]]; then
        echo "No snapshot at ${snapshot_file}"
        exit 2
    fi
    read -r -a meta < "${snapshot_file}.meta"
    if [[ ${#meta[@]} != 6 ]]; then
        echo "Invalid snapshot metadata"
        exit 3
    fi
    if [[ "${meta[5]}" != "$(git rev-parse HEAD)" ]]; then
        echo "Snapshot belongs to a different checkout revision"
        exit 3
    fi
    image_id="${meta[4]}"
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
    stop_daemon
    RESTORE_IN_PROGRESS=true
    remove_image_store
    zstd -d -T0 --quiet --stdout "${snapshot_file}" \
        | sudo tar --directory "${DATA_ROOT}" --xattrs --acls --numeric-owner --extract --file -
    start_daemon
    if ! docker run --rm --entrypoint /bin/bash "${image_id}" -c true; then
        echo "The restored image ${image_id} does not run"
        exit 4
    fi
    RESTORE_IN_PROGRESS=false
}

case "${1:-}" in
    create)
        create_snapshot "${2}"
        ;;
    restore)
        restore_snapshot "${2}"
        ;;
    *)
        echo "Usage: ${0} {create|restore} SNAPSHOT_FILE" >&2
        exit 1
        ;;
esac
