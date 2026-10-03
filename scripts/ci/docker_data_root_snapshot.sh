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
#       Keeps only the CI image in the daemon and writes its image store to SNAPSHOT_FILE, plus
#       SNAPSHOT_FILE.meta naming the daemon that wrote it and the image.
#   docker_data_root_snapshot.sh restore SNAPSHOT_FILE
#       Restores the image. Exits non-zero, leaving the daemon running and without images, when
#       the snapshot is missing or was written by a different daemon, or the daemon already holds
#       images, so the caller can fall back to `docker image load`.
set -euo pipefail

DATA_ROOT="/var/lib/docker"

# A data directory is only readable by the daemon version and storage driver that wrote it.
function daemon_fingerprint() {
    docker info --format '{{.ServerVersion}} {{.Driver}} {{.Architecture}} {{.DockerRootDir}}'
}

function stop_daemon() {
    sudo systemctl stop docker.socket docker
}

function start_daemon() {
    sudo systemctl start docker
}

function remove_image_store() {
    sudo rm -rf "${DATA_ROOT}/image" "${DATA_ROOT}/overlay2"
}

function create_snapshot() {
    local snapshot_file="${1}"
    local image_id
    image_id="$(docker images --quiet --filter "label=org.apache.airflow.image=airflow-ci" | sort -u)"
    if [[ -z "${image_id}" || "${image_id}" == *$'\n'* ]]; then
        echo "Expected exactly one CI image in the daemon, found: '${image_id}'" >&2
        exit 1
    fi
    docker ps --all --quiet | xargs --no-run-if-empty docker rm --force >/dev/null
    docker images --quiet | sort -u | { grep --invert-match --fixed-strings --line-regexp "${image_id}" || true; } \
        | xargs --no-run-if-empty docker rmi --force >/dev/null
    docker builder prune --all --force >/dev/null
    printf '%s %s\n' "$(daemon_fingerprint)" "${image_id}" > "${snapshot_file}.meta"
    stop_daemon
    sudo tar --directory "${DATA_ROOT}" --xattrs --acls --numeric-owner --create --file - image overlay2 \
        | zstd -3 -T0 --quiet --force -o "${snapshot_file}"
    start_daemon
}

function restore_snapshot() {
    local snapshot_file="${1}"
    local -a meta
    local image_id snapshot_fingerprint daemon
    if [[ ! -f "${snapshot_file}" || ! -f "${snapshot_file}.meta" ]]; then
        echo "No snapshot at ${snapshot_file}"
        exit 2
    fi
    read -r -a meta < "${snapshot_file}.meta"
    image_id="${meta[${#meta[@]}-1]}"
    snapshot_fingerprint="${meta[*]:0:${#meta[@]}-1}"
    daemon="$(daemon_fingerprint)"
    if [[ "${snapshot_fingerprint}" != "${daemon}" ]]; then
        echo "The snapshot was written by '${snapshot_fingerprint}', this daemon is '${daemon}'"
        exit 3
    fi
    if [[ -n "$(docker images --all --quiet)" ]]; then
        echo "The daemon already holds images, which restoring the snapshot would drop"
        exit 3
    fi
    stop_daemon
    remove_image_store
    zstd -d -T0 --quiet --stdout "${snapshot_file}" \
        | sudo tar --directory "${DATA_ROOT}" --xattrs --acls --numeric-owner --extract --file -
    start_daemon
    if ! docker run --rm --entrypoint /bin/bash "${image_id}" -c true; then
        echo "The restored image ${image_id} does not run"
        stop_daemon
        remove_image_store
        start_daemon
        exit 4
    fi
    rm -f "${snapshot_file}" "${snapshot_file}.meta"
}

case "${1:-}" in
    create)
        create_snapshot "${2}"
        ;;
    restore)
        restore_snapshot "${2}"
        ;;
    *)
        echo "Usage: ${0} create|restore SNAPSHOT_FILE" >&2
        exit 1
        ;;
esac
