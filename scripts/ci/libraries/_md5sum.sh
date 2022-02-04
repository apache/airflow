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

declare -a MODIFIED_FILES
#
# Verifies if stored md5sum of the file changed since the last time it was checked
# The md5sum files are stored in .build directory - you can delete this directory
# If you want to rebuild everything from the scratch
#
function md5sum::calculate_file_md5sum {
    local file="${1}"
    local md5sum
    local md5sum_cache_dir="${BUILD_CACHE_DIR}/${BRANCH_NAME}/${PYTHON_MAJOR_MINOR_VERSION}/${THE_IMAGE_TYPE}"
    mkdir -pv "${md5sum_cache_dir}"
    md5sum=$(md5sum "${file}")
    local md5sum_file
    md5sum_file="${md5sum_cache_dir}"/$(basename "$(dirname "${file}")")-$(basename "${file}").md5sum
    local md5sum_file_new
    md5sum_file_new=${CACHE_TMP_FILE_DIR}/$(basename "$(dirname "${file}")")-$(basename "${file}").md5sum.new
    echo "${md5sum}" > "${md5sum_file_new}"
    local ret_code=0
    if [[ ! -f "${md5sum_file}" ]]; then
        verbosity::print_info "Missing md5sum for ${file#${AIRFLOW_SOURCES}} (${md5sum_file#${AIRFLOW_SOURCES}})"
        ret_code=1
    else
        diff "${md5sum_file_new}" "${md5sum_file}" >/dev/null
        local res=$?
        if [[ "${res}" != "0" ]]; then
            verbosity::print_info "The md5sum changed for ${file}: was $(cat "${md5sum_file}") now it is $(cat "${md5sum_file_new}")"
            if [[ ${CI} == "true" ]]; then
                echo "${COLOR_RED}The file has changed: ${file}${COLOR_RESET}"
                echo "${COLOR_BLUE}==============================${COLOR_RESET}"
                cat "${file}"
                echo "${COLOR_BLUE}==============================${COLOR_RESET}"
            fi
            ret_code=1
        fi
    fi
    return ${ret_code}
}

#
# Moves md5sum file from it's temporary location in CACHE_TMP_FILE_DIR to
# BUILD_CACHE_DIR - thus updating stored MD5 sum for the file
#
function md5sum::move_file_md5sum {
    local file="${1}"
    local md5sum_file
    local md5sum_cache_dir="${BUILD_CACHE_DIR}/${BRANCH_NAME}/${PYTHON_MAJOR_MINOR_VERSION}/${THE_IMAGE_TYPE}"
    mkdir -pv "${md5sum_cache_dir}"
    md5sum_file="${md5sum_cache_dir}"/$(basename "$(dirname "${file}")")-$(basename "${file}").md5sum
    local md5sum_file_new
    md5sum_file_new=${CACHE_TMP_FILE_DIR}/$(basename "$(dirname "${file}")")-$(basename "${file}").md5sum.new
    if [[ -f "${md5sum_file_new}" ]]; then
        mv "${md5sum_file_new}" "${md5sum_file}"
        verbosity::print_info "Updated md5sum file ${md5sum_file} for ${file}: $(cat "${md5sum_file}")"
    fi
}

#
# Stores md5sum files for all important files and
# records that we built the images locally so that next time we use
# it from the local docker cache rather than pull (unless forced)
#
function md5sum::update_all_md5() {
    verbosity::print_info
    verbosity::print_info "Updating md5sum files"
    verbosity::print_info
    for file in "${FILES_FOR_REBUILD_CHECK[@]}"
    do
        md5sum::move_file_md5sum "${AIRFLOW_SOURCES}/${file}"
    done
    mkdir -pv "${BUILD_CACHE_DIR}/${BRANCH_NAME}"
    touch "${BUILT_CI_IMAGE_FLAG_FILE}"
}

function md5sum::update_all_md5_with_group() {
    start_end::group_start "Update MD5 hashes for pulled images"
    md5sum::update_all_md5
    start_end::group_end
}

function md5sum::calculate_md5sum_for_all_files() {
    FILES_MODIFIED="false"
    set +e
    for file in "${FILES_FOR_REBUILD_CHECK[@]}"
    do
        if ! md5sum::calculate_file_md5sum "${AIRFLOW_SOURCES}/${file}"; then
            FILES_MODIFIED="true"
            MODIFIED_FILES+=( "${file}" )
        fi
    done
    set -e
}

#
# Checks md5sum of all important files in order to optimise speed of running various operations
# That mount sources of Airflow to container and require docker image built with latest dependencies.
# the Docker image will only be marked for rebuilding only in case any of the important files change:
# * setup.py
# * setup.cfg
# * Dockerfile.ci
#
# This is needed because we want to skip rebuilding of the image when only airflow sources change but
# Trigger rebuild in case we need to change dependencies (setup.py, setup.cfg, change version of Airflow
# or the Dockerfile.ci itself changes.
#
# Another reason to skip rebuilding Docker is thar currently it takes a bit longer time than simple Docker
# We need to fix group permissions of files in Docker because different linux build services have
# different default umask and Docker uses group permissions in checking for cache invalidation.
#
# As result of this check - most of the static checks will start pretty much immediately.
#
function md5sum::check_if_docker_build_is_needed() {
    verbosity::print_info
    verbosity::print_info "Checking if image build is needed for ${THE_IMAGE_TYPE} image."
    verbosity::print_info
    if [[ ${FORCE_BUILD_IMAGES:=""} == "true" ]]; then
        verbosity::print_info
        verbosity::print_info "${COLOR_YELLOW}Docker image build is forced for ${THE_IMAGE_TYPE} image${COLOR_RESET}"
        verbosity::print_info
        md5sum::calculate_md5sum_for_all_files
        needs_docker_build="true"
    else
        md5sum::calculate_md5sum_for_all_files
        if [[ ${FILES_MODIFIED} == "true" ]]; then
            needs_docker_build="true"
        fi
        if [[ ${needs_docker_build} == "true" ]]; then
            verbosity::print_info
            verbosity::print_info "${COLOR_YELLOW}The files were modified and likely the ${THE_IMAGE_TYPE} image needs rebuild: ${MODIFIED_FILES[*]}${COLOR_RESET}"
            verbosity::print_info
        else
            verbosity::print_info
            verbosity::print_info "${COLOR_GREEN}Docker image build is not needed for ${THE_IMAGE_TYPE} image!${COLOR_RESET}"
            verbosity::print_info
        fi
    fi
}
