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

# Removes airflow CI and base images
function remove_all_images() {
    echo
    "${AIRFLOW_SOURCES}/confirm" "Removing all local images ."
    echo
    verbose_docker rmi "${PYTHON_BASE_IMAGE}" || true
    verbose_docker rmi "${AIRFLOW_CI_IMAGE}" || true
    echo
    echo "###################################################################"
    echo "NOTE!! Removed Airflow images for Python version ${PYTHON_MAJOR_MINOR_VERSION}."
    echo "       But the disk space in docker will be reclaimed only after"
    echo "       running 'docker system prune' command."
    echo "###################################################################"
    echo
}
