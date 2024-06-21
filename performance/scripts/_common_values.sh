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
# shellcheck disable=SC2034

# Determine the project used - if not passed by the PROJECT_ID value, it is retrieved from current project
PROJECT_ID=${PROJECT_ID:=$(gcloud config get-value "core/project")}
readonly PROJECT_ID

# Registry where all images are stored - for caching purpose and to store the final images
# By default it is stored in GCR in the project registry
# Your service account for CI builds has to have write/read permission to that registry
CONTAINER_REGISTRY_URL=${CONTAINER_REGISTRY_URL:="gcr.io/${PROJECT_ID}"}
readonly CONTAINER_REGISTRY_URL
