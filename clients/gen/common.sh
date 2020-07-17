#!/bin/bash

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

OPENAPI_GENERATOR_CLI_VER=4.3.1
GIT_USER=${GIT_USER:-apache}

function gen_client {
    lang=$1
    shift
    docker run --rm \
        -v "${SPEC_PATH}:/spec" \
        -v "${OUTPUT_DIR}:/output" \
        openapitools/openapi-generator-cli:v${OPENAPI_GENERATOR_CLI_VER} \
        generate \
        --input-spec "/spec" \
        --generator-name "${lang}" \
        --git-user-id "${GIT_USER}" \
        --output "/output" "$@"
}
