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

if [ "$#" -ne 2 ]; then
    echo "USAGE: $0 SPEC_PATH OUTPUT_DIR"
    exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# shellcheck source=./clients/gen/common.sh
source "${SCRIPT_DIR}/common.sh"

VERSION=1.0.0
go_config=(
    "packageVersion=${VERSION}"
    "enumClassPrefix=true"
)

SPEC_PATH=$(realpath "$1")
if [ ! -d "$2" ]; then
    echo "$2 is not a valid directory or does not exist."
    exit 1
fi
OUTPUT_DIR=$(realpath "$2")

# create openapi ignore file to keep generated code clean
cat <<EOF > "${OUTPUT_DIR}/.openapi-generator-ignore"
.travis.yml
git_push.sh
EOF

set -ex
IFS=','

SPEC_PATH="${SPEC_PATH}" \
OUTPUT_DIR="${OUTPUT_DIR}" \
    gen_client go \
    --package-name airflow \
    --git-repo-id airflow-client-go/airflow \
    --additional-properties "${go_config[*]}"

# patch generated client to support problem HTTP API
# this patch can be removed after following upstream patch gets merged:
# https://github.com/OpenAPITools/openapi-generator/pull/6793
cd "${OUTPUT_DIR}" && patch -b <<'EOF'
--- client.go
+++ client.go
@@ -37,7 +37,7 @@ import (
 )

 var (
-	jsonCheck = regexp.MustCompile(`(?i:(?:application|text)/(?:vnd\.[^;]+\+)?json)`)
+	jsonCheck = regexp.MustCompile(`(?i:(?:application|text)/(?:vnd\.[^;]+\+)?(?:problem\+)?json)`)
	xmlCheck  = regexp.MustCompile(`(?i:(?:application|text)/xml)`)
 )
EOF

pushd "${OUTPUT_DIR}"
    # prepend license headers
    pre-commit run --all-files || true
popd
