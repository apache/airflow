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
set -euo pipefail

function prepare_tool_script() {
    IMAGE="${1}"
    VOLUME="${2}"
    TOOL="${3}"
    COMMAND="${4:-}"

    TARGET_TOOL_PATH="/usr/bin/${TOOL}"
    TARGET_TOOL_UPDATE_PATH="/usr/bin/${TOOL}-update"

    cat >"${TARGET_TOOL_PATH}" <<EOF
#!/usr/bin/env bash
DOCKER_ARGS=(
    -v "\${HOST_AIRFLOW_SOURCES}/tmp:/tmp" \\
    -v "\${HOST_AIRFLOW_SOURCES}/files:/files" \\
    -v "\${HOST_AIRFLOW_SOURCES}:/opt/airflow" \\
    -v "\${HOST_HOME}/${VOLUME}:/root/${VOLUME}" \\
    --interactive \\
)
if [ -t 0 ] ; then
    DOCKER_ARGS+=(
        --tty \\
    )
fi

docker run "\${DOCKER_ARGS[@]}" "${IMAGE}" "\${@}"

RES=\$?
if [[ \${HOST_OS} == "Linux" ]]; then
    docker run --rm \
        -v "\${HOST_AIRFLOW_SOURCES}/tmp:/tmp" \\
        -v "\${HOST_AIRFLOW_SOURCES}/files:/files" \\
        -v "\${HOST_HOME}/${VOLUME}:/root/${VOLUME}" \\
        "\${AIRFLOW_CI_IMAGE}" bash -c \\
        "find '/tmp/' '/files/' '/root/${VOLUME}' -user root -print0 | xargs --null chown '\${HOST_USER_ID}.\${HOST_GROUP_ID}' --no-dereference" >/dev/null 2>&1
fi
exit \${RES}
EOF

    cat >"${TARGET_TOOL_UPDATE_PATH}" <<EOF
#!/usr/bin/env bash
docker pull "${IMAGE}"
EOF

    chmod a+x "${TARGET_TOOL_PATH}" "${TARGET_TOOL_UPDATE_PATH}"
}

GCLOUD_IMAGE="gcr.io/google.com/cloudsdktool/cloud-sdk:latest"

prepare_tool_script "amazon/aws-cli:latest" ".aws" aws
prepare_tool_script "mcr.microsoft.com/azure-cli:latest" ".azure" az az

function prepare_gcloud_script() {
    IMAGE="${1}"
    TOOL="${2}"
    COMMAND="${2}"
    VOLUME=".config/gcloud"

    TARGET_TOOL_PATH="/usr/bin/${TOOL}"
    TARGET_TOOL_UPDATE_PATH="/usr/bin/${TOOL}-update"

    cat >"${TARGET_TOOL_PATH}" <<EOF
#!/usr/bin/env bash

DOCKER_ARGS=(
    --rm \\
    -v "\${HOST_AIRFLOW_SOURCES}/tmp:/tmp" \\
    -v "\${HOST_AIRFLOW_SOURCES}/files:/files" \\
    -v "\${HOST_AIRFLOW_SOURCES}:/opt/airflow" \\
    -v "\${HOST_HOME}/${VOLUME}:/root/${VOLUME}" \\
    -e GOOGLE_APPLICATION_CREDENTIALS \\
    -e CLOUDSDK_CONFIG \\
    --interactive \\
)
if [ -t 0 ] ; then
    DOCKER_ARGS+=(
        --tty \\
    )
fi

docker run "\${DOCKER_ARGS[@]}" "${IMAGE}" "${COMMAND}" "\${@}"

RES=\$?
if [[ \${HOST_OS} == "Linux" ]]; then
    docker run --rm \
        -v "\${HOST_AIRFLOW_SOURCES}/tmp:/tmp" \\
        -v "\${HOST_AIRFLOW_SOURCES}/files:/files" \\
        -v "\${HOST_HOME}/${VOLUME}:/root/${VOLUME}" \\
        "\${AIRFLOW_CI_IMAGE}" bash -c \\
        "find '/tmp/' '/files/' '/root/${VOLUME}' -user root -print0 | xargs --null chown '\${HOST_USER_ID}.\${HOST_GROUP_ID}' --no-dereference" >/dev/null 2>&1
fi
exit \${RES}
EOF

    cat >"${TARGET_TOOL_UPDATE_PATH}" <<EOF
#!/usr/bin/env bash
docker pull "${IMAGE}"
EOF

    chmod a+x "${TARGET_TOOL_PATH}" "${TARGET_TOOL_UPDATE_PATH}"
}

prepare_gcloud_script "${GCLOUD_IMAGE}" bq
prepare_gcloud_script "${GCLOUD_IMAGE}" gcloud
prepare_gcloud_script "${GCLOUD_IMAGE}" gsutil

function prepare_terraform_script() {
    TOOL="terraform"
    IMAGE="hashicorp/terraform:latest"

    TARGET_TOOL_PATH="/usr/bin/${TOOL}"
    TARGET_TOOL_UPDATE_PATH="/usr/bin/${TOOL}-update"

    cat >"${TARGET_TOOL_PATH}" <<EOF
#!/usr/bin/env bash

DOCKER_ARGS=(
    -v "\${HOST_AIRFLOW_SOURCES}/tmp:/tmp" \\
    -v "\${HOST_AIRFLOW_SOURCES}/files:/files" \\
    -v "\${HOST_AIRFLOW_SOURCES}:/opt/airflow" \\
    -v "\${HOST_HOME}/.aws:/root/.aws" \\
    -v "\${HOST_HOME}/.azure:/root/.azure" \\
    -v "\${HOST_HOME}/.config/gcloud:/root/.config/gcloud" \\
    -w /opt/airflow  \\
    --env-file <(env | grep TF) \\
    -e GOOGLE_APPLICATION_CREDENTIALS \\
    -e CLOUDSDK_CONFIG \\
)
if [ -t 0 ] ; then
    DOCKER_ARGS+=(
        --tty \\
    )
fi

docker run "\${DOCKER_ARGS[@]}" "${IMAGE}" "\$@"

RES=\$?
if [[ \${HOST_OS} == "Linux" ]]; then
    docker run --rm \
        -v "\${HOST_AIRFLOW_SOURCES}/tmp:/tmp" \\
        -v "\${HOST_AIRFLOW_SOURCES}/files:/files" \\
        -v "\${HOST_HOME}/.aws:/root/.aws" \\
        -v "\${HOST_HOME}/.azure:/root/.azure" \\
        -v "\${HOST_HOME}/.config/gcloud:/root/.config/gcloud" \\
        "\${AIRFLOW_CI_IMAGE}" bash -c \\
        "find '/tmp/' '/files/' '/root/.aws' '/root/.azure' '/root/.config/gcloud' -user root -print0 | xargs --null chown '\${HOST_USER_ID}.\${HOST_GROUP_ID}' --no-dereference" >/dev/null 2>&1
fi
exit \${RES}
EOF

    cat >"${TARGET_TOOL_UPDATE_PATH}" <<EOF
#!/usr/bin/env bash
docker pull "${IMAGE}"
EOF

    chmod a+x "${TARGET_TOOL_PATH}" "${TARGET_TOOL_UPDATE_PATH}"
}

prepare_terraform_script

function prepare_java_script() {
    # Apache Beam v2.14 doesn't support Java 11, so  I used the previous LTS version.
    IMAGE="openjdk:8-jre-slim"
    TOOL="java"
    JAVA_BIN="/usr/local/openjdk-8/bin/java"

    TARGET_TOOL_PATH="/usr/bin/${TOOL}"
    TARGET_TOOL_UPDATE_PATH="/usr/bin/${TOOL}-update"

    cat >"${TARGET_TOOL_PATH}" <<EOF
#!/usr/bin/env bash
DOCKER_ARGS=(
    --rm \\
    -v "\${HOST_AIRFLOW_SOURCES}/tmp/:/tmp/" \\
    -v "\${HOST_AIRFLOW_SOURCES}/files/:/files/" \\
    -v "\${HOST_AIRFLOW_SOURCES}:/opt/airflow" \\
    -v "\${HOST_HOME}/.config/gcloud:/root/.config/gcloud" \\
    -e GOOGLE_APPLICATION_CREDENTIALS \\
    -e CLOUDSDK_CONFIG \\
    --interactive \\
)
if [ -t 0 ] ; then
    DOCKER_ARGS+=(
        --tty \\
    )
fi

COMMAND="${JAVA_BIN} \$(printf "%q " "\$@")"

docker run "\${DOCKER_ARGS[@]}" "${IMAGE}" /bin/bash -c "ulimit -c unlimited; \${COMMAND}"

RES=\$?
if [[ \${HOST_OS} == "Linux" ]]; then
    docker run --rm \\
        -v "\${HOST_AIRFLOW_SOURCES}/tmp:/tmp" \\
        -v "\${HOST_AIRFLOW_SOURCES}/files:/files" \\
        -v "\${HOST_AIRFLOW_SOURCES}:/opt/airflow" \\
        -v "\${HOST_HOME}/.config/gcloud:/root/.config/gcloud" \\
        "\${AIRFLOW_CI_IMAGE}" bash -c \\
        "find '/tmp/' '/files/' '/root/${VOLUME}' -user root -print0 | xargs --null chown '\${HOST_USER_ID}.\${HOST_GROUP_ID}' --no-dereference" >/dev/null 2>&1
fi
exit \${RES}
EOF

    cat >"${TARGET_TOOL_UPDATE_PATH}" <<EOF
#!/usr/bin/env bash
docker pull "${IMAGE}"
EOF

    chmod a+x "${TARGET_TOOL_PATH}" "${TARGET_TOOL_UPDATE_PATH}"
}

prepare_java_script
