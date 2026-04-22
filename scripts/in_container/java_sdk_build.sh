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

# Builds Java SDK bundles (stub + pure Java) from the java-sdk source tree.
#
# Creates two bundle directories under BUNDLES_OUTPUT_DIR:
#   python-stub-java-tasks/
#     dags/        - Python stub DAGs with Java task stubs
#     jar-bundles/ - compiled JARs for the Java coordinator
#   pure-java/     - compiled JARs only (DAG defined entirely in Java)
#
# Each bundle gets a distinct DAG ID so both can run simultaneously.
#
# Usage:
#   export JAVA_SDK_SRC_DIR=/path/to/java-sdk
#   export BUNDLES_OUTPUT_DIR=/path/to/output
#   bash java_sdk_build.sh
#
# After running, source the generated env file to configure Airflow:
#   source ${BUNDLES_OUTPUT_DIR}/java_sdk_env.sh
#
# Required environment variables:
#   JAVA_SDK_SRC_DIR    - path to the java-sdk source tree
#   BUNDLES_OUTPUT_DIR  - directory where bundle dirs will be created
#
# Optional environment variables:
#   JAVA_STUB_BUNDLE_NAME  - stub bundle directory name (default: python-stub-java-tasks)
#   JAVA_PURE_BUNDLE_NAME  - pure Java bundle directory name (default: pure-java)
#   JAVA_STUB_DAG_ID       - DAG ID for the stub bundle (default: java_sdk_stub_example)
#   JAVA_PURE_DAG_ID       - DAG ID for the pure Java bundle (default: java_sdk_pure_java_example)
#   JAVA_SDK_DAGS_DIR      - path to Python stub DAG files (default: JAVA_SDK_SRC_DIR/dags)

set -euo pipefail

COLOR_BLUE='\033[0;34m'
COLOR_GREEN='\033[0;32m'
COLOR_RESET='\033[0m'

# Required
JAVA_SDK_SRC_DIR="${JAVA_SDK_SRC_DIR:?JAVA_SDK_SRC_DIR must be set}"
BUNDLES_OUTPUT_DIR="${BUNDLES_OUTPUT_DIR:?BUNDLES_OUTPUT_DIR must be set}"

# Optional with defaults
JAVA_STUB_BUNDLE_NAME="${JAVA_STUB_BUNDLE_NAME:-python-stub-java-tasks}"
JAVA_PURE_BUNDLE_NAME="${JAVA_PURE_BUNDLE_NAME:-pure-java}"
JAVA_STUB_DAG_ID="${JAVA_STUB_DAG_ID:-java_sdk_stub_example}"
JAVA_PURE_DAG_ID="${JAVA_PURE_DAG_ID:-java_sdk_pure_java_example}"
JAVA_SDK_DAGS_DIR="${JAVA_SDK_DAGS_DIR:-${JAVA_SDK_SRC_DIR}/dags}"

WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT

echo -e "${COLOR_BLUE}Building Java SDK bundles...${COLOR_RESET}"
echo "  Source:  ${JAVA_SDK_SRC_DIR}"
echo "  Output:  ${BUNDLES_OUTPUT_DIR}"

# Portable sed -i (macOS requires a backup extension, Linux does not)
sed_inplace() {
    if [[ "$(uname)" == "Darwin" ]]; then
        sed -i '' "$@"
    else
        sed -i "$@"
    fi
}

# Copy java-sdk source excluding build artifacts
copy_sdk_source() {
    local dest="$1"
    mkdir -p "${dest}"
    tar -cf - -C "${JAVA_SDK_SRC_DIR}" \
        --exclude='.gradle' --exclude='build' --exclude='.kotlin' . \
        | tar -xf - -C "${dest}"
}

# Replace the DAG ID in JavaExample.java
set_java_dag_id() {
    local sdk_dir="$1"
    local dag_id="$2"
    local java_file="${sdk_dir}/example/src/java/org/apache/airflow/example/JavaExample.java"
    sed_inplace "s/\"java_example\"/\"${dag_id}\"/" "${java_file}"
    echo "  Set JavaExample.java dag_id -> '${dag_id}'"
}

# Build the Java SDK example project with Gradle
build_example() {
    local sdk_dir="$1"
    local gradlew="${sdk_dir}/gradlew"

    if [[ ! -x "${gradlew}" ]]; then
        chmod +x "${gradlew}"
    fi

    echo -e "${COLOR_BLUE}  Running spotlessApply...${COLOR_RESET}"
    (cd "${sdk_dir}" && ./gradlew spotlessApply --no-configuration-cache -q)

    echo -e "${COLOR_BLUE}  Running :example:installDist...${COLOR_RESET}"
    (cd "${sdk_dir}" && ./gradlew clean :example:installDist -x test --no-configuration-cache -q)
}

# Path to built JARs after installDist
jars_dir() {
    echo "$1/example/build/install/example/lib"
}

mkdir -p "${BUNDLES_OUTPUT_DIR}"

# ---- Build stub bundle ----
echo -e "\n${COLOR_BLUE}=== Building stub bundle (${JAVA_STUB_BUNDLE_NAME}) ===${COLOR_RESET}"
STUB_COPY="${WORK_DIR}/java-sdk-stub"
copy_sdk_source "${STUB_COPY}"
set_java_dag_id "${STUB_COPY}" "${JAVA_STUB_DAG_ID}"
build_example "${STUB_COPY}"

STUB_BUNDLE="${BUNDLES_OUTPUT_DIR}/${JAVA_STUB_BUNDLE_NAME}"
mkdir -p "${STUB_BUNDLE}/jar-bundles" "${STUB_BUNDLE}/dags"
cp "$(jars_dir "${STUB_COPY}")"/*.jar "${STUB_BUNDLE}/jar-bundles/"

# Copy and patch stub DAGs
cp "${JAVA_SDK_DAGS_DIR}"/*.py "${STUB_BUNDLE}/dags/"
sed_inplace "s/\"java_example\"/\"${JAVA_STUB_DAG_ID}\"/" "${STUB_BUNDLE}/dags/stub_dag.py"
echo "  Set stub_dag.py dag_id -> '${JAVA_STUB_DAG_ID}'"

STUB_JAR_COUNT=$(find "${STUB_BUNDLE}/jar-bundles" -name '*.jar' | wc -l | tr -d ' ')
echo -e "${COLOR_GREEN}  Stub bundle: ${STUB_JAR_COUNT} JARs + DAGs${COLOR_RESET}"

# ---- Build pure Java bundle ----
echo -e "\n${COLOR_BLUE}=== Building pure Java bundle (${JAVA_PURE_BUNDLE_NAME}) ===${COLOR_RESET}"
PURE_COPY="${WORK_DIR}/java-sdk-pure"
copy_sdk_source "${PURE_COPY}"
set_java_dag_id "${PURE_COPY}" "${JAVA_PURE_DAG_ID}"
build_example "${PURE_COPY}"

PURE_BUNDLE="${BUNDLES_OUTPUT_DIR}/${JAVA_PURE_BUNDLE_NAME}"
mkdir -p "${PURE_BUNDLE}"
cp "$(jars_dir "${PURE_COPY}")"/*.jar "${PURE_BUNDLE}/"

PURE_JAR_COUNT=$(find "${PURE_BUNDLE}" -name '*.jar' | wc -l | tr -d ' ')
echo -e "${COLOR_GREEN}  Pure Java bundle: ${PURE_JAR_COUNT} JARs${COLOR_RESET}"

# ---- Generate env file ----
# This file can be sourced to configure Airflow with both DAG bundles.
# For breeze (in-container), source it directly.
# For e2e tests, the caller sets container paths separately via docker-compose.
ENV_FILE="${BUNDLES_OUTPUT_DIR}/java_sdk_env.sh"
cat > "${ENV_FILE}" <<ENVEOF
# Generated by java_sdk_build.sh - source this to configure Airflow for Java SDK bundles
export AIRFLOW__JAVA__BUNDLES_FOLDER="${STUB_BUNDLE}/jar-bundles"
export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[{"name": "${JAVA_STUB_BUNDLE_NAME}", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {"path": "${STUB_BUNDLE}/dags", "refresh_interval": 20}}, {"name": "${JAVA_PURE_BUNDLE_NAME}", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {"path": "${PURE_BUNDLE}", "refresh_interval": 20}}]'
# The necessary connections and variables for the Java SDK example DAGs are included here for convenience, but can also be set separately via the Airflow UI or CLI.
export AIRFLOW_CONN_TEST_HTTP='{"conn_type": "http", "login": "user", "password": "pass", "host": "example.com", "port": 1234, "extra": {"param1": "val1", "param2": "val2"}}'
export AIRFLOW_VAR_MY_VARIABLE=123
ENVEOF

echo -e "\n${COLOR_GREEN}Java SDK build complete!${COLOR_RESET}"
echo "  Stub bundle: ${STUB_BUNDLE}"
echo "  Pure bundle: ${PURE_BUNDLE}"
echo "  Env file:    ${ENV_FILE}"
