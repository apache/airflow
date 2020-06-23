#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${SCRIPT_DIR}/common.sh"

VERSION=1.0.0
java_config=(
    "packageVersion=${VERSION}"
    "enumClassPrefix=true"
    "hideGenerationTimestamp=true"
)

set -ex
IFS=','
gen_client java-native java --library native \
    --api-package org.apache.airflow.client.api --group-id org.apache.airflow \
    --artifact-id airflow-java-client \
    --additional-properties "${java_config[*]}"
