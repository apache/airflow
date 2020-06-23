#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SPEC_DIR="${SCRIPT_DIR}/../../airflow/api_connexion/openapi"
OUTPUT_DIR="${SCRIPT_DIR}/.."

OPENAPI_GENERATOR_CLI_VER=4.3.1

function gen_client {
    client_type=$1
    shift
    lang=$1
    shift
    docker run --rm \
        -v "${SPEC_DIR}/v1.yaml:/spec" \
        -v "${OUTPUT_DIR}/${client_type}:/${client_type}" \
        openapitools/openapi-generator-cli:v${OPENAPI_GENERATOR_CLI_VER} \
        generate \
        --input-spec "/spec" \
        --generator-name "${lang}" \
        --output "/${client_type}" "$@"
}

