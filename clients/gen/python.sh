#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${SCRIPT_DIR}/common.sh"

VERSION=1.0.0
python_config=(
    "packageVersion=${VERSION}"
    "projectName=airflow-client"
)

set -ex
IFS=','
gen_client python python --package-name airflow_client \
    --additional-properties "${python_config[*]}"
