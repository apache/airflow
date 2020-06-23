#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${SCRIPT_DIR}/common.sh"

VERSION=1.0.0
go_config=(
    "packageVersion=${VERSION}"
    "enumClassPrefix=true"
)

set -ex
IFS=','
gen_client go go \
    --package-name airflow \
    --git-user-id apache \
    --git-repo-id airflow \
    --additional-properties "${go_config[*]}"
