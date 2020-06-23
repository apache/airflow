#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${SCRIPT_DIR}/common.sh"

VERSION=1.0.0
javascript_config=(
    "packageVersion=${VERSION}"
)

set -ex
IFS=','
gen_client javascript javascript \
    --additional-properties "${javascript_config[*]}"
