#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

${SCRIPT_DIR}/gen/python.sh
${SCRIPT_DIR}/gen/go.sh
${SCRIPT_DIR}/gen/javascript.sh
${SCRIPT_DIR}/gen/java-native.sh
