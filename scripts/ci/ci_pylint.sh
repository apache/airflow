#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script to run Pylint on all code. Can be started from any working directory
# ./scripts/ci/ci_pylint.sh

set -uo pipefail

# Uncomment to see the commands executed
# set -x

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd ${MY_DIR}/../../ || exit 1

echo
echo "Running in $(pwd)"
echo

echo
echo "Running pylint for source code without tests"
echo

find . -name "*.py" \
-not -path "./.eggs/*" \
-not -path "./airflow/www/node_modules/*" \
-not -path "./airflow/_vendor/*" \
-not -path "./build/*" \
-not -path "./tests/*" \
-not -name 'webserver_config.py' | grep -vFf scripts/ci/pylint_todo.txt | xargs pylint --output-format=colorized
RES_MAIN=$?

echo
echo "Running pylint for tests"
echo

find . -name "*.py" -path './tests/*' | \
grep -vFf scripts/ci/pylint_todo.txt | \
xargs pylint --disable="
    missing-docstring,
    no-self-use,
    too-many-public-methods,
    protected-access
    " \
    --output-format=colorized
RES_TESTS=$?

popd || exit 1

if [[ "${RES_TESTS}" != 0 || "${RES_MAIN}" != 0 ]]; then
    echo
    echo "There were some pylint errors. Exiting"
    echo
    exit 1
else
    echo
    echo "Pylint check succeeded"
    echo
fi
