#!/usr/bin/env bash

#
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

set -e

FWDIR="$(cd "`dirname "$0"`"; pwd)"
cd "$FWDIR"

[ -d _build ] && rm -r _build

NUM_IGNORED_WARNINGS=4
NUM_CURRENT_WARNINGS=$(make html |\
    tee /dev/tty |\
    grep 'build succeeded' |\
    head -1 |\
    sed -E 's/build succeeded, ([0-9]+) warnings\./\1/g')

if [ "${NUM_CURRENT_WARNINGS}" != "${NUM_IGNORED_WARNINGS}" ]; then
    echo
    echo "Unexpected problems found in the documentation. "
    echo "Currently, ${NUM_IGNORED_WARNINGS} warnings are ignored."
    echo
    exit 1
fi
