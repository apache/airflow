#!/bin/bash
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
rm -rf docker-context-files/*.whl
rm -rf docker-context-files/*.tgz
export FORCE_ANSWER_TO_QUESTIONS="true"
export CI="true"

if [[ $1 == "" ]]; then
  echo
  echo ERROR! Please specify python version as parameter
  echo
  exit 1
fi

python_version=$1

./breeze prepare-build-cache --python "${python_version}" --verbose
./breeze prepare-build-cache --python "${python_version}" --production-image --verbose
