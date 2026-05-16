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
cp -v ./files/constraints-*/constraints*.txt constraints/
cd constraints || exit 1

set +e
git diff --color --exit-code --ignore-matching-lines="^#.*"
diff_status=$?
set -e

if [[ ${diff_status} -eq 0 ]]; then
echo "No changes in constraints"
elif [[ ${diff_status} -eq 1 ]]; then
echo "Changes detected in constraints, proceeding..."
else
echo "Failed to diff constraints"
exit "${diff_status}"
fi
