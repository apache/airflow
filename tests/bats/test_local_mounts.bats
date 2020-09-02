#!/usr/bin/env bats


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

@test "convert volume list to docker params" {
  load bats_utils

  read -r -a RES <<< "$(convert_local_mounts_to_docker_params)"

  [[ ${#RES[@]} -gt 0 ]] # Array should be non-zero length
  [[ $((${#RES[@]} % 2)) == 0 ]] # Array should be even length

  for i in "${!RES[@]}"; do
    if [[ $((i % 2)) == 0 ]]; then
      # Every other value should be `-v`
      [[ ${RES[$i]} == "-v" ]]
    else
      # And the options should be of form <src>:<dest>:cached
      [[ ${RES[$i]} = *:*:cached ]]
    fi
  done
}
