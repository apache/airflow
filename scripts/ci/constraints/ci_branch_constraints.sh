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
if [[ ${GITHUB_REF} == 'refs/heads/main' ]]; then
  echo "::set-output name=branch::constraints-main"
elif [[ ${GITHUB_REF} =~ refs/heads/v([0-9\-]*)\-(test|stable) ]]; then
  echo "::set-output name=branch::constraints-${BASH_REMATCH[1]}"
else
  # Assume PR to constraints-main here
  echo
  echo "[${COLOR_YELLOW}Assuming that the PR is to 'main' branch!${COLOR_RESET}"
  echo
  echo "::set-output name=branch::constraints-main"
fi
