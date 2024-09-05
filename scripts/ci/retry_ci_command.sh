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

# This script should be only used commands without waiting interactive inputs.
# For example: 'apt-get install [package_name]' will ask you to input 'y' while installing.
# This means you cannot interactively take input while using this script.
COMMAND_TO_RETRY=$1
NUMBER_OF_RETRY=$2
NUMBER_OF_RETRY="${NUMBER_OF_RETRY:-1}"
CURRENT_RETRY=1
SUCCESS=false

while [ "$SUCCESS" = false ] && [ "$CURRENT_RETRY" -le "$NUMBER_OF_RETRY" ]; do
  if CURRENT_OUTPUT=$($COMMAND_TO_RETRY); then
    SUCCESS=true
  else
    echo "Current # of Attempt: $CURRENT_RETRY. Trying again..."
    CURRENT_RETRY=$(( CURRENT_RETRY + 1 ))
  fi
  echo "$CURRENT_OUTPUT"
done

