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
# This is an AMI that is based on Basic Amazon Linux AMI with installed and configured docker service
WORKING_DIR="/tmp/armdocker"
INSTANCE_INFO="${WORKING_DIR}/instance_info.json"
AUTOSSH_LOGFILE="${WORKING_DIR}/autossh.log"

function stop_arm_instance() {
    INSTANCE_ID=$(jq < "${INSTANCE_INFO}" ".Instances[0].InstanceId" -r)
    docker buildx rm --force airflow_cache || true
    aws ec2 terminate-instances --instance-ids "${INSTANCE_ID}"
    cat ${AUTOSSH_LOGFILE}

}

stop_arm_instance
