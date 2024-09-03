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
SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
# This is an AMI that is based on Basic Amazon Linux AMI with installed and configured docker service
WORKING_DIR="/tmp/armdocker"
INSTANCE_INFO="${WORKING_DIR}/instance_info.json"
ARM_AMI="ami-0e43196369d299715"  # AMI ID of latest arm-docker-ami-v*
INSTANCE_TYPE="m7g.medium"  # m7g.medium -> 1 vCPUS 4 GB RAM
MARKET_OPTIONS="MarketType=spot,SpotOptions={MaxPrice=0.25,SpotInstanceType=one-time}"
REGION="us-east-2"
EC2_USER="ec2-user"
USER_DATA_FILE="${SCRIPTS_DIR}/initialize.sh"
METADATA_ADDRESS="http://169.254.169.254/latest/meta-data"
MAC_ADDRESS=$(curl -s "${METADATA_ADDRESS}/network/interfaces/macs/" | head -n1 | tr -d '/')
CIDR=$(curl -s "${METADATA_ADDRESS}/network/interfaces/macs/${MAC_ADDRESS}/vpc-ipv4-cidr-block/")

: "${GITHUB_TOKEN:?Should be set}"

function start_arm_instance() {
    set -x
    mkdir -p "${WORKING_DIR}"
    cd "${WORKING_DIR}" || exit 1
    aws ec2 run-instances \
        --region "${REGION}" \
        --image-id "${ARM_AMI}" \
        --count 1 \
        --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":16}}]" \
        --instance-type "${INSTANCE_TYPE}" \
        --user-data "file://${USER_DATA_FILE}" \
        --instance-market-options "${MARKET_OPTIONS}" \
        --instance-initiated-shutdown-behavior terminate \
        --output json \
        > "${INSTANCE_INFO}"

    INSTANCE_ID=$(jq < "${INSTANCE_INFO}" ".Instances[0].InstanceId" -r)
    if [[ ${INSTANCE_ID} == "" ]]; then
        echo "ERROR!!!! Failed to start ARM instance. Likely because it could not be allocated on spot market."
        exit 1
    fi
    AVAILABILITY_ZONE=$(jq < "${INSTANCE_INFO}" ".Instances[0].Placement.AvailabilityZone" -r)
    aws ec2 wait instance-status-ok --instance-ids "${INSTANCE_ID}"
    INSTANCE_PRIVATE_DNS_NAME=$(aws ec2 describe-instances \
        --filters "Name=instance-state-name,Values=running" "Name=instance-id,Values=${INSTANCE_ID}" \
        --query 'Reservations[*].Instances[*].PrivateDnsName' --output text)
    SECURITY_GROUP=$(jq < "${INSTANCE_INFO}" ".Instances[0].NetworkInterfaces[0].Groups[0].GroupId" -r)
    rm -f my_key
    ssh-keygen -t rsa -f my_key -N ""
    aws ec2-instance-connect send-ssh-public-key --instance-id "${INSTANCE_ID}" \
        --availability-zone "${AVAILABILITY_ZONE}" \
        --instance-os-user "${EC2_USER}" \
        --ssh-public-key "file://${WORKING_DIR}/my_key.pub"
    aws ec2 authorize-security-group-ingress --region "${REGION}" --group-id "${SECURITY_GROUP}" \
            --protocol tcp --port 22 --cidr "${CIDR}" || true
    export AUTOSSH_LOGFILE="${WORKING_DIR}/autossh.log"
    autossh -f "-L12357:/var/run/docker.sock" \
        -N -o "IdentitiesOnly=yes" -o "StrictHostKeyChecking=no" \
        -i "${WORKING_DIR}/my_key" "${EC2_USER}@${INSTANCE_PRIVATE_DNS_NAME}"

    bash -c 'echo -n "Waiting port 12357 .."; for _ in `seq 1 40`; do echo -n .; sleep 0.25; nc -z localhost 12357 && echo " Open." && exit ; done; echo " Timeout!" >&2; exit 1'
}

function create_context() {
    echo
    echo "Creating buildx context: airflow_cache"
    echo
    docker buildx rm --force airflow_cache || true
    docker buildx create --name airflow_cache
    docker buildx create --name airflow_cache --append localhost:12357
    docker buildx ls
    echo
    echo "Context created"
    echo
}

start_arm_instance
create_context
