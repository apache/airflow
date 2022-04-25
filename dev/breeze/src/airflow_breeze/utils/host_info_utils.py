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
"""
Returns information about Host that should be passed to the docker-compose.
"""
import platform

from airflow_breeze.utils.run_utils import run_command


def get_host_user_id():
    host_user_id = ''
    os = get_host_os()
    if os == 'Linux' or os == 'Darwin':
        host_user_id = run_command(cmd=['id', '-ur'], capture_output=True, text=True).stdout.strip()
    return host_user_id


def get_host_group_id():
    host_group_id = ''
    os = get_host_os()
    if os == 'Linux' or os == 'Darwin':
        host_group_id = run_command(cmd=['id', '-gr'], capture_output=True, text=True).stdout.strip()
    return host_group_id


def get_host_os():
    # Linux: Linux
    # Mac: Darwin
    # Windows: Windows
    return platform.system()
