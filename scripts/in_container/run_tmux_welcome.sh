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
cd /opt/airflow/ || exit
clear
echo "Welcome to your tmux based running Airflow environment (courtesy of Breeze)."
echo
echo "     To stop Airflow and exit tmux, just type 'stop_airflow'."
echo
echo "     If you want to rebuild webserver assets dynamically, run 'cd airflow/www; yarn && yarn dev' and restart airflow webserver with '-d' flag."
echo
