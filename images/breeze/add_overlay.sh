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

# shellcheck disable=SC2034
time_breeze="44:37"
time_breeze_installation=" 3:04"
time_breeze_running_tests=" 2:06"
time_breeze_select_backend_python=" 7:15"
time_breeze_using_tmux=" 2:34"
time_breeze_using_exec=" 1:35"
time_breeze_cloud_tools=" 1:55"
time_breeze_integrations=" 3:20"
time_breeze_build_images=" 1:48"
time_breeze_build_images_prod=" 1:30"
time_breeze_build_images_released_versions=" 1:30"
time_breeze_static_checks=" 1:24"
time_breeze_build_docs=" 1:04"
time_breeze_generate_requirements=" 1:37"
time_breeze_initialize_virtualenv=" 2:53"
time_breeze_kubernetes_tests=" 9:06"
time_breeze_stop=" 1:37"

for i in breeze*
do
    variable_name="time_${i%.*}"
    displayed_time="${!variable_name}"
    convert "$i" -strokewidth 0 -fill "rgba( 220, 220, 220 , 0.5 )" \
        -draw "rectangle 1620,980 1920,1080" \
        -fill "rgba( 255, 255, 255 , 1 )" \
        -draw "path 'M 1650,1010 L 1650,1050 L 1690,1030 Z'" \
        -pointsize 54 -draw "text 1745,1050 '${displayed_time}'" \
            "overlayed_${i}"
done
