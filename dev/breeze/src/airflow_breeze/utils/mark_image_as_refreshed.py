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
from __future__ import annotations

from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.utils.cache import touch_cache_file
from airflow_breeze.utils.md5_build_check import calculate_md5_checksum_for_files
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR


def mark_image_as_refreshed(ci_image_params: BuildCiParams):
    ci_image_cache_dir = BUILD_CACHE_DIR / ci_image_params.airflow_branch
    ci_image_cache_dir.mkdir(parents=True, exist_ok=True)
    touch_cache_file(f"built_{ci_image_params.python}", root_dir=ci_image_cache_dir)
    calculate_md5_checksum_for_files(ci_image_params.md5sum_cache_dir, update=True)
