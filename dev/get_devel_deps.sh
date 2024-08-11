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

# Run this script in Breeze to get the list of all devel dependencies
VERSION=2.9.3
uv pip freeze --python /usr/local/bin/python | grep -v "pip==" | grep -v "uv==" > freeze.txt
uv pip uninstall -r freeze.txt --python /usr/local/bin/python
uv pip install "apache-airflow[all]==${VERSION}" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${VERSION}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt" --python /usr/local/bin/python
uv pip freeze --python /usr/local/bin/python >non-devel-freeze.txt
sed "s/==.*//" < freeze.txt > all_deps.txt
sed "s/==.*//" < non-devel-freeze.txt > all_non_devel_deps.txt
grep -v -f all_non_devel_deps.txt all_deps.txt
