#
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


# Make `airflow` a namespace package, supporting installing
# airflow.providers.* in different locations (i.e. one in site, and one in user
# lib.)  This is required by some IDEs to resolve the import paths.
# And it _has_ to be the first code line too. Sad panda
#
# Note: this file is not installed or distributed in any distribution!
__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from __future__ import annotations  # noqa: F404
