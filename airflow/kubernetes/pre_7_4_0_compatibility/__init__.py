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

# All the classes in this module should only be kept for backwards-compatibility reasons.
# old cncf.kubernetes providers will use those in their frozen version for pre-7.4.0 release
import warnings

warnings.warn(
    "This module is deprecated. The `cncf.kubernetes` provider before version 7.4.0 uses this module - "
    "you should migrate to a newer version of `cncf.kubernetes` to get rid of this warning. If you "
    "import the module via `airflow.kubernetes` import, please use `cncf.kubernetes' "
    "provider 7.4.0+ and switch all your imports to use `apache.airflow.providers.cncf.kubernetes` "
    "to get rid of the warning.",
    DeprecationWarning,
    stacklevel=2,
)
