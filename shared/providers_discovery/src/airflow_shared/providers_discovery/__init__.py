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

from __future__ import annotations

from .providers_discovery import (
    KNOWN_UNHANDLED_OPTIONAL_FEATURE_ERRORS as KNOWN_UNHANDLED_OPTIONAL_FEATURE_ERRORS,
    HookClassProvider as HookClassProvider,
    HookInfo as HookInfo,
    LazyDictWithCache as LazyDictWithCache,
    PluginInfo as PluginInfo,
    ProviderInfo as ProviderInfo,
    _check_builtin_provider_prefix as _check_builtin_provider_prefix,
    _create_provider_info_schema_validator as _create_provider_info_schema_validator,
    discover_all_providers_from_packages as discover_all_providers_from_packages,
    log_import_warning as log_import_warning,
    log_optional_feature_disabled as log_optional_feature_disabled,
    provider_info_cache as provider_info_cache,
)
