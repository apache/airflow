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

from pathlib import Path

# run_generate_constraints reads generated/provider_dependencies.json at import
# time.  In the CI scripts-test runner the file does not exist because it is
# only produced during image builds.  Create a minimal stub so that importing
# the module succeeds.
_generated_dir = Path(__file__).resolve().parents[3] / "generated"
_provider_deps_file = _generated_dir / "provider_dependencies.json"
if not _provider_deps_file.exists():
    _generated_dir.mkdir(parents=True, exist_ok=True)
    _provider_deps_file.write_text("{}")
