<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# PoC Agent Skills

<!-- agent-skill:start -->
id: run_targeted_tests
title: Run targeted tests
description: Run targeted tests using uv locally, falling back to Breeze when system dependencies are missing or results differ from CI
preferred_context: host
allowed_contexts: host,breeze
local_command: uv run --project {project} pytest {test_path} -xvs
breeze_command: breeze run pytest {test_path} -xvs
inside_breeze_command: pytest {test_path} -xvs
fallback_when: missing_system_dependencies|local_environment_mismatch|ci_local_discrepancy
<!-- agent-skill:end -->

<!-- agent-skill:start -->
id: setup_breeze_environment
title: Set up Breeze environment
description: Set up Breeze environment from host; should not be executed inside Breeze container
preferred_context: host
allowed_contexts: host,breeze
local_command: breeze shell
breeze_command: breeze shell
inside_breeze_command: NONE
fallback_when: none
<!-- agent-skill:end -->
