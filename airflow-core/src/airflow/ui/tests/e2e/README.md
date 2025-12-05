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

# Airflow UI End-to-End Tests

UI automation tests using Playwright for critical Airflow workflows.

## Prerequisites

**Requires running Airflow with example DAGs:**

- Airflow UI running on `http://localhost:28080` (default)
- Admin user: `admin/admin`
- Example DAGs loaded (uses `example_bash_operator`)

## Running Tests

### Using Breeze

```bash
# Basic run
breeze testing ui-e2e-tests

# Specific test with browser visible
breeze testing ui-e2e-tests --test-pattern "dag-trigger.spec.ts" --headed

# Different browsers
breeze testing ui-e2e-tests --browser firefox --headed
breeze testing ui-e2e-tests --browser webkit --headed
```

### Using pnpm directly

```bash
cd airflow-core/src/airflow/ui

# Install dependencies
pnpm install
pnpm exec playwright install

# Run tests
pnpm test:e2e:headed                    # Show browser
pnpm test:e2e:ui                       # Interactive debugging
```

## Test Structure

```
tests/e2e/
├── pages/           # Page Object Models
└── specs/           # Test files
```

## Configuration

Set environment variables if needed:

```bash
export AIRFLOW_UI_BASE_URL=http://localhost:28080
export TEST_USERNAME=admin
export TEST_PASSWORD=admin
export TEST_DAG_ID=example_bash_operator
```

## Debugging

```bash
# Step through tests
breeze testing ui-e2e-tests --debug-e2e

# View test report
pnpm test:e2e:report
```

Find test artifacts in `test-results/` and reports in `playwright-report/`.
