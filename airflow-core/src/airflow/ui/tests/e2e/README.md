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

This directory contains end-to-end (E2E) tests for the Airflow UI using Playwright.

## Overview

These tests focus on **critical user workflows** to catch regressions while maintaining a manageable test suite:

- ✅ **Authentication flow** - Login/logout functionality
- ✅ **DAG management** - Triggering DAGs and basic operations
- ✅ **Navigation** - Core UI navigation and page loading

Following the **test pyramid approach**, we maintain a small set of high-value E2E tests while expanding unit test coverage.

## Quick Start

### Prerequisites

- Node.js 18+ and pnpm installed
- Running Airflow instance with test data
- Test user credentials (default: admin/admin)

### Installation

```bash
# Install dependencies
pnpm install

# Install Playwright browsers
pnpm test:e2e:install
```

### Running Tests

```bash
# Run all E2E tests (headless)
pnpm test:e2e

# Run with visible browser (development)
pnpm test:e2e:headed

# Run in interactive UI mode (debugging)
pnpm test:e2e:ui

# Run specific test file
pnpm test:e2e tests/e2e/specs/dag-trigger.spec.ts
```

## Test Structure

```
tests/e2e/
├── pages/              # Page Object Models
│   ├── BasePage.ts     # Common page functionality
│   ├── LoginPage.ts    # Authentication
│   └── DagsPage.ts     # DAG operations
└── specs/              # Test specifications
    ├── dag-trigger.spec.ts   # DAG triggering workflow
    └── dag-list.spec.ts      # DAG navigation workflow
```

## Configuration

### Environment Variables

Set these for your test environment:

```bash
# Test credentials
export TEST_USERNAME=admin
export TEST_PASSWORD=admin

# Test DAG (should exist in your instance)
export TEST_DAG_ID=example_bash_operator

# Airflow URL
export AIRFLOW_UI_BASE_URL=http://localhost:8080
```

### Browser Selection

Tests run on Chromium by default. To test other browsers:

```bash
# Firefox
pnpm test:e2e --project=firefox

# WebKit (Safari)
pnpm test:e2e --project=webkit
```

## Test Development Guidelines

### Writing Tests

1. **Follow Page Object Model** - Keep selectors and actions in page objects
2. **Robust Selectors** - Use multiple fallback selectors for UI flexibility
3. **Explicit Waits** - Always wait for elements/states, never use fixed delays
4. **Clear Test Names** - Describe the user workflow being tested

### Example Test Structure

```typescript
test('should complete user workflow', async ({ page }) => {
  // Step 1: Setup/Authentication
  const loginPage = new LoginPage(page);
  await loginPage.login('admin', 'admin');

  // Step 2: Navigate
  const dagsPage = new DagsPage(page);
  await dagsPage.navigate();

  // Step 3: Perform action
  await dagsPage.triggerDag('test_dag');

  // Step 4: Verify result
  await dagsPage.verifyDagTriggered('test_dag');
});
```

### Debugging Failed Tests

```bash
# Run in debug mode (step through test)
pnpm test:e2e:debug

# Generate test code interactively
pnpm exec playwright codegen http://localhost:8080

# View last test report
pnpm test:e2e:report
```

## CI/CD Integration

Tests run automatically on:

- Pull requests affecting UI or API code
- Pushes to main branch
- Manual workflow dispatch

### Artifacts

On failure, CI uploads:

- Screenshots of failed tests
- Videos of test execution
- Playwright trace files
- HTML test report

## Maintenance Notes

### Selector Strategy

Page objects use multiple selector strategies for robustness:

```typescript
// Multiple fallbacks for different UI implementations
this.loginButton = page.locator([
  '[data-testid="login-button"]',  // Preferred: test IDs
  'button[type="submit"]',         // Semantic: form buttons
  'button:has-text("Login")',      // Content: button text
  '.login-form button'             // Fallback: CSS classes
].join(', ')).first();
```

### Adding New Tests

1. **Check existing coverage** - Avoid duplicating test scenarios
2. **Focus on user workflows** - Test complete user journeys, not individual components
3. **Consider maintenance cost** - E2E tests are expensive to maintain
4. **Add to CI selectively** - Not every test needs to run on every PR

## Community Guidelines

This implementation follows the Airflow community's feedback:

- **Limited scope** - Critical workflows only (~10-15 scenarios max)
- **TypeScript** - Familiar to UI developers
- **Robust tooling** - Good debugging and failure analysis
- **CI integration** - Smart triggering to avoid slowing development

## Getting Help

- **Local development issues** - Check Playwright documentation
- **CI failures** - Review artifacts and trace files
- **Test maintenance** - Refer to Page Object Model patterns
- **Community discussion** - Join #dev channel on Airflow Slack

## Contributing

When adding new E2E tests:

1. Discuss scope on the dev mailing list first
2. Follow existing patterns and conventions
3. Include comprehensive error handling
4. Update documentation for new workflows
5. Test locally across different browsers

Remember: E2E tests should focus on **critical user journeys** that would be difficult to catch with unit tests alone.
