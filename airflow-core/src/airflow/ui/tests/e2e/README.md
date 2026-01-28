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

# UI End-to-End Tests

End-to-end tests for the Airflow UI using Playwright.

## Running Tests

### Using Breeze (Recommended)

The easiest way to run the tests:

```bash
breeze testing ui-e2e-tests

# Run specific browser
breeze testing ui-e2e-tests --browser firefox

# Run specific test
breeze testing ui-e2e-tests --test-pattern "dag-trigger.spec.ts"

# Debug mode
breeze testing ui-e2e-tests --debug-e2e

# See the browser
breeze testing ui-e2e-tests --headed
```

### Direct Execution

If you already have Airflow running on `http://localhost:8080`:

```bash
cd airflow-core/src/airflow/ui
pnpm install
pnpm test:e2e:install
pnpm test:e2e
```

## CI Integration

Tests run in GitHub Actions via workflow dispatch. The workflow uses `breeze testing ui-e2e-tests` which handles starting Airflow with docker-compose, running the tests, and cleanup.

To run manually:

1. Go to Actions → UI End-to-End Tests
2. Click Run workflow
3. Select browser and other options

## Directory Structure

```
tests/e2e/
├── pages/           # Page objects
│   ├── BasePage.ts
│   ├── LoginPage.ts
│   └── DagsPage.ts
└── specs/           # Test files
    └── dag-trigger.spec.ts
```

## Writing Tests

We use the Page Object Model pattern:

```typescript
// pages/DagPage.ts
export class DagPage extends BasePage {
  readonly pauseButton: Locator;

  constructor(page: Page) {
    super(page);
    this.pauseButton = page.locator('[data-testid="dag-pause"]');
  }

  async pause() {
    await this.pauseButton.click();
  }
}

// specs/dag.spec.ts
test('pause DAG', async ({ page }) => {
  const dagPage = new DagPage(page);
  await dagPage.goto();
  await dagPage.pause();
  await expect(dagPage.pauseButton).toHaveAttribute('aria-pressed', 'true');
});
```

## Configuration

Environment variables (with defaults):

- `AIRFLOW_UI_BASE_URL` - Airflow URL (default: `http://localhost:8080`)
- `TEST_USERNAME` - Username (default: `airflow`)
- `TEST_PASSWORD` - Password (default: `airflow`)
- `TEST_DAG_ID` - Test DAG ID (default: `example_bash_operator`)

## Debugging

View test report after running locally:

```bash
pnpm test:e2e:report
```

When tests fail in CI, check the uploaded artifacts for screenshots and HTML reports.

## Breeze Options

```bash
breeze testing ui-e2e-tests --help
```

Common options:

- `--browser` - chromium, firefox, webkit, or all
- `--headed` - Show browser window
- `--debug-e2e` - Enable Playwright inspector
- `--ui-mode` - Interactive UI mode
- `--test-pattern` - Run specific test file
- `--workers` - Number of parallel workers

## Contributing New Tests

This framework uses the **Page Object Model (POM)** pattern. Each page's elements and interactions are encapsulated in a page class.

### Steps to Add a New Test

1. **Create a page object** (if needed) in `pages/`
   - Extend `BasePage` and define page elements as locators
   - Add methods for page interactions
   - See existing pages: [LoginPage.ts](pages/LoginPage.ts), [DagsPage.ts](pages/DagsPage.ts)

2. **Create a spec file** in `specs/`
   - Import page objects and write test steps
   - See existing test: [dag-trigger.spec.ts](specs/dag-trigger.spec.ts)

3. **Run tests locally**

   ```bash
   breeze testing ui-e2e-tests --test-pattern "your-test.spec.ts"
   ```

4. **Submit PR**

### Best Practices

- Use `data-testid` attributes for selectors when available
- Keep tests independent - each test should set up its own state
- Add meaningful assertions, not just navigation checks

### Naming Convention

- Spec files: `<feature>.spec.ts`
- Page objects: `<Page>Page.ts`
- Test names: Start with "should"
