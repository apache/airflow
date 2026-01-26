# Airflow UI End-to-End Tests

This directory contains E2E tests for the Apache Airflow UI using Playwright.

## Prerequisites

- Node.js (v16 or later)
- pnpm package manager
- A running Airflow instance (for tests to interact with)

## Setup

1. Install dependencies:
```bash
cd airflow/ui
pnpm install
```

2. Install Playwright browsers:
```bash
pnpm exec playwright install
```

## Running Tests

### Run all E2E tests:
```bash
pnpm test:e2e
```

### Run tests in UI mode (interactive):
```bash
pnpm test:e2e:ui
```

### Run tests in debug mode:
```bash
pnpm test:e2e:debug
```

### Run tests on a specific browser:
```bash
pnpm test:e2e:chromium    # Chrome
pnpm test:e2e:firefox     # Firefox
pnpm test:e2e:webkit      # Safari
```

### Run a specific test file:
```bash
pnpm exec playwright test tests/e2e/specs/variables.spec.ts
```

## Configuration

Test configuration is managed in `testConfig.ts`. You can override values using environment variables:

```bash
# Set custom Airflow URL
export AIRFLOW_BASE_URL=http://localhost:8080

# Set custom credentials
export AIRFLOW_USERNAME=admin
export AIRFLOW_PASSWORD=admin

# Run tests
pnpm test:e2e
```

## Test Structure

```
tests/e2e/
├── pages/              # Page Object Models
│   ├── BasePage.ts     # Base class for all pages
│   ├── LoginPage.ts    # Login page interactions
│   └── VariablesPage.ts # Variables page interactions
├── specs/              # Test specifications
│   └── variables.spec.ts # Variables page tests
└── testConfig.ts       # Test configuration
```

## Page Object Model (POM)

All tests follow the Page Object Model pattern:
- **Page Objects** encapsulate UI interactions
- **Test Specs** use page objects to test functionality
- This keeps tests maintainable and readable

## Writing New Tests

1. Create a new Page Object in `pages/` (extend `BasePage`)
2. Create a new test spec in `specs/`
3. Use the page object methods in your tests

Example:
```typescript
import { test } from "@playwright/test";
import { LoginPage } from "../pages/LoginPage";
import { MyNewPage } from "../pages/MyNewPage";

test.describe("My Feature", () => {
  test.beforeEach(async ({ page }) => {
    const loginPage = new LoginPage(page);
    await loginPage.login();
  });

  test("should do something", async ({ page }) => {
    const myPage = new MyNewPage(page);
    await myPage.doSomething();
    // Add assertions
  });
});
```

## Reports

After running tests, view the HTML report:
```bash
pnpm exec playwright show-report
```

## Troubleshooting

### Tests failing to connect to Airflow

Make sure Airflow is running and accessible at the URL specified in `testConfig.ts` or `AIRFLOW_BASE_URL` environment variable.

### Authentication issues

Verify that the credentials in `testConfig.ts` or environment variables (`AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD`) are correct.

### Timeout errors

Increase timeouts in `testConfig.ts` if your Airflow instance is slow to respond.

## Contributing

When adding new E2E tests:
- Follow the existing POM pattern
- Ensure tests clean up their own data in `afterAll` hooks
- Use unique test data prefixes to avoid conflicts
- Make tests work across all browsers (Chromium, Firefox, WebKit)
- Avoid hardcoded values - use `testConfig` or dynamic generation
