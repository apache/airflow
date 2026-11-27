 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# AGENTS instructions

For setup, pnpm commands, project structure, styling, state management, testing guidance, and general best practices, see the contributing docs:
[`contributing-docs/15_node_environment_setup.rst`](../../../../contributing-docs/15_node_environment_setup.rst)

Additional commands and conventions not covered there are listed below.

## Additional Commands

For e2e tests (Playwright), see [`tests/e2e/README.md`](tests/e2e/README.md).

## Coding Standards

- **TypeScript:** Strict mode is enabled (`strict`, `noUnusedLocals`, `noUnusedParameters`, `noUncheckedIndexedAccess`). Do not use `any`; fix type errors rather than suppressing them.
- **Path aliases:** Use the configured aliases (`src/*`, `openapi/*`, `tests/*`) rather than relative `../../` imports.
- **Icons:** Use `react-icons` exclusively. Do not add other icon packages. Prefer the three icon sets already dominant in the codebase — `react-icons/fi` (Feather, 82 uses), `react-icons/md` (Material Design, 28 uses), and `react-icons/lu` (Lucide, 26 uses) — before reaching for a less-used set.
- **Shared components:** Place reusable components in `src/components/`. The top-level `src/system-components/` directory is reserved for customized Chakra components only — do not put generic app components there.
- **API calls:** Use the auto-generated clients in `openapi-gen/` (generated from the OpenAPI spec via `pnpm codegen`). Do not write raw `axios` calls against API endpoints by hand.
- **No `useMemo` or `useCallback`:** The project uses the React compiler (`babel-plugin-react-compiler`), which handles memoization automatically. Do not add `useMemo` or `useCallback` manually — they are unnecessary and may interfere with the compiler's optimizations.

## i18n

- **Never add placeholder translations to i18n JSON files.** The English files (`en.json`) are already the source/placeholder — other locale files should only contain real translations. Do not duplicate English strings into non-English locale files as stand-ins.
- **Reuse existing translation keys whenever possible** instead of adding new ones. Before adding a new i18n key, check whether an existing key with the same or equivalent meaning already exists and use that instead.

## Runtime verification

When you verify a change against a running Airflow, start it with the
[runtime verification skill](../../../../.agents/skills/airflow-runtime-verification/SKILL.md).
The skill defines the placeholders used below. Most pages need data. Create a Dag run with the
[Task SDK section](../../../../task-sdk/AGENTS.md#runtime-verification) first.

The API server serves the compiled UI from `dist/` in this folder. A new worktree has no build, and
a build made before a UI change shows the old UI without an error. Run
`prek run compile-ui-assets --hook-stage manual --all-files` before the first UI check and after
every UI change, then reload the page.

Run the `pnpm` commands below from this folder.

### A page

The first command is needed once per checkout, the second once per machine:

```bash
pnpm install --frozen-lockfile
pnpm exec playwright install chromium
pnpm exec playwright screenshot --full-page --wait-for-timeout 3000 \
  http://localhost:<web>/dags/<dag_id> "$(git rev-parse --show-toplevel)/files/verify/<run>/dag-page.png"
```

Useful pages: `/dags`, `/dags/<dag_id>`, `/dags/<dag_id>/runs/<dag_run_id>`. Open the image and
check that it shows what the change claims. A screenshot shows a time in the time zone of the host.

### A flow of clicks

Reuse the page objects of the UI end-to-end tests in `tests/e2e/pages/`, with a configuration of
your own. The `playwright.config.ts` of this folder does not fit: it only collects
`tests/e2e/specs`, and its global setup logs in and waits for the example Dags.

1. In `files/verify/<run>/ui-flow/` at the root of the checkout, write `playwright.config.ts`:

   ```ts
   import { defineConfig, devices } from "@playwright/test";

   export default defineConfig({
     outputDir: "./test-results",
     projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
     reporter: [["list"]],
     testDir: ".",
     use: { baseURL: process.env.AIRFLOW_UI_BASE_URL, screenshot: "on", trace: "on" },
   });
   ```

2. Next to it write the spec as `trigger.spec.ts` (the name must end in `.spec.ts`), importing a
   page object by the alias this folder uses:

   ```ts
   import { expect, test } from "@playwright/test";
   import { DagsPage } from "tests/e2e/pages/DagsPage";

   test("trigger the Dag from its page", async ({ page }) => {
     const dagsPage = new DagsPage(page);

     await dagsPage.navigate();

     const runId = await dagsPage.triggerDag("<dag_id>");

     expect(runId).not.toBeNull();
     console.log(`VERIFY triggered ${runId}`);
   });
   ```

3. Run it:

   ```bash
   AIRFLOW_UI_BASE_URL=http://localhost:<web> pnpm exec playwright test \
     --config "$(git rev-parse --show-toplevel)/files/verify/<run>/ui-flow/playwright.config.ts" \
     --tsconfig ./tsconfig.app.json --project chromium
   ```

   `--tsconfig` resolves the alias. The trace and a screenshot of the last frame are written to
   `ui-flow/test-results/`. The last frame of this spec still shows the trigger dialog. Take a
   screenshot of `/dags/<dag_id>/runs/<dag_run_id>` with the command above to show the outcome.

4. Read the result back from the API. In `GET /api/v2/dags/<dag_id>/dagRuns`, a run triggered in
   the UI has `ui` in the `triggered_by` field.

Unpause the Dag before a trigger flow, or the run stays queued. `triggerDag` opens the page of the
Dag by its id. The helpers that pick a row from the Dags list, such as `getDagLink`, match the id as
a substring. When one Dag id contains another, they match two elements and the step fails.
