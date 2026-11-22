<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# The REST API and the UI

A change under `airflow-core/src/airflow/api_fastapi/` or `airflow-core/src/airflow/ui/` is seen by
API clients and by people using the UI. The proof is the HTTP exchange and the rendered page.

The instance from [`SKILL.md`](../SKILL.md) is up and healthy. `<web>` is its port. Most checks need
data first: create it with the recipe in [`dag-run.md`](dag-run.md).

## REST API

Save the status code with the body, for example:

```bash
curl -s -w '\nhttp_status=%{http_code}\n' http://localhost:<web>/api/v2/dags \
  | tee -a files/verify/<run>/api.log
```

- **Error paths.** `GET /api/v2/dags/no_such_dag` returns `404` with a `detail` string. A body that
  fails validation returns `422`, and `detail[].loc` names the field.
- **A change and its read-back.** `PATCH /api/v2/dags/<dag_id>` with `{"is_paused": true}`, then
  `GET /api/v2/dags/<dag_id>`, then the same fact from the CLI with `airflow dags list -o plain`.
- **Routes of the UI.** The React application calls `/ui/...` routes, for example
  `/ui/dags?limit=5`. They are private and change freely. When a change claims an effect on the
  API, prove it on a public `/api/v2` route.

The running instance serves its API reference at `http://localhost:<web>/docs` and the OpenAPI
document at `http://localhost:<web>/openapi.json`.

## A page

Run these from `airflow-core/src/airflow/ui`. The first is needed once per checkout, the second
once per machine:

```bash
pnpm install --frozen-lockfile
pnpm exec playwright install chromium
pnpm exec playwright screenshot --full-page --wait-for-timeout 3000 \
  http://localhost:<web>/dags/<dag_id> "$(git rev-parse --show-toplevel)/files/verify/<run>/dag-page.png"
```

Useful pages: `/dags`, `/dags/<dag_id>`, `/dags/<dag_id>/runs/<dag_run_id>`. Open the image and
check that it shows what the change claims. A screenshot shows a time in the time zone of the host.

## A flow of clicks

Reuse the page objects of the UI end-to-end tests in `tests/e2e/pages/`, with a configuration of
your own. The `playwright.config.ts` of the repository does not fit: it only collects
`tests/e2e/specs`, and its global setup logs in and waits for the example Dags.

1. In `files/verify/<run>/ui-flow/` write `playwright.config.ts`:

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
   page object by the alias the repository uses:

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

3. Run it from `airflow-core/src/airflow/ui`:

   ```bash
   AIRFLOW_UI_BASE_URL=http://localhost:<web> pnpm exec playwright test \
     --config "$(git rev-parse --show-toplevel)/files/verify/<run>/ui-flow/playwright.config.ts" \
     --tsconfig ./tsconfig.app.json --project chromium
   ```

   `--tsconfig` resolves the alias. The trace and a screenshot of the last frame are written to
   `ui-flow/test-results/`. The last frame of this spec still shows the trigger dialog; take a
   screenshot of `/dags/<dag_id>/runs/<dag_run_id>` with the command above to show the outcome.

4. Read the result back from the API: in `GET /api/v2/dags/<dag_id>/dagRuns`, a run triggered in
   the UI has `ui` in the `triggered_by` field.

Unpause the Dag before a trigger flow, or the run stays queued. `triggerDag` opens the page of the
Dag by its id. The helpers that pick a row from the Dags list, such as `getDagLink`, match the id as
a substring: when one Dag id contains another, they match two elements and the step fails.
