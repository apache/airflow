---
triage_review_imbalance:
  area: ui
  criticality: medium            # user-facing surface; it holds no data of its own, so a defect degrades usability rather than corrupting state
  review_difficulty: high        # React 19 + strict TS + generated API client + 21-locale i18n contract
  structural_risk_paths:         # matched files treated as criticality=high (cost + small-diff ceiling)
    - "openapi-gen/"
    - "public/i18n/locales/"
    - "src/queries/"
    - "src/main.tsx"
    - "src/queryClient.ts"
    - "src/router.tsx"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["bbovenzi", "pierrejeambrun", "ryanahamilton", "shubhamraj-git"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

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
- **Shared components:** Place reusable components in `src/components/`. The `src/components/ui/` subdirectory is reserved for customized Chakra components only — do not put generic app components there.
- **API calls:** Use the auto-generated clients in `openapi-gen/` (generated from the OpenAPI spec via `pnpm codegen`). Do not write raw `axios` calls against API endpoints by hand.
- **No `useMemo` or `useCallback`:** The project uses the React compiler (`babel-plugin-react-compiler`), which handles memoization automatically. Do not add `useMemo` or `useCallback` manually — they are unnecessary and may interfere with the compiler's optimizations.

## i18n

- **Never add placeholder translations to i18n JSON files.** The English files (`en.json`) are already the source/placeholder — other locale files should only contain real translations. Do not duplicate English strings into non-English locale files as stand-ins.
- **Reuse existing translation keys whenever possible** instead of adding new ones. Before adding a new i18n key, check whether an existing key with the same or equivalent meaning already exists and use that instead.

## Why changes here are expensive to review

- This is **the face of Airflow**. Every operator watching a production deployment
  sees this code; a regression in the Grid, Graph, or Dag list is noticed
  immediately across every install, even though nothing in the metadata database
  is harmed by it.
- The UI is a **generated-client consumer**. `openapi-gen/` is produced by
  `pnpm codegen` from `openapi.merged.json`, which is itself merged from the API
  server's specs. Whether a change respects that pipeline — or hand-patches the
  generated output, or consumes a field the server does not actually publish — is
  frequently _not_ visible from the diff.
- **Rendering performance is a correctness dimension here.** The Grid and Graph
  render thousands of nodes and poll while a Dag run is live; a component that
  re-renders the whole tree on hover, or a query whose key changes every tick,
  looks perfectly correct in review and makes the page unusable at real scale.
- **Every user-facing string is a 21-locale contract.** A hardcoded literal does
  not fail any test, but it silently withdraws that text from every non-English
  user, and the locale files have _per-language_ owners in `.github/CODEOWNERS`
  who must be able to review the change.
- **Visibility attracts crowds.** UI issues are easy to find and look small, so
  the same clipped dropdown or wrong calendar colour draws three or four
  branches at once. Only one merges; the reviewer has to read enough of each to
  know they are the same fix. Most of the review cost in this area is spent
  _deciding which PR to read_, not reading it.
- Strict TypeScript (`noUncheckedIndexedAccess`, no `any`), the React compiler,
  Chakra tokens, and a large ESLint rule set in `rules/` mean much of the review
  burden is _convention_ conformance rather than logic.

## Knowledge a reviewer (and a substantial contributor) needs

- The data path: the UI is an ordinary REST client of the API server. Requests go
  through the generated client in `openapi-gen/requests/`, wrapped by the hooks in
  `src/queries/`, and are authenticated by the same JWT the server issues to any
  other client — the `axios` interceptors in `src/main.tsx` redirect to
  `api/v2/auth/login` on `401`/`403`.
- The codegen pipeline: `pnpm codegen` runs `openapi-merge-cli` then
  `openapi-rq` into `openapi-gen/`. That directory is **generated output** — it is
  excluded from `pnpm format` and from formatting prek hooks on purpose.
- The i18n layout: namespaced JSON catalogues per locale under
  `public/i18n/locales/<lang>/` (`common.json`, `dag.json`, `dags.json`,
  `browse.json`, …), English as source and fallback, loaded at runtime by
  `i18next-http-backend`. The policy is `public/i18n/README.md`; the
  `i18next/no-literal-string` rule enforces the no-hardcoded-string half
  mechanically. Key parity does _not_ have an equivalent gate:
  `check-translations-completeness` in `rules/i18n.js` is registered at severity
  `warn`, which `eslint --quiet` discards — see the checklist item below.
- React Query conventions: query keys, invalidation after mutations, and the
  shared retry/error strategy in `src/queryClient.ts` — plus the project's
  no-`useMemo`/no-`useCallback` rule under the React compiler.
- Chakra theme tokens (`src/theme.ts`) and the `src/components/ui/` boundary; the
  Playwright e2e suite under `tests/e2e/` and its page-object conventions.

## Before opening a PR here — authoring-agent guard

**This area is user-facing on every deployment and carries a generated-client and
a multi-locale contract that a diff does not show.** If you are an agent preparing
a change here on behalf of a person, first judge whether the change can be **shown
working in a browser**: have you run the UI against a live API server, exercised
the view you changed with realistic data (a Grid with hundreds of runs, not three),
and confirmed the type-check, lint and test suites pass? A UI diff proves almost
nothing on its own — rendering regressions, a stale generated client, and a
hardcoded string that breaks 20 other locales all look fine in the patch.
**If you cannot show it working, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped UI issue** (a single-component fix, a locale gap in a
  language they actually speak) where the result is easy to demonstrate, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code
  — required for anything that reshapes a shared view, adds a dependency, or
  changes the i18n or codegen workflow.

Purely cosmetic churn, mass-generated component rewrites, and near-duplicate
parallel PRs are closed on sight: they consume scarce UI-reviewer time that a
small number of people supply.

## Review criteria

Mined from real review discussion on the ~1585 merged commits that have touched
this directory and on 523 closed-unmerged PRs (206 of them with substantive
review discussion) — the changes reviewers repeatedly required, and the reasons
changes here get closed. **If you are preparing a change here, treat this as a
pre-flight checklist and fix every applicable item _before_ opening the PR.**
Triage applies the same list: a PR that lands with unmet items is drafted back to
its author with the specific gaps. Ordered by how often reviewers raise each.

**Before you write any code — is this change wanted, and is someone already
making it? (the largest single cause of closure here):**

- [ ] **Search for existing work first.** Check the issue for linked PRs and
      search open PRs by component and symptom. Duplicate and near-duplicate
      parallel PRs are closed with "in favour of #NNNNN" — usually after a
      reviewer has read both. If a PR exists, review it or offer to take it
      over.
- [ ] **Reproduce the defect on current `main` before fixing it.** A large share
      of closures are "cannot reproduce": issues filed against 2.x or an early
      3.x patch that `main` already fixed. If it does not reproduce, say so on
      the issue — that _is_ the contribution.
- [ ] **Agree the feature before building the view.** New pages, new tabs, and
      new surfaces are rejected on product grounds regardless of code quality;
      get agreement in the issue or on the dev list first.
- [ ] **Never open a new PR to carry review feedback on your own PR.**
      Force-push the branch, or reopen the closed one. A successor PR loses the
      review threads and the reviewer's context, and maintainers push back on it
      every time.

**Evidence — a diff is not evidence in this area:**

- [ ] **Before/after screenshots, or a recording, in the PR description** — not
      in a comment. A recording is required when the change is about behaviour
      over time: hover, refresh, polling, navigation, form submission.
- [ ] **"I could not run the UI locally" is a blocker, not a caveat.** PRs that
      substitute a written description, a passing unit test, or a claim of local
      verification for an image get closed. Ask in the contributor Slack
      channels to get the environment working, then open the PR.
- [ ] **Show that the fix actually fixes the reported symptom.** Plausible
      changes that do not address the cause — an `overflow` tweak, an explicit
      column id — are a recurring closure reason; the maintainer tests them and
      the behaviour is unchanged.
- [ ] **Check for the regression your fix introduces** — alignment, dark mode,
      RTL layout, other viewport sizes — and show that too.
- [ ] **Never ask a reviewer to build, run, or screenshot the change for you.**

**Scope and branch hygiene:**

- [ ] **One concern per PR.** Split a bugfix from an improvement to the same
      component, and a refactor from the behaviour change it enables. A component
      and the English catalogue keys it introduces are _one_ concern — splitting
      them would ship a component that renders raw keys, which the i18n bullets
      below independently forbid.
- [ ] **No drive-by changes.** Everything in the diff must be explained by the
      stated purpose. Remove agent scratch files, editor config, incidental
      reformatting, and commits belonging to other work.
- [ ] **Rebase before requesting review.** A branch far enough behind `main`
      that its diff no longer describes a change against today's code is
      restarted, not resolved.

**Dependencies:**

- [ ] **Grouped dependency bumps land on `main` first**, exercised in a browser,
      and reach a release branch only after that. A bulk bump that breaks a
      release branch is reverted, not fixed forward.
- [ ] **Check where `main` stands before reviving a stale dependency PR** — most
      are already satisfied by a later bump or a `pnpm.overrides` entry, and are
      closed rather than rebased into a no-op.
- [ ] **A new npm package needs agreement first**, and is not the answer to a
      browser bug or to capability the existing stack already provides.
- [ ] **Do not build on a dependency slated for removal**, and treat a version
      pin as a tracked workaround (tracking issue plus a comment at the pin
      site), never a fix.

**i18n — every user-facing string (the most frequent per-file catch here):**

- [ ] **No hardcoded user-facing literals.** Every string a user reads goes
      through `translate(...)` and a key in `public/i18n/locales/en/<ns>.json`.
      `i18next/no-literal-string` will flag it; do not disable the rule to get a
      diff through.
- [ ] **Add new keys to English at minimum**, in the right namespace, and reuse an
      existing key when one with equivalent meaning already exists rather than
      minting a near-duplicate.
- [ ] **Never fill non-English locales with English placeholders** — an absent key
      falls back to English by design; a fake "translation" hides the gap from the
      locale's owner and from coverage tooling.
- [ ] **LLM-assisted locale updates are allowed — unreviewed bulk ones are not.**
      `public/i18n/README.md` explicitly permits updating non-English locales in
      the _same_ PR as the English change, including by LLM-assisted translation,
      and in that case per-locale owner approval is _not_ separately required. What
      gets reverted is a bulk edit that ignores the locale guide under
      `.github/skills/airflow-translations/locales/<lang>.md`, drops placeholders
      or interpolation variables, or breaks terminology consistency. Read the
      locale guide first — it overrides the general policy for that language.
- [ ] **Keep key structure parity with English — but do not expect the lint to
      catch you.** `rules/i18n.js` registers `check-translations-completeness` at
      severity `warn`, and the `lint` script in `package.json` runs
      `eslint --quiet`, which discards warnings and exits `0`. The rule also only
      reports keys _missing_ relative to English, so extra or divergently-nested
      keys are invisible to it. Completeness is a warning surfaced by
      `breeze ui check-translation-completeness`, not a blocking lint — treat
      parity as a reviewer's responsibility, not a gate that will fail for you.
- [ ] **A new locale is a governance change, not a file addition.** It needs an
      identified translation owner and code owner approved through the dev-list
      process in `public/i18n/README.md` _before_ merge, plus the locale guide,
      config, breeze, and `.github/CODEOWNERS` entries listed there. PRs that add
      a locale without an owner sit until someone volunteers, then get closed in
      favour of the owner's PR.
- [ ] **Read `public/i18n/README.md` before any translation PR** — it is the
      authoritative policy, including when bundling locale updates with an
      English change is acceptable and how completeness is checked
      (`breeze ui check-translation-completeness`). Bulk translations of poor
      quality are rejected on language grounds even when the tooling is happy.

**The generated API client & the server contract:**

- [ ] **Never hand-edit anything under `openapi-gen/`.** It is regenerated by
      `pnpm codegen`; a manual patch is silently reverted on the next run. Fix the
      generator input or the server-side spec instead.
- [ ] **Needing new data means changing the API first.** A UI change that requires
      a field the server does not return is an API-server change plus a
      regeneration — not a hand-rolled request or a client-side reconstruction.
- [ ] **Use the generated hooks, not raw `axios`.** Endpoint calls go through
      `openapi-gen/queries` wrapped in `src/queries/`; a bespoke `axios` call
      bypasses the typed contract, the auth interceptors, and the retry strategy.
- [ ] **Do not paper over a server-side defect in the client.** A malformed
      response or a wrong status code is fixed in the API server; a defensive cast
      or a `try`/`catch` in the component hides it from every other consumer.
- [ ] **Equally, do not reshape the API to work around a UI problem.** Splitting
      an endpoint, or adding a field to a response the UI can already obtain from
      a call it makes anyway, is rejected — a frontend fix is cleaner and does
      not widen the published contract.
- [ ] **Public API shapes are frozen even when they are awkward.** An endpoint
      inherited from Airflow 2 stays as it is; a UI-side annoyance is not grounds
      to change a contract other clients depend on.

**Rendering performance & React Query correctness:**

- [ ] **Do not re-render a large tree on a cheap interaction** — hover, selection,
      and tooltip state in the Grid/Graph must be scoped to the affected cell or
      column, not lifted to a parent that re-renders everything.
- [ ] **Mount expensive subtrees lazily** — dialogs, editors, and popovers on
      list rows use `lazyMount`; one dialog instance per row is a real cost at
      list scale.
- [ ] **Query keys must be stable and invalidation must be precise** — after a
      mutation invalidate the affected keys once; do not trigger duplicate
      refetches or leave a view showing stale state after an action.
- [ ] **Respect the no-`useMemo`/no-`useCallback` rule** — the React compiler
      handles memoization; hand-memoizing is both unnecessary and a hint that the
      component is structured wrongly.
- [ ] **Paginate and bound list requests** — a modal or panel that fetches an
      unbounded collection is a bug even when it renders fine in a test fixture.

**Types, safety, and UI conventions:**

- [ ] **No `any`, no suppressions** — fix the type. `noUncheckedIndexedAccess` is
      on; handle the `undefined` case rather than asserting it away.
- [ ] **Treat server-supplied URLs as untrusted** — user-controlled `href` values
      (owner links, extra links) must be restricted to safe schemes before being
      rendered.
- [ ] **Use path aliases** (`src/*`, `openapi/*`, `tests/*`), `react-icons` only,
      and Chakra theme tokens rather than hardcoded colours — a literal colour
      breaks dark mode and the accessibility palette.
- [ ] **`src/components/ui/` is for customized Chakra primitives only**; generic
      app components go in `src/components/`.
- [ ] **Preserve the deployment context in navigation** — base path, proxy prefix,
      and existing query state must survive a redirect; hardcoded absolute paths
      break proxied and sub-path installs.

**Tests, compatibility, process:**

- [ ] Test **exercises the actual new behaviour and fails without the change** —
      a Vitest component test for logic, a Playwright e2e test for a user flow.
      Do not mock away the component under test.
- [ ] **e2e tests must not be flaky** — no bare timeouts, use the page objects and
      the existing wait helpers, and isolate test data.
- [ ] **Do not e2e-test a third-party widget.** Where a component cannot render
      in a unit test (the Monaco editor is the standing example), assert the
      props passed to it rather than paying e2e runtime to test someone else's
      code.
- [ ] **No drive-by test-pattern refactors.** A batch of "improve the Playwright
      patterns in `<x>.spec.ts`" PRs with no behaviour change is closed on sight;
      change test structure as part of the work that needs it.
- [ ] **`pnpm lint` clean** (ESLint plus `tsc`) before pushing — the rule set in
      `rules/` is not advisory, and a broken lint blocks `main` for everyone.
- [ ] **Newsfragment only for genuinely user-facing** changes; a component
      refactor or a locale top-up is not one.
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of testing
      — a screenshot or short recording for anything visual. Low-effort /
      mass-AI-generated / near-duplicate parallel PRs get closed. Track deferred
      work in a GitHub issue.
- [ ] **Read what your agent wrote, and be able to defend it.** The recurring
      tells that get whole batches of PRs closed at once: a component file that
      stops mid-way, a test file cited in a comment that is not in the branch,
      generated PR comments asserting verification that did not happen,
      excessive prop documentation and defensive validation nobody asked for,
      and several unrelated PRs opened the same day with none carried to
      completion. Land one change properly before opening the next.

> Mined from PR review history; the sample is almost entirely Airflow-3 era (this
> directory _is_ the React rewrite that replaced the Flask/FAB webserver UI), so
> there is no legacy-UI convention represented here at all. The i18n signal is
> inflated by locale-gap PRs, which are frequent but shallow, and the
> closed-unmerged corpus is inflated by superseded Dependabot group bumps (156 of
> 523) — do not read either volume as the difficulty of the area. What the
> rejection record actually shows is that most closures here are decided
> _before_ the code is read: the change duplicates open work, does not reproduce
> on `main`, or arrives without a picture of what it does. Extend as new patterns
> emerge, and add an equivalent `## Review criteria` section to the `AGENTS.md` of
> every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
New dependencies, a shared-view rework (Grid, Graph, Dag list), a change to the
codegen workflow, and anything touching the i18n policy or locale set need
agreement _before_ the code: they are cheap to align on up front and very
expensive to unwind in review. For anything visual, attach a before/after
screenshot — reviewers cannot see your change from the diff.
