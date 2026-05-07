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
