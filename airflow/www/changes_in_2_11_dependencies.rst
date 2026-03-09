 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Airflow 2.11.2rc1 — Frontend Dependency Changes
================================================

Full summary of all upgrades grouped by risk level.
Based on actual ``package.json`` diff between 2.11.1 and 2.11.2rc1.


HIGH RISK — Major version bumps with known breaking changes
-----------------------------------------------------------

+------------------------------+------------+------------+----------------------------------------------------------------+
| Package                      | Old        | New        | Breaking Changes                                               |
+==============================+============+============+================================================================+
| react / react-dom            | ^18.0.0    | ^19.2.4    | Major upgrade to React 19. New hooks (use, useActionState,     |
|                              |            |            | useFormStatus, useOptimistic), ref as prop, async transitions, |
|                              |            |            | Suspense changes, removal of legacy APIs (forwardRef less      |
|                              |            |            | needed, string refs removed, defaultProps deprecated for       |
|                              |            |            | function components). react-dom/client changes.                |
+------------------------------+------------+------------+----------------------------------------------------------------+
| react-router-dom             | ^6.3.0     | ^7.13.1    | Major v7 upgrade. Route definition API changes, loader/action  |
|                              |            |            | patterns changed, new framework mode, createBrowserRouter      |
|                              |            |            | recommended over BrowserRouter.                                |
+------------------------------+------------+------------+----------------------------------------------------------------+
| echarts                      | ^5.4.2     | ^6.0.0     | Breaking API changes in chart options, deprecated configs      |
|                              |            |            | removed, default themes changed. May silently render charts    |
|                              |            |            | differently.                                                   |
+------------------------------+------------+------------+----------------------------------------------------------------+
| framer-motion                | ^6.0.0     | ^11.18.2   | API overhaul across 5 major versions. AnimatePresence exit     |
|                              |            |            | animations changed, useAnimation -> useAnimationControls,      |
|                              |            |            | layout animation API changed.                                  |
+------------------------------+------------+------------+----------------------------------------------------------------+
| eslint                       | ^8.6.0     | ^9.27.0    | Flat config is now the default. .eslintrc files deprecated in  |
|                              |            |            | favor of eslint.config.js. Many plugin/config APIs changed.    |
|                              |            |            | --ext and --ignore-path removed.                               |
+------------------------------+------------+------------+----------------------------------------------------------------+
| jest                         | ^27.3.1    | ^30.2.0    | Complete rewrite. Test globals imported differently,           |
|                              |            |            | jest.config.js format changes, timer mocking API changes,      |
|                              |            |            | snapshot format changed.                                       |
+------------------------------+------------+------------+----------------------------------------------------------------+
| typescript                   | ^4.6.3     | ^5.9.3     | Enum behavior changes, decorator support changes,              |
|                              |            |            | moduleResolution defaults changed. May surface new type        |
|                              |            |            | errors.                                                        |
+------------------------------+------------+------------+----------------------------------------------------------------+
| swagger-ui-dist              | 4.1.3      | 5.32.0     | CSS class names changed, rendering differences, bundled CSS    |
|                              |            |            | restructured.                                                  |
+------------------------------+------------+------------+----------------------------------------------------------------+
| react-markdown               | ^8.0.4     | ^10.1.0    | Major v9 and v10. Moved to ESM-only, plugin API changes,       |
|                              |            |            | remark/rehype ecosystem alignment, component prop changes.     |
+------------------------------+------------+------------+----------------------------------------------------------------+
| d3-shape                     | ^2.1.0     | ^3.2.0     | d3.line().curve() defaults changed, some curve types renamed.  |
+------------------------------+------------+------------+----------------------------------------------------------------+
| moment-timezone              | ^0.5.43    | ^0.6.0     | Timezone data format changed, some edge-case parsing           |
|                              |            |            | differences.                                                   |
+------------------------------+------------+------------+----------------------------------------------------------------+
| elkjs                        | ^0.7.1     | ^0.11.1    | Layout algorithm changes -- graph layouts may render           |
|                              |            |            | differently.                                                   |
+------------------------------+------------+------------+----------------------------------------------------------------+
| color                        | ^4.2.3     | ^5.0.3     | Major v5. API changes in color manipulation methods.           |
+------------------------------+------------+------------+----------------------------------------------------------------+
| camelcase-keys               | ^7.0.0     | ^10.0.2    | Major v8/v9/v10. ESM-only since v8, API surface changes.       |
+------------------------------+------------+------------+----------------------------------------------------------------+
| type-fest                    | ^2.17.0    | ^5.4.4     | Major v3/v4/v5. Type definitions changed/renamed/removed       |
|                              |            |            | across versions.                                               |
+------------------------------+------------+------------+----------------------------------------------------------------+


MEDIUM RISK — Dev/build tooling or breaking changes less likely to affect runtime
---------------------------------------------------------------------------------

Testing & code quality
^^^^^^^^^^^^^^^^^^^^^^

+------------------------------+------------+------------+----------------------------------------------------------------+
| Package                      | Old        | New        | Breaking Changes                                               |
+==============================+============+============+================================================================+
| @typescript-eslint/*         | ^5.x       | ^8.56.1    | New rules enabled by default, some rules removed/renamed,      |
|                              |            |            | stricter defaults. Many new lint errors likely.                |
+------------------------------+------------+------------+----------------------------------------------------------------+
| @testing-library/react       | ^13.0.0    | ^16.3.2    | renderHook now exported from main, cleanup behavior changed,   |
|                              |            |            | act() wrapping changes. Requires React 18+.                    |
+------------------------------+------------+------------+----------------------------------------------------------------+
| @testing-library/jest-dom    | ^5.16.0    | ^6.9.1     | Matchers now use @jest/expect instead of jest-jasmine2. Some   |
|                              |            |            | matchers renamed.                                              |
+------------------------------+------------+------------+----------------------------------------------------------------+
| prettier                     | ^2.8.4     | ^3.8.1     | Trailing commas now default to all, different formatting for   |
|                              |            |            | various constructs. Will change code formatting.               |
+------------------------------+------------+------------+----------------------------------------------------------------+
| openapi-typescript           | ^5.4.1     | ^7.13.0    | Generated types may have different structure.                  |
|                              |            |            | generate-api-types script output may change.                   |
+------------------------------+------------+------------+----------------------------------------------------------------+

UI libraries
^^^^^^^^^^^^

+------------------------------+------------+------------+----------------------------------------------------------------+
| Package                      | Old        | New        | Breaking Changes                                               |
+==============================+============+============+================================================================+
| react-syntax-highlighter     | ^15.5.0    | ^16.1.1    | Theme/style import paths may have changed.                     |
+------------------------------+------------+------------+----------------------------------------------------------------+
| remark-gfm                   | ^3.0.1     | ^4.0.1     | Major v4. ESM-only, unified/remark ecosystem v11 alignment.    |
+------------------------------+------------+------------+----------------------------------------------------------------+
| @chakra-ui/react             | 2.4.2      | 2.10.9     | Minor within v2, but large jump. Theme token changes,          |
|                              |            |            | component API additions, some deprecations.                    |
+------------------------------+------------+------------+----------------------------------------------------------------+

Webpack loaders & plugins
^^^^^^^^^^^^^^^^^^^^^^^^^

+--------------------------------+------------+------------+--------------------------------------------------------------+
| Package                        | Old        | New        | Breaking Changes                                             |
+================================+============+============+==============================================================+
| webpack-cli                    | ^4.0.0     | ^6.0.1     | CLI argument changes, config resolution changes.             |
+--------------------------------+------------+------------+--------------------------------------------------------------+
| copy-webpack-plugin            | ^6.0.3     | ^14.0.0    | Config format changes, pattern/globOptions API may differ.   |
+--------------------------------+------------+------------+--------------------------------------------------------------+
| css-loader                     | 5.2.7      | 7.1.4      | Major v6/v7. modules option defaults changed, url() handling |
|                                |            |            | changes.                                                     |
+--------------------------------+------------+------------+--------------------------------------------------------------+
| css-minimizer-webpack-plugin   | ^4.0.0     | ^8.0.0     | Multiple major bumps. Configuration API changes.             |
+--------------------------------+------------+------------+--------------------------------------------------------------+
| style-loader                   | ^1.2.1     | ^4.0.0     | Major v2/v3/v4. injectType option changes, esModule default  |
|                                |            |            | changed.                                                     |
+--------------------------------+------------+------------+--------------------------------------------------------------+
| imports-loader                 | ^1.1.0     | ^5.0.0     | Syntax and configuration API changes.                        |
+--------------------------------+------------+------------+--------------------------------------------------------------+
| mini-css-extract-plugin        | ^1.6.2     | ^2.10.0    | Major v2. experimentalUseImportModule removed, config        |
|                                |            |            | changes.                                                     |
+--------------------------------+------------+------------+--------------------------------------------------------------+
| webpack-manifest-plugin        | ^4.0.0     | ^6.0.1     | Output format and seed option changes.                       |
+--------------------------------+------------+------------+--------------------------------------------------------------+
| babel-loader                   | ^9.1.0     | ^10.0.0    | Major v10. Requires Babel 7.12+, config resolution changes.  |
+--------------------------------+------------+------------+--------------------------------------------------------------+
| clean-webpack-plugin           | ^3.0.0     | ^4.0.0     | Output cleaning behavior changes.                            |
+--------------------------------+------------+------------+--------------------------------------------------------------+

Linting & formatting
^^^^^^^^^^^^^^^^^^^^

+------------------------------+------------+------------+----------------------------------------------------------------+
| Package                      | Old        | New        | Breaking Changes                                               |
+==============================+============+============+================================================================+
| stylelint                    | ^15.10.1   | ^17.4.0    | Major v16/v17. Config format changes, rule renames, stricter   |
|                              |            |            | defaults.                                                      |
+------------------------------+------------+------------+----------------------------------------------------------------+
| stylelint-config-standard    | ^20.0.0    | ^40.0.0    | Rule changes and additions tracking stylelint.                 |
+------------------------------+------------+------------+----------------------------------------------------------------+
| eslint-config-airbnb-ts      | ^17.0.0    | ^18.0.0    | Updated peer deps and rule changes.                            |
+------------------------------+------------+------------+----------------------------------------------------------------+
| eslint-config-prettier       | ^8.6.0     | ^10.1.8    | Some removed rules, adapted for ESLint 9 flat config.          |
+------------------------------+------------+------------+----------------------------------------------------------------+
| eslint-plugin-html           | ^6.0.2     | ^8.1.4     | ESLint 9 flat config support.                                  |
+------------------------------+------------+------------+----------------------------------------------------------------+
| eslint-plugin-promise        | ^4.2.1     | ^7.2.1     | Rule renames and additions, ESLint 9 support.                  |
+------------------------------+------------+------------+----------------------------------------------------------------+
| eslint-plugin-react-hooks    | ^4.5.0     | ^7.0.1     | Stricter rules, new violations detected.                       |
+------------------------------+------------+------------+----------------------------------------------------------------+
| eslint-plugin-standard       | ^4.0.1     | ^5.0.0     | Some rules removed or renamed.                                 |
+------------------------------+------------+------------+----------------------------------------------------------------+

Other
^^^^^

+------------------------------+------------+------------+----------------------------------------------------------------+
| Package                      | Old        | New        | Breaking Changes                                               |
+==============================+============+============+================================================================+
| tsconfig-paths               | ^3.14.2    | ^4.2.0     | Major v4. Path resolution behavior changes.                    |
+------------------------------+------------+------------+----------------------------------------------------------------+


LOW RISK — Minor/patch-level or backwards-compatible changes
------------------------------------------------------------

+--------------------+--------------+--------------+------------------------------------------+
| Package            | Old          | New          | Notes                                    |
+====================+==============+==============+==========================================+
| axios              | ^1.6.0       | ^1.13.6      | Minor within v1, backwards compatible    |
+--------------------+--------------+--------------+------------------------------------------+
| webpack            | ^5.94.0      | ^5.105.4     | Minor within v5                          |
+--------------------+--------------+--------------+------------------------------------------+
| react-icons        | ^5.2.1       | ^5.6.0       | Minor within v5                          |
+--------------------+--------------+--------------+------------------------------------------+
| url-loader         | 4.1.0        | 4.1.1        | Patch bump                               |
+--------------------+--------------+--------------+------------------------------------------+


New packages added
------------------

+------------------------------+------------+----------------------------------------------------------------+
| Package                      | Version    | Notes                                                          |
+==============================+============+================================================================+
| @eslint/eslintrc             | ^3.3.1     | Compatibility layer for eslintrc configs in ESLint 9 flat      |
|                              |            | config                                                         |
+------------------------------+------------+----------------------------------------------------------------+
| @testing-library/dom         | ^10.0.0    | New direct dependency (was likely transitive before)           |
+------------------------------+------------+----------------------------------------------------------------+
| eslint-import-resolver-ts    | ^4.4.3     | TypeScript import resolution for eslint-plugin-import          |
+------------------------------+------------+----------------------------------------------------------------+
| globals                      | ^17.4.0    | Global variable definitions for ESLint 9 flat config           |
+------------------------------+------------+----------------------------------------------------------------+
| jest-environment-jsdom       | ^30.2.0    | jsdom environment for Jest 30 (now a separate package)         |
+------------------------------+------------+----------------------------------------------------------------+


Packages removed
----------------

+------------------------------+---------------------------------------------------------+
| Package                      | Notes                                                   |
+==============================+=========================================================+
| stylelint-config-prettier    | No longer needed with stylelint 17 + prettier 3         |
+------------------------------+---------------------------------------------------------+


Script changes
--------------

- ``lint`` command: removed ``--ignore-path=.eslintignore`` ``--ext .js,.jsx,.ts,.tsx`` flags
  (not supported in ESLint 9)
- ``lint`` command: removed ``tsc`` step (was: ``eslint ... && tsc``, now: ``eslint ...`` only)


Key areas to manually verify
-----------------------------

1. **React 19** — THE BIGGEST CHANGE. All components should be tested. ``forwardRef`` patterns,
   string refs, ``defaultProps`` on function components, legacy context, and many deprecated APIs
   are removed. ``ReactDOM.render`` is gone (must use ``createRoot``).
2. **react-router-dom v7** — Route definitions, navigation hooks, and loader/action patterns may
   have changed.
3. **echarts 6** — All charts should be visually verified; rendering defaults changed.
4. **framer-motion 11** — Animation components should be visually checked; API changed across
   5 major versions.
5. **ESLint 9 (flat config)** — ``.eslintrc`` must be migrated to ``eslint.config.js``. The
   ``@eslint/eslintrc`` compat layer is added.
6. **swagger-ui-dist 5** — The API docs page styling may look different.
7. **prettier 3** — Running ``yarn format`` will reformat many files (trailing commas, etc.).
8. **jest 30** — The test config (``jest.config.js``, ``babel.config.js``) may need updates.
   ``jest-environment-jsdom`` now separate.
9. **react-markdown 10** — Markdown rendering in the UI should be verified. Plugin API changed.
10. **Webpack build pipeline** — Many loaders and plugins jumped multiple major versions. Build
    may need config adjustments.


Impact Assessment
-----------------

.. note::

   This excludes build and lint issues already verified and fixed.


HIGH IMPACT (will likely cause build/test/runtime failures)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**1. React 19** (``^18`` → ``^19.2.4``) — Impact: **LOW** despite being a major bump

- The codebase is clean: all functional components, hooks-only, no class components
- No ``defaultProps``, no string refs, no legacy context, no ``ReactDOM.render``
- 5 files use ``forwardRef`` — still works in React 19 (just no longer required)
- ``@types/react`` jumped to ``^19.2.14`` which may surface type errors on ``children`` prop
  (no longer implicit)
- **Verdict:** Runtime likely fine, but TypeScript compilation may break on implicit ``children``

**2. Jest 30** (``^27`` → ``^30``) — Impact: **MEDIUM-HIGH**

- ``jest.config.js`` exists and uses ``testEnvironment: "jsdom"`` — now requires separate
  ``jest-environment-jsdom`` package (added)
- ``jest-globals-setup.js`` already patches React 19 ``AggregateError`` — someone has partially
  adapted
- ``babel-jest`` jumped to ``^30.2.0`` to match
- Snapshot format changes may cause all snapshot tests to fail on first run (fixable with
  ``--updateSnapshot``)


MEDIUM IMPACT (may cause visual or functional differences)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**3. echarts 6** — Impact: **MEDIUM** (4 files)

- Used in ``ReactECharts.tsx``, ``RunDurationChart.tsx``, ``Calendar.tsx``,
  ``AllTaskDuration.tsx``
- Uses ``init()``, ``setOption()``, ``on()``, ``dispose()`` — core APIs, likely stable
- Chart rendering defaults may change — visual verification needed

**4. react-router-dom v7** (``^6`` → ``^7``) — Impact: **LOW** despite major bump

- Only 13 files use it, all with basic patterns: ``useSearchParams``, ``BrowserRouter``,
  ``MemoryRouter``
- No ``Routes``/``Route`` components, no ``useNavigate``, no ``useParams``, no loaders/actions
- v7 is largely backward-compatible with v6 patterns for these basic APIs
- **Verdict:** Likely works without changes

**5. react-markdown 10** (``^8`` → ``^10``) — Impact: **MEDIUM** (1 file)

- ``ReactMarkdown.tsx`` uses ``Components`` type and ``ReactMarkdownOptions`` from
  ``react-markdown/lib/react-markdown``
- The internal import path ``react-markdown/lib/react-markdown`` will almost certainly break
  in v10
- Plugin API (``remarkPlugins``) and ``components`` prop may have changed

**6. elkjs 0.11** (``^0.7`` → ``^0.11``) — Impact: **LOW-MEDIUM** (4 files)

- Uses ``new ELK()``, ``elk.layout()``, layout options — core API likely stable
- Layout algorithm changes may render DAG graphs differently — visual verification needed

**7. framer-motion 11** (``^6`` → ``^11``) — Impact: **LOW** (1 file only)

- Only ``Tooltip.tsx`` uses it: ``motion.div``, ``AnimatePresence``, ``variants``,
  ``initial``/``animate``/``exit``
- These core APIs survived across versions
- Should work but worth a visual check on tooltip animations


LOW IMPACT
^^^^^^^^^^

- **color v5** — 2 files, simple ``.hex()`` calls, likely compatible
- **camelcase-keys v10** — 1 file, uses ``camelcaseKeys(data, { deep, stopPaths })`` — API
  likely stable but ESM-only since v8
- **prettier 3** — Will reformat files on ``yarn format`` (trailing commas). Not a runtime issue
- **swagger-ui-dist 5** — Not imported in source code (served statically). CSS may look different
- **d3-shape v3** — Not actually used in source code despite being installed
- **moment-timezone 0.6** — Not directly imported in source code
- **type-fest v5** — Types only, may cause compilation errors


Recommended verification order
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Visual check: echarts charts, DAG graph layouts (elkjs), tooltip animations, swagger API docs page
