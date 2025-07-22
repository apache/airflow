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

Accessibility Tests
===================

Automated accessibility testing for Airflow's React UI using Lighthouse CI.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Overview
--------

Accessibility tests run automatically on PRs that modify UI files in ``airflow-core/src/airflow/ui/``. 
The tests use the `Lighthouse CI Action <https://github.com/marketplace/actions/lighthouse-ci-action>`_ 
to ensure WCAG compliance and color contrast standards.

**Requirements:**
- 90% minimum accessibility score
- 100% color contrast compliance
- Comprehensive ARIA and semantic HTML validation

When Tests Run
--------------

- **Pull requests** to ``main`` modifying UI files
- **Pushes** to ``main`` branch
- **Manual trigger** via GitHub Actions

Local Testing
-------------

Run accessibility tests locally before submitting PRs:

.. code-block:: bash

    # Install Lighthouse CLI
    npm install -g @lhci/cli

    # Build and test the UI
    cd airflow-core/src/airflow/ui
    pnpm build
    cd ../../../../
    lhci autorun --config .lighthouserc.js

Understanding Results
---------------------

**PR Comments** show:
- Overall accessibility score (target: 90+)
- Color contrast status (must be 100%)
- Number of issues found
- Link to detailed reports

**Common Issues:**
- **Color contrast failures**: Use Chakra UI's accessible color tokens
- **Missing ARIA labels**: Add proper labels to interactive elements
- **Keyboard navigation**: Ensure all controls are keyboard accessible

Quick Fixes
-----------

.. code-block:: typescript

    // Good: Use semantic color tokens
    <Button colorScheme="blue">Click me</Button>
    
    // Good: Proper ARIA labels
    <input aria-label="Search tasks" />
    
    // Good: Semantic HTML
    <button type="submit">Submit</button>

Configuration
-------------

Tests are configured in:
- ``.lighthouserc.js`` - Test thresholds and settings
- ``.github/workflows/lighthouse-ci.yml`` - CI workflow

Resources
---------

- `WCAG Guidelines <https://www.w3.org/WAI/WCAG21/quickref/>`_
- `Chakra UI Accessibility <https://chakra-ui.com/docs/styled-system/accessibility>`_
- `axe DevTools <https://www.deque.com/axe/devtools/>`_ - Browser extension for testing 