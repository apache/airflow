/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Copy Button - Progressive Enhancement
// Adds copy buttons to elements with data-copy-target attribute
(function() {
  // Only add copy buttons if Clipboard API is available
  if (!navigator.clipboard) return;

  // Find all elements that should have copy buttons
  document.querySelectorAll('[data-copy-target]').forEach(container => {
    const targetId = container.dataset.copyTarget;
    const targetElement = document.getElementById(targetId);

    if (!targetElement) return;

    const copyButton = document.createElement('button');
    copyButton.className = 'copy';
    copyButton.title = 'Copy';
    copyButton.setAttribute('aria-label', 'Copy to clipboard');
    copyButton.innerHTML = '<svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" /></svg>';

    copyButton.addEventListener('click', async () => {
      const textToCopy = targetElement.textContent || '';
      try {
        await navigator.clipboard.writeText(textToCopy);

        // Show success state
        copyButton.innerHTML = '<svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" style="color:var(--color-green-400)"></path></svg>';

        // Reset after 2 seconds
        setTimeout(() => {
          copyButton.innerHTML = '<svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" /></svg>';
        }, 2000);
      } catch (err) {
        console.error('Failed to copy:', err);
      }
    });

    container.appendChild(copyButton);
  });
})();
