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

// Theme Toggle
(function() {
  const themeToggle = document.getElementById('theme-toggle');
  const html = document.documentElement;
  const sunIcon = document.getElementById('theme-icon-sun');
  const moonIcon = document.getElementById('theme-icon-moon');

  // Get current theme from color-scheme property
  function getCurrentTheme() {
    return html.style.colorScheme || 'dark';
  }

  // Update icon visibility based on current theme
  function updateIcons() {
    const isLight = getCurrentTheme() === 'light';
    if (sunIcon) sunIcon.style.display = isLight ? 'block' : 'none';
    if (moonIcon) moonIcon.style.display = isLight ? 'none' : 'block';
  }

  // Initialize icons on load
  updateIcons();

  // Toggle theme
  if (themeToggle) {
    themeToggle.addEventListener('click', () => {
      const currentTheme = getCurrentTheme();
      const newTheme = currentTheme === 'light' ? 'dark' : 'light';

      html.style.colorScheme = newTheme;
      localStorage.setItem('theme', newTheme);
      updateIcons();
    });
  }
})();
