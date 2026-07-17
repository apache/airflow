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

(function() {
  var installPip = document.getElementById('install-pip');
  var versionSelect = document.getElementById('version-select');
  var extrasSelect = document.getElementById('extras-select');
  var extraDepsInfo = document.getElementById('extra-deps-info');
  var extraDepsList = document.getElementById('extra-deps-list');
  var extrasDataEl = document.getElementById('extras-data');
  var moduleSearch = document.getElementById('module-search');
  var moduleTabs = document.querySelectorAll('.module-tab');
  var categoryBtns = document.querySelectorAll('.category-btn');
  var moduleItems = document.querySelectorAll('.modules .module');
  var copyImportBtns = document.querySelectorAll('.copy-import');

  if (!installPip) return;

  var packageName = installPip.dataset.package;

  var extrasData = {};
  if (extrasDataEl) {
    try {
      extrasData = JSON.parse(extrasDataEl.textContent || '{}') || {};
    } catch (e) {
      extrasData = {};
    }
  }

  var currentExtra = '';
  var currentType = 'all';
  var currentCategory = '';
  var currentSearch = '';

  function updateInstallCommand() {
    var version = versionSelect ? versionSelect.value : '';
    var extraPart = currentExtra ? '[' + currentExtra + ']' : '';
    var versionPart = version ? '==' + version : '';
    var pkg = packageName + extraPart + versionPart;
    installPip.textContent = 'pip install ' + pkg;
  }

  if (versionSelect) {
    versionSelect.addEventListener('change', function() {
      updateInstallCommand();
      var providerUrl = versionSelect.dataset.providerUrl;
      var version = versionSelect.value;
      if (providerUrl && /^[\w./-]+$/.test(providerUrl) && /^[\w.]+$/.test(version)) {
        window.location.href = providerUrl + version + '/';
      }
    });

    // Dynamic version list: fetch current versions so old pages stay current.
    // The statically-rendered dropdown serves as a fallback if the fetch fails.
    var providerId = versionSelect.dataset.providerUrl
      .replace(/.*\/providers\//, '').replace(/\/$/, '');
    var base = window.__REGISTRY_BASE__ || '/';
    fetch(base + 'api/providers/' + encodeURIComponent(providerId) + '/versions.json')
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(data) {
        if (!data || !data.versions) return;
        // Derive current version from URL path, not the static dropdown value
        // (the dropdown may only contain the latest version as a placeholder).
        var pathParts = window.location.pathname.replace(/\/$/, '').split('/');
        var current = pathParts[pathParts.length - 1] || versionSelect.value;
        versionSelect.innerHTML = '';
        var found = false;
        data.versions.forEach(function(v) {
          var opt = document.createElement('option');
          opt.value = v;
          opt.textContent = v + (v === data.latest ? ' (latest)' : '');
          if (v === current) { opt.selected = true; found = true; }
          versionSelect.appendChild(opt);
        });
        // If the current version isn't in the deployed list, restore the
        // original dropdown so the user doesn't lose context.
        if (!found) {
          versionSelect.innerHTML = '';
          var fallback = document.createElement('option');
          fallback.value = current;
          fallback.textContent = current;
          fallback.selected = true;
          versionSelect.appendChild(fallback);
          data.versions.forEach(function(v) {
            var opt = document.createElement('option');
            opt.value = v;
            opt.textContent = v + (v === data.latest ? ' (latest)' : '');
            versionSelect.appendChild(opt);
          });
        }
        // Update the "View latest" banner to reflect the actual latest
        // version from S3 (it may differ from the static build-time value).
        var latestLink = document.getElementById('latest-link');
        var latestLabel = document.getElementById('latest-version-label');
        if (latestLink && latestLabel && data.latest) {
          var providerUrl = versionSelect.dataset.providerUrl;
          latestLabel.textContent = data.latest;
          latestLink.href = providerUrl + data.latest + '/';
        }
      })
      .catch(function() { /* keep static dropdown as fallback */ });
  }

  if (extrasSelect) {
    extrasSelect.addEventListener('change', function() {
      currentExtra = extrasSelect.value;
      updateInstallCommand();

      if (currentExtra && extrasData[currentExtra]) {
        if (extraDepsList) extraDepsList.textContent = extrasData[currentExtra].join(', ');
        if (extraDepsInfo) extraDepsInfo.hidden = false;
      } else {
        if (extraDepsInfo) extraDepsInfo.hidden = true;
      }
    });
  }

  // Module filtering
  function filterModules() {
    moduleItems.forEach(function(item) {
      var name = item.dataset.name || '';
      var type = item.dataset.type || '';
      var category = item.dataset.category || '';

      var matchesType = currentType === 'all' || type === currentType;
      var matchesCategory = !currentCategory || category === currentCategory;
      var matchesSearch = !currentSearch || name.includes(currentSearch.toLowerCase());

      item.style.display = (matchesType && matchesCategory && matchesSearch) ? '' : 'none';
    });
  }

  moduleTabs.forEach(function(tab) {
    tab.addEventListener('click', function() {
      moduleTabs.forEach(function(t) { t.classList.remove('active'); });
      tab.classList.add('active');
      currentType = tab.dataset.type || 'all';
      filterModules();
    });
  });

  categoryBtns.forEach(function(btn) {
    btn.addEventListener('click', function() {
      categoryBtns.forEach(function(b) { b.classList.remove('active'); });
      btn.classList.add('active');
      currentCategory = btn.dataset.category || '';
      filterModules();
    });
  });

  // Debounced module search
  var searchTimer;
  if (moduleSearch) {
    moduleSearch.addEventListener('input', function(e) {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(function() {
        currentSearch = e.target.value.trim();
        filterModules();
      }, 150);
    });
  }

  // Copy import buttons
  copyImportBtns.forEach(function(btn) {
    btn.addEventListener('click', async function() {
      var text = btn.dataset.copy;
      if (!text) return;
      try {
        await navigator.clipboard.writeText(text);
        btn.classList.add('copied');
        btn.setAttribute('aria-label', 'Copied!');
        setTimeout(function() {
          btn.classList.remove('copied');
          btn.setAttribute('aria-label', 'Copy import statement');
        }, 2000);
      } catch (err) {
        console.warn('Clipboard write failed:', err);
      }
    });
  });

  // Module highlight from URL hash
  function highlightModule() {
    var hash = window.location.hash ? window.location.hash.slice(1) : null;
    var target = hash ? decodeURIComponent(hash) : null;
    if (!target) return;

    var card = document.getElementById(target);
    if (card && card.classList.contains('module')) {
      card.classList.add('highlighted');
      setTimeout(function() {
        card.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }, 300);
    }
  }

  highlightModule();
})();
