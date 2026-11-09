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

// Filters for the /durable/ page. State lives in the query string
// (?capability=deferrable&provider=amazon&type=sensor&q=glue) so a filtered
// view can be shared.
(function() {
  const searchInput = document.getElementById('durable-search');
  const capabilityButtons = document.querySelectorAll('.capability-btn');
  const providerSelect = document.getElementById('durable-provider-filter');
  const typeSelect = document.getElementById('durable-type-filter');
  const status = document.getElementById('durable-status');
  const emptyState = document.getElementById('durable-empty');
  const groups = document.querySelectorAll('.durable-page .provider-group');

  if (!searchInput || !providerSelect || !typeSelect) return;

  const state = { capability: 'all', provider: 'all', type: 'all', q: '' };
  let debounceTimer;

  function matches(item) {
    const search = state.q.toLowerCase();
    const capability =
      state.capability === 'all' ||
      (state.capability === 'durable' && item.dataset.durable === 'true') ||
      (state.capability === 'deferrable' && item.dataset.deferrable === 'true');
    return capability &&
      (state.type === 'all' || item.dataset.type === state.type) &&
      (!search || item.dataset.name.includes(search));
  }

  function apply() {
    let visible = 0;
    groups.forEach(group => {
      const inProvider = state.provider === 'all' || group.dataset.provider === state.provider;
      let groupVisible = 0;
      group.querySelectorAll('.module').forEach(item => {
        const show = inProvider && matches(item);
        item.style.display = show ? '' : 'none';
        if (show) groupVisible++;
      });
      group.style.display = groupVisible ? '' : 'none';
      group.querySelector('h2 .count').textContent = groupVisible;
      visible += groupVisible;
    });
    emptyState.hidden = visible !== 0;
    status.textContent = visible + ' module' + (visible === 1 ? '' : 's');
    updateURL();
  }

  function setCapability(capability) {
    state.capability = capability;
    capabilityButtons.forEach(btn => {
      const active = btn.dataset.capability === capability;
      btn.classList.toggle('active', active);
      btn.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
  }

  function updateURL() {
    const params = new URLSearchParams();
    Object.entries(state).forEach(([key, value]) => {
      if (value && value !== 'all') params.set(key, value);
    });
    const qs = params.toString();
    history.replaceState(null, '', window.location.pathname + (qs ? '?' + qs : ''));
  }

  // Assigning a value a select has no option for leaves it blank, so an
  // unknown provider or type in the URL falls back to "all".
  function selectFromURL(select, value) {
    select.value = value || 'all';
    if (!select.value) select.value = 'all';
    return select.value;
  }

  function readURL() {
    const params = new URLSearchParams(window.location.search);
    const capability = params.get('capability');
    if (Array.from(capabilityButtons).some(btn => btn.dataset.capability === capability)) {
      setCapability(capability);
    }
    state.provider = selectFromURL(providerSelect, params.get('provider'));
    state.type = selectFromURL(typeSelect, params.get('type'));
    state.q = (params.get('q') || '').trim();
    searchInput.value = state.q;
  }

  searchInput.addEventListener('input', () => {
    state.q = searchInput.value.trim();
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(apply, 200);
  });

  capabilityButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      setCapability(btn.dataset.capability);
      apply();
    });
  });

  providerSelect.addEventListener('change', () => {
    state.provider = providerSelect.value;
    apply();
  });

  typeSelect.addEventListener('change', () => {
    state.type = typeSelect.value;
    apply();
  });

  readURL();
  apply();
})();
