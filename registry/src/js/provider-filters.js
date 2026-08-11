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
  const searchInput = document.getElementById('provider-search');
  const lifecycleButtons = document.querySelectorAll('.lifecycle-btn');
  const categorySelect = document.getElementById('category-filter');
  const sortSelect = document.getElementById('provider-sort');
  const providerGrid = document.getElementById('provider-grid');
  const emptyState = document.getElementById('empty-state');
  const providerItems = document.querySelectorAll('.provider-item');

  if (!searchInput || !providerGrid || !emptyState) return;

  let currentLifecycle = 'all';
  let currentCategory = 'all';
  let currentSearch = '';
  let debounceTimer;

  // Kept in sync by hand with normalize() in src/_data/providerKeywordMatch.js:
  // that module runs at build time under CommonJS and this file is a browser
  // IIFE, so the two can't share it. If the two drift, a query like
  // 'pydantic-ai' stops finding the integration named "Pydantic AI".
  function normalize(text) {
    return text.toLowerCase().replace(/[-_\s]+/g, ' ');
  }

  // The card prints the full distribution name under the title, so that string
  // has to be a working query. Matching against it directly would make
  // 'apache', 'airflow' and 'providers' match every card, so drop the shared
  // prefix off the query instead and match the id alone.
  const PACKAGE_PREFIX = normalize('apache-airflow-providers-');

  function normalizeSearch(text) {
    const search = normalize(text);
    return search.startsWith(PACKAGE_PREFIX) ? search.slice(PACKAGE_PREFIX.length) : search;
  }

  function filterProviders() {
    let visibleCount = 0;
    const search = normalizeSearch(currentSearch);

    providerItems.forEach(item => {
      const lifecycle = item.dataset.lifecycle;
      const name = item.dataset.name || '';
      const id = item.dataset.id || '';
      const categories = item.dataset.categories || '';
      const integrations = item.dataset.integrations || '';
      const services = item.dataset.services || '';

      const matchesLifecycle = currentLifecycle === 'all' || lifecycle === currentLifecycle;
      const matchesSearch = normalize(name).includes(search) ||
        normalize(id).includes(search) ||
        normalize(integrations).includes(search) ||
        normalize(services).includes(search);
      const matchesCategory = currentCategory === 'all' ||
        categories.split(',').includes(currentCategory);

      if (matchesLifecycle && matchesSearch && matchesCategory) {
        item.style.display = 'block';
        visibleCount++;
      } else {
        item.style.display = 'none';
      }
    });

    if (visibleCount === 0) {
      emptyState.style.display = 'block';
      providerGrid.style.display = 'none';
    } else {
      emptyState.style.display = 'none';
      providerGrid.style.display = 'grid';
    }

    const filterStatus = document.getElementById('filter-status');
    if (filterStatus) {
      filterStatus.textContent = visibleCount === 0
        ? 'No providers match your filters'
        : visibleCount + ' provider' + (visibleCount !== 1 ? 's' : '') + ' shown';
    }

    updateURL();
  }

  function updateURL() {
    const params = new URLSearchParams();
    if (currentCategory !== 'all') params.set('category', currentCategory);
    if (currentLifecycle !== 'all') params.set('lifecycle', currentLifecycle);
    if (currentSearch) params.set('q', currentSearch);
    const qs = params.toString();
    const newURL = window.location.pathname + (qs ? '?' + qs : '');
    history.replaceState(null, '', newURL);
  }

  function readURLParams() {
    const params = new URLSearchParams(window.location.search);
    const cat = params.get('category');
    const lc = params.get('lifecycle');
    const q = params.get('q');

    if (cat && categorySelect) {
      currentCategory = cat;
      categorySelect.value = cat;
    }
    if (lc) {
      currentLifecycle = lc;
      lifecycleButtons.forEach(btn => {
        const isMatch = btn.dataset.lifecycle === lc;
        btn.classList.toggle('active', isMatch);
        btn.setAttribute('aria-pressed', isMatch ? 'true' : 'false');
      });
    }
    if (q) {
      currentSearch = q;
      searchInput.value = q;
    }
  }

  searchInput.addEventListener('input', (e) => {
    currentSearch = e.target.value.trim();
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(filterProviders, 200);
  });

  lifecycleButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      lifecycleButtons.forEach(b => {
        b.classList.remove('active');
        b.setAttribute('aria-pressed', 'false');
      });
      btn.classList.add('active');
      btn.setAttribute('aria-pressed', 'true');
      currentLifecycle = btn.dataset.lifecycle;
      filterProviders();
    });
  });

  if (categorySelect) {
    categorySelect.addEventListener('change', () => {
      currentCategory = categorySelect.value;
      filterProviders();
    });
  }

  function sortProviders() {
    const sortBy = sortSelect.value;
    const items = Array.from(providerItems);

    items.sort((a, b) => {
      switch (sortBy) {
        case 'downloads':
          return Number(b.dataset.downloads || 0) - Number(a.dataset.downloads || 0);
        case 'name':
          return (a.dataset.name || '').localeCompare(b.dataset.name || '');
        case 'updated':
          return (b.dataset.updated || '').localeCompare(a.dataset.updated || '');
        default:
          return 0;
      }
    });

    items.forEach(item => providerGrid.appendChild(item));
  }

  if (sortSelect) {
    sortSelect.addEventListener('change', sortProviders);
  }

  readURLParams();
  sortProviders();
  if (currentCategory !== 'all' || currentLifecycle !== 'all' || currentSearch) {
    filterProviders();
  }
})();
