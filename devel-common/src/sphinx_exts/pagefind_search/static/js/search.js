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

let pagefind = null;
let searchResults = [];
let selectedIndex = 0;

async function initPagefind() {
  if (!pagefind) {
    try {
      const pagefindPath = window.PAGEFIND_PATH || './_pagefind/pagefind.js';
      const absoluteUrl = new URL(pagefindPath, document.baseURI || window.location.href).href;

      const pf = await import(/* webpackIgnore: true */ absoluteUrl);
      pagefind = pf;
      await pagefind.options({
        excerptLength: 15,
        ranking: {
          termFrequency: 1.0,
          termSaturation: 0.7,
          termSimilarity: 7.5,  // Maximum boost for exact/similar matches
          pageLength: 0         // No penalty for long pages
        }
      });
    } catch (e) {
      console.error('Failed to load Pagefind:', e);
      displaySearchError('Search index not available. Please rebuild the documentation.');
      return null;
    }
  }
  return pagefind;
}

function displaySearchError(message) {
  const resultsDiv = document.getElementById('search-results');
  if (resultsDiv) {
    resultsDiv.innerHTML = `
      <div class="search-modal__no-results">
        <p>${message}</p>
      </div>
    `;
  }
}

function openSearch() {
  const modal = document.getElementById('search-modal');
  const input = document.getElementById('search-input');
  if (modal && input) {
    modal.style.display = 'flex';
    input.focus();
    document.body.style.overflow = 'hidden';
  }
}

function closeSearch() {
  const modal = document.getElementById('search-modal');
  const input = document.getElementById('search-input');
  const results = document.getElementById('search-results');

  if (modal) modal.style.display = 'none';
  if (input) input.value = '';
  if (results) results.innerHTML = '';

  document.body.style.overflow = '';
  searchResults = [];
  selectedIndex = 0;
}

function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

async function performSearch(query) {
  const resultsContainer = document.getElementById('search-results');
  if (!resultsContainer) return;

  if (!query || query.length < 2) {
    resultsContainer.innerHTML = '';
    return;
  }

  const pf = await initPagefind();
  if (!pf) {
    resultsContainer.innerHTML = `
      <div class="search-modal__no-results">
        <p>Search index not available</p>
        <p class="search-modal__no-results-hint">Index is built automatically during 'make html'</p>
      </div>
    `;
    return;
  }

  try {
    const search = await pf.search(query);
    searchResults = await Promise.all(
      search.results.slice(0, 10).map(r => r.data())
    );

    renderResults(searchResults);
  } catch (e) {
    console.error('Search error:', e);
    resultsContainer.innerHTML = `
      <div class="search-modal__no-results">
        <p>Search temporarily unavailable</p>
        <p class="search-modal__no-results-hint">Please try again later</p>
      </div>
    `;
  }
}

function renderResults(results) {
  const container = document.getElementById('search-results');
  if (!container) return;

  if (results.length === 0) {
    container.innerHTML = `
      <div class="search-modal__no-results">
        <p>No results found</p>
        <p class="search-modal__no-results-hint">Try different keywords or check spelling</p>
      </div>
    `;
    return;
  }

  container.innerHTML = results.map((result, index) => `
    <a
      href="${result.url}"
      class="search-modal__result-item ${index === selectedIndex ? 'search-modal__result-item--selected' : ''}"
      data-index="${index}"
      role="option"
      aria-selected="${index === selectedIndex}"
    >
      <div class="search-modal__result-icon">
        <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
          <path d="M1 2.5A1.5 1.5 0 0 1 2.5 1h11A1.5 1.5 0 0 1 15 2.5v11a1.5 1.5 0 0 1-1.5 1.5h-11A1.5 1.5 0 0 1 1 13.5v-11zM2.5 2a.5.5 0 0 0-.5.5v11a.5.5 0 0 0 .5.5h11a.5.5 0 0 0 .5-.5v-11a.5.5 0 0 0-.5-.5h-11z"/>
          <path d="M4 5.5a.5.5 0 0 1 .5-.5h7a.5.5 0 0 1 0 1h-7a.5.5 0 0 1-.5-.5zm0 3a.5.5 0 0 1 .5-.5h7a.5.5 0 0 1 0 1h-7a.5.5 0 0 1-.5-.5zm0 3a.5.5 0 0 1 .5-.5h4a.5.5 0 0 1 0 1h-4a.5.5 0 0 1-.5-.5z"/>
        </svg>
      </div>
      <div class="search-modal__result-content">
        <div class="search-modal__result-title">${result.meta?.title || 'Untitled'}</div>
        <div class="search-modal__result-breadcrumb">${result.url.replace(/^\/docs\//, '').replace(/\.html$/, '')}</div>
        ${result.excerpt ? `<div class="search-modal__result-excerpt">${result.excerpt}</div>` : ''}
      </div>
    </a>
  `).join('');
}

function handleKeyboardNav(e) {
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    selectedIndex = Math.min(selectedIndex + 1, searchResults.length - 1);
    renderResults(searchResults);
    scrollToSelected();
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    selectedIndex = Math.max(selectedIndex - 1, 0);
    renderResults(searchResults);
    scrollToSelected();
  } else if (e.key === 'Enter' && searchResults.length > 0) {
    e.preventDefault();
    window.location.href = searchResults[selectedIndex].url;
  } else if (e.key === 'Escape') {
    closeSearch();
  }
}

function scrollToSelected() {
  const selected = document.querySelector('.search-modal__result-item--selected');
  if (selected) {
    selected.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  document.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
      e.preventDefault();
      openSearch();
    }
  });

  const searchInput = document.getElementById('search-input');
  if (searchInput) {
    searchInput.addEventListener('input', debounce((e) => {
      selectedIndex = 0;
      performSearch(e.target.value);
    }, 150));

    searchInput.addEventListener('keydown', handleKeyboardNav);
  }

  const backdrop = document.querySelector('.search-modal__backdrop');
  if (backdrop) {
    backdrop.addEventListener('click', closeSearch);
  }

  const closeButton = document.querySelector('.search-modal__close');
  if (closeButton) {
    closeButton.addEventListener('click', closeSearch);
  }
});

window.openSearch = openSearch;
window.closeSearch = closeSearch;
