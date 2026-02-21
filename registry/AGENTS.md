 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Agent Guidelines for Airflow Registry

This document contains rules, patterns, and guidelines for working on the Airflow Registry project.

## Project Overview

## Core Principles


### 0. Minimal JavaScript

- Keep JavaScript to minimum necessary
- Theme toggle only
- No frameworks (no React, Vue, etc.)
- Progressive enhancement approach

## 1. Quality Standards

- **Visual consistency**: Changes should match existing design system and styling
- **Semantic HTML**: Use proper elements and structure
- **Maintainable CSS**: Descriptive class names, leverage cascade
- **Light/Dark mode**: Full support with smooth transitions
- **Responsive**: Mobile-first, works on all screen sizes

### 2. Semantic CSS & HTML

Follow the principles from https://css-tricks.com/semantic-class-names/

#### 2.1 Class Names Describe CONTENT, Not Appearance

Class names should answer "what is this?" not "how should it look?"

**Good examples:**

- `.provider-card` - it's a card displaying provider information
- `.connection-types` - a section showing connection types
- `.dependencies` - shows dependency information
- `.stats` - displays statistical data

**Bad examples:**

- `.blue-box` - describes appearance (what if design changes to green?)
- `.flex-row` - describes layout implementation
- `.mt-4` - describes spacing value
- `.text-lg` - describes typography

**Why this matters:**

- Design changes shouldn't require HTML updates
- Code is self-documenting (you understand structure without seeing CSS)
- Multiple elements can share appearance without sharing meaning

#### 2.2 Use CSS Cascade to Avoid Redundant Prefixes

When an element only makes sense within its parent, use descendant selectors instead of prefixing class names.

**Good - leveraging cascade:**

```html
<section class="technical">
  <div class="python-version">...</div>
  <div class="connections">...</div>
</section>
```

```css
.technical { /* section styles */ }
.technical h3 { /* heading styles */ }
.technical .python-version { /* specific section */ }
.technical code { /* all code in technical section */ }
```

**Bad - redundant prefixes:**

```html
<section class="technical">
  <div class="technical-python">...</div>
  <div class="technical-connections">...</div>
</section>
```

```css
.technical { /* section styles */ }
.technical-heading { /* redundant prefix */ }
.technical-python { /* redundant prefix */ }
.technical-code { /* redundant prefix */ }
```

**Why this matters:**

- Less typing, less repetition
- Clearer hierarchy in CSS
- If `.python-version` only appears in `.technical`, the prefix adds no information

#### 2.3 Leverage Semantic HTML5 Elements

Use the right HTML element for the job. This improves accessibility, SEO, and reduces need for classes.

**Semantic elements and their uses:**

- `<header>` - Page or section header
- `<footer>` - Page or section footer
- `<nav>` - Navigation menus
- `<main>` - Main content of the page
- `<section>` - Thematic grouping of content
- `<article>` - Self-contained content
- `<aside>` - Side content/sidebars
- `<dl>`, `<dt>`, `<dd>` - Definition lists (key-value pairs)
- `<details>`, `<summary>` - Collapsible content

**Example - Statistics:**

```html
<!-- Good: Semantic -->
<dl class="stats">
  <div>
    <dt>1,000+</dt>
    <dd>Providers</dd>
  </div>
</dl>
```

```css
.stats dt { font-size: var(--text-3xl); }
.stats dd { font-size: var(--text-sm); }
```

**Example - Provider detail header:**

```html
<!-- Good: Semantic structure -->
<header class="card">
  <div class="top">
    <div class="logo">...</div>
    <div class="details">
      <h1>Amazon</h1>
      <p class="description">...</p>
    </div>
  </div>

  <div class="stats">...</div>
</header>
```

```css
.provider-detail-page header { /* styles */ }
.provider-detail-page header h1 { /* title */ }
.provider-detail-page header .stats { /* grid of stats */ }
```

#### 2.4 When to Add Classes vs Use Element Selectors

**Use element selectors when:**

- There's only one of that element in the scope
- The element's meaning is clear from context
- Example: `.provider-detail-page header h1` - there's only one main heading

**Add a class when:**

- Multiple instances need different styling
- The role isn't obvious from the element alone
- You need to target it from JavaScript
- Example: `.python-version`, `.connections`, `.dependencies` - all are `<div>` but serve different purposes

**Prefer simpler selectors:**

- `section code` over `section .code-snippet`
- `details summary` over `.details-trigger`
- `.connections a` over `.connection-badge`

#### 2.5 No Utility Classes

Utility classes describe HOW something looks, not WHAT it is.

**Remove these patterns:**

- Spacing: `.mb-2`, `.py-12`, `.gap-4`, `.px-6`, `.space-y-4`
- Layout: `.flex`, `.grid`, `.grid-cols-2`, `.items-center`
- Display: `.block`, `.inline-block`, `.hidden`
- Text: `.text-center`, `.text-lg`, `.font-bold`, `.uppercase`
- Colors: `.text-blue-500`, `.bg-gray-100`

**Instead, style contextually:**

```css
/* Good */
.technical h3 {
  font-size: var(--text-sm);
  font-weight: var(--font-semibold);
  margin-bottom: var(--space-3);
  display: flex;
  align-items: center;
}

/* Bad */
<h3 class="text-sm font-semibold mb-3 flex items-center">
```

#### 2.6 Contextual Component Styling

Components should be styled based on WHERE they appear, not through generic modifier classes.

**No generic button classes:**

```css
/* Bad */
.btn { }
.btn-primary { }
.btn-secondary { }
```

**Style contextually instead:**

```css
/* Good */
.hero .popular-providers a {
  padding: var(--space-2) var(--space-4);
  background: var(--bg-tertiary);
  border: 1px solid var(--border-primary);
  border-radius: var(--radius-lg);
}

.provider-detail-page .actions .btn-primary {
  background: var(--accent-primary);
  color: var(--color-navy-900);
}
```

**Why this matters:**

- Buttons in hero look different from buttons in provider detail
- Context determines appearance
- Reduces need for modifier classes

## CSS Architecture

### File Structure

```
src/css/
├── tokens.css          # Design tokens (colors, spacing, fonts)
├── main.css            # Main styles with imports
└── base/
    ├── reset.css       # CSS reset
    └── typography.css  # Base typography
```

### Semantic Section Pattern

Each major section follows this pattern:

```css
/* Section container */
.section-name {
  padding: var(--space-20) 0;
  background: var(--bg-primary);
}

/* Nested elements using cascade */
.section-name header {
  /* Header styles */
}

.section-name h2 {
  /* Title styles */
}

.section-name .some-element {
  /* Element styles */
}
```

### Design Tokens

Use CSS custom properties defined in `tokens.css`:

- Colors: `--color-navy-900`, `--color-cyan-400`, `--color-gray-700`, etc.
- Spacing: `--space-4` (1rem), `--space-8` (2rem), etc.
- Typography: `--text-base`, `--text-lg`, `--font-semibold`, etc.
- Theme variables: `--bg-primary`, `--text-primary`, `--border-primary`

**IMPORTANT**: Always use CSS custom properties instead of hardcoded color values:

- ✅ Good: `border: 1px solid var(--color-gray-700);`
- ❌ Bad: `border: 1px solid #334155;`

#### When to Use Semantic vs Direct Color Variables

When defining new variables in `tokens.css`, follow this hierarchy:

1. **Prefer semantic variables** (with -light/-dark suffixes) over direct color variables:
   - ✅ Good: `--color-input-bg: light-dark(var(--bg-tertiary-light), var(--bg-tertiary-dark));`
   - ❌ Bad: `--color-input-bg: light-dark(var(--color-gray-100), var(--color-navy-700));`

2. **Only use direct color variables** when:
   - Defining the base semantic variables themselves (e.g., `--bg-tertiary-light: var(--color-gray-100);`)
   - No appropriate semantic variable exists for your use case
   - The color combination is intentionally different from existing semantic patterns

3. **In component CSS** (main.css):
   - Use computed semantic variables: `background: var(--bg-primary);`
   - Or use inline `light-dark()` with semantic variables for contextual colors

This creates a clear hierarchy: base colors → semantic variables → computed variables → usage

### Light/Dark Mode

The theme system uses the CSS `color-scheme` property and `light-dark()` function:

- Default theme is dark mode
- Theme switching sets `document.documentElement.style.colorScheme = 'light' | 'dark'`

- Theme computed variables defined in `tokens.css` with progressive enhancement:

  ```css
  /* Progressive enhancement: fallback then light-dark() */
  --bg-primary: var(--bg-primary-light);
  --bg-primary: light-dark(var(--bg-primary-light), var(--bg-primary-dark));
  ```

- Most elements use computed theme variables:

  ```css
  .element {
    background: var(--bg-primary);
    color: var(--text-primary);
  }
  ```

- Contextual colors use inline `light-dark()`:

  ```css
  .kbd {
    border-color: light-dark(var(--border-primary-light), var(--color-navy-600));
  }
  ```

**Progressive Enhancement**: Each theme variable is declared twice in tokens.css:

1. First with light value as fallback for browsers without `light-dark()` support
2. Then with `light-dark()` which overrides in supporting browsers

## Development Workflow

### Feature Development Checklist

When adding or modifying features:

1. **Plan semantic HTML structure** using appropriate elements
2. **Write semantic CSS** using cascade pattern
3. **Add light/dark mode styles** for all elements
4. **Test responsiveness** across different screen sizes
5. **Verify consistency** with existing design system

### Common Issues

- **Colors not switching in dark mode**: Ensure element uses computed theme variables like `var(--bg-primary)` or inline `light-dark()`
- **Missing dark mode colors**: Check that theme variable has both `-light` and `-dark` variants defined in tokens.css
- **Wrong border colors**: Check color tokens are indigo not gray

## CSS Deletion Guidelines

When removing non-semantic CSS:

1. **Search for usage first**: Use grep to find where classes are used
2. **Confirm removal**: If only in CSS and not in refactored templates, delete it
3. **Test visually**: Take screenshots before and after
4. **Common deletions**:
   - Grid/flex utilities: `.grid`, `.flex`, `.items-center`, etc.
   - Spacing utilities: `.mb-*`, `.py-*`, `.gap-*`, etc.
   - Button classes: `.btn`, `.btn-primary`, etc.
   - Text utilities: `.text-center`, `.text-lg`, etc.

## Quality Standards

- **Visual consistency**: Changes should match existing design system and styling
- **Semantic HTML**: Use proper elements and structure
- **Maintainable CSS**: Descriptive class names, leverage cascade
- **Light/Dark mode**: Full support with smooth transitions (except during screenshots)
- **Responsive**: Mobile-first, works on all screen sizes

## Reference Files

Key files in the project:

- `src/css/main.css` - Main styles
- `src/css/tokens.css` - Design tokens
- `src/` - All page templates (index, provider-detail, providers, explore, stats)

## Development Server

Local development server runs at http://localhost:8080

## Deployment Architecture

The registry is built in the `apache/airflow` repo and served at `airflow.apache.org/registry/`.

### How it works

1. **Build**: `registry-build.yml` extracts metadata, builds the 11ty site, and syncs to S3.
   Supports two modes:
   - **Full build** (no `provider` input): extracts all ~99 providers (~12 min)
   - **Incremental build** (`provider=amazon`): extracts one provider (~30s), merges
     with existing data from S3 via `merge_registry_data.py`, then builds the full site
2. **S3 buckets**: `{live|staging}-docs-airflow-apache-org/registry/` (same bucket as docs, different prefix)
3. **Serving**: Apache HTTPD at `airflow.apache.org` rewrites `/registry/*` to CloudFront, which serves from S3
4. **Auto-trigger**: When `publish-docs-to-s3.yml` publishes provider docs, its
   `update-registry` job calls `registry-build.yml` with the provider ID for an
   incremental registry update

### Path prefix

- Production: `REGISTRY_PATH_PREFIX=/registry/` (default)
- Local dev: `REGISTRY_PATH_PREFIX=/` (set automatically by `pnpm dev`)
- All internal links use 11ty's `| url` filter, which prepends the prefix

### Changes needed in `apache/airflow-site` repo (separate PR)

1. **`.htaccess` rewrite rule** - Add a rule to proxy `/registry/*` through CloudFront:

```apache
RewriteRule ^registry/(.*)$ https://<cloudfront-distribution>.cloudfront.net/registry/$1 [P,L]
```

2. **Navigation link** - Add a "Registry" entry to the site header/nav in the Hugo templates,
   pointing to `/registry/`.

These changes mirror the existing `/docs/*` rewrite pattern.

## Troubleshooting

### Can't find where a class is used

```bash
# Search in templates
grep -r "class-name" src/

# Search in CSS
grep "\.class-name" src/css/
```

### Color issues

1. Use browser DevTools to inspect computed styles
2. Check both light and dark mode
3. Verify color tokens in `tokens.css`
4. Ensure computed theme variables are used correctly
