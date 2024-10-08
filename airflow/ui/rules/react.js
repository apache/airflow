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

/**
 * @import { FlatConfig } from "@typescript-eslint/utils/ts-eslint";
 */
import { fixupPluginRules } from "@eslint/compat";
import jsxA11y from "eslint-plugin-jsx-a11y";
import react from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";

import { ERROR, WARN } from "./levels.js";

/**
 * ESLint React namespace.
 */
export const reactNamespace = "react";

/**
 * ESLint React Hooks namespace.
 */
export const reactHooksNamespace = "react-hooks";

/**
 * ESLint React A11y namespace.
 */
export const jsxA11yNamespace = "jsx-a11y";

/**
 * ESLint React Refresh namespace.
 */
export const reactRefreshNamespace = "react-refresh";

/**
 * ESLint React rules.
 *
 * @see [eslint-plugin-jsx-a11y](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y#supported-rules)
 * @see [eslint-plugin-react](https://github.com/jsx-eslint/eslint-plugin-react#list-of-supported-rules)
 * @see [eslint-plugin-react-hooks](https://github.com/facebook/react/tree/main/packages/eslint-plugin-react-hooks#custom-configuration)
 */
export const reactRules = /** @type {const} @satisfies {FlatConfig.Config} */ ({
  plugins: {
    [jsxA11yNamespace]: jsxA11y,
    [reactHooksNamespace]: fixupPluginRules(reactHooks),
    [reactNamespace]: react,
    [reactRefreshNamespace]: reactRefresh,
  },
  rules: {
    /**
     * Enforce that all elements that require alternative text have
     * meaningful information to relay back to the end user.
     *
     * @see [jsx-a11y/alt-text](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/alt-text.md)
     */
    [`${jsxA11yNamespace}/alt-text`]: ERROR,

    /**
     * Enforces `<a>` values are not exact matches for the phrases "click
     * here", "here", "link", "a link", or "learn more".
     *
     * @see [jsx-a11y/anchor-ambiguous-text](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/anchor-ambiguous-text.md)
     */
    [`${jsxA11yNamespace}/anchor-ambiguous-text`]: ERROR,

    /**
     * Enforce that anchors have content and that the content is accessible
     * to screen readers
     *
     * @see [jsx-a11y/anchor-has-content](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/anchor-has-content.md)
     */
    [`${jsxA11yNamespace}/anchor-has-content`]: ERROR,

    /**
     * Use anchors as links, not as buttons or something else.
     *
     * @see [jsx-a11y/anchor-is-valid](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/anchor-is-valid.md)
     */
    [`${jsxA11yNamespace}/anchor-is-valid`]: ERROR,

    /**
     * Elements with `aria-activedescendant` must be tabbable.
     *
     * @see [jsx-a11y/aria-activedescendant-has-tabindex](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/aria-activedescendant-has-tabindex.md)
     */
    [`${jsxA11yNamespace}/aria-activedescendant-has-tabindex`]: ERROR,

    /**
     * Elements cannot use an invalid ARIA attribute.
     *
     * @see [jsx-a11y/aria-props](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/aria-props.md)
     */
    [`${jsxA11yNamespace}/aria-props`]: ERROR,

    /**
     * ARIA state and property values must be valid.
     *
     * @see [jsx-a11y/aria-proptypes](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/aria-proptypes.md)
     */
    [`${jsxA11yNamespace}/aria-proptypes`]: ERROR,

    /**
     * Elements with ARIA roles must use a valid, non-abstract ARIA role.
     *
     * @see [jsx-a11y/aria-role](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/aria-role.md)
     */
    [`${jsxA11yNamespace}/aria-role`]: ERROR,

    /**
     * Ensures DOM elements that don't support aria roles do not contain the
     * `role` or `aria-*` props.
     *
     * @see [jsx-a11y/aria-unsupported-elements](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/aria-unsupported-elements.md)
     */
    [`${jsxA11yNamespace}/aria-unsupported-elements`]: ERROR,

    /**
     * Ensure the autocomplete attribute is correct and suitable for the
     * form field it is used with.
     *
     * @see [jsx-a11y/autocomplete-valid](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/autocomplete-valid.md)
     */
    [`${jsxA11yNamespace}/autocomplete-valid`]: ERROR,

    /**
     * Enforce `onClick` is accompanied by at least one of the following:
     * `onKeyUp`, `onKeyDown`, `onKeyPress`.
     *
     * @see [jsx-a11y/click-events-have-key-events](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/click-events-have-key-events.md)
     */
    [`${jsxA11yNamespace}/click-events-have-key-events`]: ERROR,

    /**
     * Enforce that a control (an interactive element) has a text label.
     *
     * @see [jsx-a11y/control-has-associated-label](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/control-has-associated-label.md)
     */
    [`${jsxA11yNamespace}/control-has-associated-label`]: ERROR,

    /**
     * Enforce that heading elements (`h1`, `h2`, etc.) have content and
     * that the content is accessible to screen readers.
     *
     * @see [jsx-a11y/heading-has-content](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/heading-has-content.md)
     */
    [`${jsxA11yNamespace}/heading-has-content`]: ERROR,

    /**
     * HTML elements must have the lang prop.
     *
     * @see [jsx-a11y/html-has-lang](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/html-has-lang.md)
     */
    [`${jsxA11yNamespace}/html-has-lang`]: ERROR,

    /**
     * `<iframe>` elements must have a unique title property to indicate its
     * content to the user.
     *
     * @see [jsx-a11y/iframe-has-title](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/iframe-has-title.md)
     */
    [`${jsxA11yNamespace}/iframe-has-title`]: ERROR,

    /**
     * Enforce img alt attribute does not contain the word image, picture,
     * or photo.
     *
     * @see [jsx-a11y/img-redundant-alt](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/img-redundant-alt.md)
     */
    [`${jsxA11yNamespace}/img-redundant-alt`]: ERROR,

    /**
     * Elements with an interactive role and interaction handlers (mouse or
     * key press) must be focusable.
     *
     * @see [jsx-a11y/interactive-supports-focus](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/interactive-supports-focus.md)
     */
    [`${jsxA11yNamespace}/interactive-supports-focus`]: ERROR,

    /**
     * Enforce that a label tag has a text label and an associated control.
     *
     * @see [jsx-a11y/label-has-associated-control](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/label-has-associated-control.md)
     */
    [`${jsxA11yNamespace}/label-has-associated-control`]: ERROR,

    /**
     * The lang prop on the <html> element must be a valid IETF's BCP 47
     * language tag.
     *
     * @see [jsx-a11y/lang](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/lang.md)
     */
    [`${jsxA11yNamespace}/lang`]: ERROR,

    /**
     * Medial elements must have captions.
     *
     * @see [jsx-a11y/media-has-caption](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/media-has-caption.md)
     */
    [`${jsxA11yNamespace}/media-has-caption`]: ERROR,

    /**
     * Enforce `onmouseover`/`onmouseout` are accompanied by
     * `onfocus`/`onblur`.
     *
     * @see [jsx-a11y/mouse-events-have-key-events](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/mouse-events-have-key-events.md)
     */
    [`${jsxA11yNamespace}/mouse-events-have-key-events`]: ERROR,

    /**
     * Forbids `accessKey` prop on element.
     *
     * @see [jsx-a11y/no-access-key](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-access-key.md)
     */
    [`${jsxA11yNamespace}/no-access-key`]: ERROR,

    /**
     * Enforce that `aria-hidden="true"` is not set on focusable elements.
     *
     * @see [jsx-a11y/no-aria-hidden-on-focusable](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-aria-hidden-on-focusable.md)
     */
    [`${jsxA11yNamespace}/no-aria-hidden-on-focusable`]: ERROR,

    /**
     * Enforce that `autoFocus` prop is not used on elements.
     *
     * @see [jsx-a11y/no-autofocus](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-autofocus.md)
     */
    [`${jsxA11yNamespace}/no-autofocus`]: ERROR,

    /**
     * Enforces that no distracting elements are used (`marquee` and
     * `blink`).
     *
     * @see [jsx-a11y/no-distracting-elements](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-distracting-elements.md)
     */
    [`${jsxA11yNamespace}/no-distracting-elements`]: ERROR,

    /**
     * Avoid removing interactivity to interactive elements using
     * non-interactive roles.
     *
     * @see [jsx-a11y/no-interactive-element-to-noninteractive-role](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-interactive-element-to-noninteractive-role.md)
     */
    [`${jsxA11yNamespace}/no-interactive-element-to-noninteractive-role`]:
      ERROR,

    /**
     * Avoid adding interactivity to non-interactive elements (event
     * handlers).
     *
     * @see [jsx-a11y/no-noninteractive-element-interactions](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-noninteractive-element-interactions.md)
     */
    [`${jsxA11yNamespace}/no-noninteractive-element-interactions`]: ERROR,

    /**
     * Avoid adding interactivity to non-interactive elements using
     * interactive roles.
     *
     * @see [jsx-a11y/no-noninteractive-element-to-interactive-role](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-noninteractive-element-to-interactive-role.md)
     */
    [`${jsxA11yNamespace}/no-noninteractive-element-to-interactive-role`]:
      ERROR,

    /**
     * Avoid adding tabindex to non-interactive elements.
     *
     * @see [jsx-a11y/no-noninteractive-tabindex](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-noninteractive-tabindex.md)
     */
    [`${jsxA11yNamespace}/no-noninteractive-tabindex`]: ERROR,

    /**
     * Avoid redundant roles (like <button> with role button).
     *
     * @see [jsx-a11y/no-redundant-roles](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-redundant-roles.md)
     */
    [`${jsxA11yNamespace}/no-redundant-roles`]: ERROR,

    /**
     * Avoid interactions with static elements.
     *
     * @see [jsx-a11y/no-static-element-interactions](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/no-static-element-interactions.md)
     */
    [`${jsxA11yNamespace}/no-static-element-interactions`]: ERROR,

    /**
     * Use tags instead of roles (like <button> instead of role button).
     *
     * @see [jsx-a11y/prefer-tag-over-role](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/prefer-tag-over-role.md)
     */
    [`${jsxA11yNamespace}/prefer-tag-over-role`]: ERROR,

    /**
     * Some roles require more aria attributes.
     *
     * @see [jsx-a11y/role-has-required-aria-props](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/role-has-required-aria-props.md)
     */
    [`${jsxA11yNamespace}/role-has-required-aria-props`]: ERROR,

    /**
     * Enforce valid aria attributes for each role.
     *
     * @see [jsx-a11y/role-supports-aria-props](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/role-supports-aria-props.md)
     */
    [`${jsxA11yNamespace}/role-supports-aria-props`]: ERROR,

    /**
     * Ensures scope attribute is used in th elements.
     *
     * @see [jsx-a11y/scope](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/scope.md)
     */
    [`${jsxA11yNamespace}/scope`]: ERROR,

    /**
     * Only use `0` and `-1` as `tabIndex` values.
     *
     * @see [jsx-a11y/tabindex-no-positive](https://github.com/jsx-eslint/eslint-plugin-jsx-a11y/blob/main/docs/rules/tabindex-no-positive.md)
     */
    [`${jsxA11yNamespace}/tabindex-no-positive`]: ERROR,

    /**
     * Enforces having all the values used in an effect or memoizable value
     * listed as dependencies.
     */
    [`${reactHooksNamespace}/exhaustive-deps`]: ERROR,

    /**
     * Enforces React Rules of Hooks.
     *
     * @see [Rules of Hooks](https://react.dev/warnings/invalid-hook-call-warning).
     */
    [`${reactHooksNamespace}/rules-of-hooks`]: ERROR,

    /**
     * Prevent usage of button elements without an explicit type attribute.
     *
     * @see [react/button-has-type](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/button-has-type.md)
     */
    [`${reactNamespace}/button-has-type`]: ERROR,

    /**
     * Enforce using `onChange` or `readonly` attribute when `checked` is
     * used.
     *
     * @see [react/checked-requires-onchange-or-readonly](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/checked-requires-onchange-or-readonly.md)
     */
    [`${reactNamespace}/checked-requires-onchange-or-readonly`]: ERROR,

    /**
     * Enforce consistent usage of destructuring assignment of props, state,
     * and context
     *
     * @see [react/destructuring-assignment](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/destructuring-assignment.md)
     */
    [`${reactNamespace}/destructuring-assignment`]: [ERROR, "always"],

    /**
     * Disallow old element.
     *
     * @see [react/forbid-elements](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/forbid-elements.md)
     */
    [`${reactNamespace}/forbid-elements`]: [
      ERROR,
      {
        forbid: [
          { element: "acronym", message: "Use <abbr> instead." },
          { element: "big", message: "Use CSS instead." },
          { element: "center", message: "Use CSS instead." },
          { element: "dir", message: "Use <ul> instead." },
          { element: "embed", message: "Use <object> instead." },
          { element: "font", message: "Use CSS instead." },
          { element: "frame", message: "Use <iframe> instead." },
          { element: "frameset", message: "Use <iframe> instead." },
          { element: "marquee", message: "Use CSS instead." },
          { element: "nobr", message: "Use CSS instead." },
          {
            element: "noembed",
            message: "Put fallback code inside an <object> instead.",
          },
          { element: "noframes", message: "Use <iframe> instead." },
          {
            element: "param",
            message:
              "Use the <object> element with a data attribute to set the URL of an external resource.",
          },
          { element: "plaintext", message: "Use <pre> instead." },
          { element: "rb", message: "Use UTF-8 instead." },
          { element: "rtc", message: "Use UTF-8 and CSS instead." },
          { element: "strike", message: "Use <del> or <s> instead." },
          {
            element: "tt",
            message: "Use <code>, <kbd>, <samp>, <var> or <pre> instead.",
          },
          { element: "xmp", message: "Use <code> or <pre> instead." },
        ],
      },
    ],

    /**
     * Enforce a specific function type for function components.
     *
     * @see [react/function-component-definition](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/function-component-definition.md)
     */
    [`${reactNamespace}/function-component-definition`]: [
      ERROR,
      {
        namedComponents: "arrow-function",
        unnamedComponents: "arrow-function",
      },
    ],

    /**
     * Ensure destructuring and symmetric naming of useState hook value and
     * setter variables
     *
     * @see [react/hook-use-state](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/hook-use-state.md)
     */
    [`${reactNamespace}/hook-use-state`]: ERROR,

    /**
     * Enforce `sandbox` attribute on `iframe` elements.
     *
     * @see [react/iframe-missing-sandbox](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/iframe-missing-sandbox.md)
     */
    [`${reactNamespace}/iframe-missing-sandbox`]: ERROR,

    /**
     * Enforce boolean attributes notation in JSX to never set it explicitly.
     *
     * @see [react/jsx-boolean-value](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-boolean-value.md)
     */
    [`${reactNamespace}/jsx-boolean-value`]: [
      ERROR,
      "never",
      { assumeUndefinedIsFalse: true },
    ],

    /**
     * Enforce curly braces or braces in JSX props and/or children.
     *
     * @see [react/jsx-curly-brace-presence](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-curly-brace-presence.md)
     */
    [`${reactNamespace}/jsx-curly-brace-presence`]: [ERROR, "never"],

    /**
     * Enforce shorthand form for fragments `<></>`.
     *
     * @see [react/jsx-fragments](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-fragments.md)
     */
    [`${reactNamespace}/jsx-fragments`]: [ERROR, "syntax"],

    /**
     * Disallow missing `key` props in iterators/collection literals.
     *
     * @see [react/jsx-key](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-key.md)
     */
    [`${reactNamespace}/jsx-key`]: ERROR,

    /**
     * Enforce JSX maximum depth to 5.
     *
     * @see [react/jsx-max-depth](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-max-depth.md)
     */
    [`${reactNamespace}/jsx-max-depth`]: [ERROR, { max: 5 }],

    /**
     * Disallow comments from being inserted as text nodes.
     *
     * @see [react/jsx-no-comment-textnodes](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-no-comment-textnodes.md)
     */
    [`${reactNamespace}/jsx-no-comment-textnodes`]: ERROR,

    /**
     * Prevent react contexts from taking non-stable values.
     *
     * @see [react/jsx-no-constructed-context-values](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-no-constructed-context-values.md)
     */
    [`${reactNamespace}/jsx-no-constructed-context-values`]: ERROR,

    /**
     * Disallow problematic leaked values from being rendered.
     *
     * @see [react/jsx-no-leaked-render](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-no-leaked-render.md)
     */
    [`${reactNamespace}/jsx-no-leaked-render`]: ERROR,

    /**
     * Prevent usage of `javascript:` URLs.
     *
     * @see [react/jsx-no-script-url](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-no-script-url.md)
     */
    [`${reactNamespace}/jsx-no-script-url`]: ERROR,

    /**
     * Prevent usage of unsafe `target='_blank'` without `rel="noreferrer"`.
     *
     * @see [react/jsx-no-target-blank](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-no-target-blank.md)
     */
    [`${reactNamespace}/jsx-no-target-blank`]: ERROR,

    /**
     * Disallow unnecessary fragments.
     *
     * @see [react/jsx-no-useless-fragment](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-no-useless-fragment.md)
     */
    [`${reactNamespace}/jsx-no-useless-fragment`]: ERROR,

    /**
     * Enforce PascalCase for user-defined JSX components.
     *
     * @see [react/jsx-pascal-case](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-pascal-case.md)
     */
    [`${reactNamespace}/jsx-pascal-case`]: ERROR,

    /**
     * Disallow JSX prop spreading the same identifier multiple times.
     *
     * @see [react/jsx-props-no-spread-multi](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/jsx-props-no-spread-multi.md)
     */
    [`${reactNamespace}/jsx-props-no-spread-multi`]: ERROR,

    /**
     * Prevent usage of Array index in keys.
     *
     * @see [react/no-array-index-key](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/no-array-index-key.md)
     */
    [`${reactNamespace}/no-array-index-key`]: ERROR,

    /**
     * Prevent usage of dangerous JSX properties.
     *
     * @see [react/no-danger](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/no-danger.md)
     */
    [`${reactNamespace}/no-danger`]: ERROR,

    /**
     * Disallow usage of deprecated methods.
     *
     * @see [react/no-deprecated](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/no-deprecated.md)
     */
    [`${reactNamespace}/no-deprecated`]: ERROR,

    /**
     * Prevent multiple component definition per file.
     *
     * @see [react/no-multi-comp](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/no-multi-comp.md)
     */
    [`${reactNamespace}/no-multi-comp`]: WARN,

    /**
     * Enforce that namespaces are not used in React elements.
     *
     * @see [react/no-namespace](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/no-namespace.md)
     */
    [`${reactNamespace}/no-namespace`]: ERROR,

    /**
     * Disallow usage of referential-type variables as default param in
     * functional component.
     *
     * @see [react/no-object-type-as-default-prop](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/no-object-type-as-default-prop.md)
     */
    [`${reactNamespace}/no-object-type-as-default-prop`]: ERROR,

    /**
     * Disallow using string references.
     *
     * @see [react/no-string-refs](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/no-string-refs.md)
     */
    [`${reactNamespace}/no-string-refs`]: ERROR,

    /**
     * Disallow unescaped HTML entities from appearing in markup.
     *
     * @see [react/no-unescaped-entities](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/no-unescaped-entities.md)
     */
    [`${reactNamespace}/no-unescaped-entities`]: ERROR,

    /**
     * Prevent creating unstable components inside components.
     *
     * @see [react/no-unstable-nested-components](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/no-unstable-nested-components.md)
     */
    [`${reactNamespace}/no-unstable-nested-components`]: ERROR,

    /**
     * Enforce that props are read-only.
     *
     * @see [react/prefer-read-only-props](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/prefer-read-only-props.md)
     */
    [`${reactNamespace}/prefer-read-only-props`]: ERROR,

    /**
     * Disallow extra closing tags for components without children.
     *
     * @see [react/self-closing-comp](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/self-closing-comp.md)
     */
    [`${reactNamespace}/self-closing-comp`]: ERROR,

    /**
     * Enforce property declarations alphabetical sorting.
     *
     * @see [react/sort-prop-types](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/sort-prop-types.md)
     */
    [`${reactNamespace}/sort-prop-types`]: ERROR,

    /**
     * Disallow void DOM elements from receiving children.
     *
     * @see [react/void-dom-elements-no-children](https://github.com/jsx-eslint/eslint-plugin-react/blob/HEAD/docs/rules/void-dom-elements-no-children.md)
     */
    [`${reactNamespace}/void-dom-elements-no-children`]: ERROR,

    /**
     * Validate that your components can safely be updated with fast refresh.
     *
     * @see [Allow constant export](https://github.com/ArnaudBarre/eslint-plugin-react-refresh?tab=readme-ov-file#allowconstantexport-v040)
     */
    [`${reactRefreshNamespace}/only-export-components`]: [
      WARN,
      { allowConstantExport: true },
    ],
  },
  settings: { react: { version: "18" } },
});
