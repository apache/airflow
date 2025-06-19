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
import unicorn from "eslint-plugin-unicorn";

import { ERROR, WARN } from "./levels.js";

/**
 * ESLint `unicorn` namespace.
 */
export const unicornNamespace = "unicorn";

/**
 * ESLint `unicorn` rules.
 * @see [eslint-plugin-unicorn](https://github.com/sindresorhus/eslint-plugin-unicorn#rules)
 */
export const unicornRules = /** @type {const} @satisfies {FlatConfig.Config} */ ({
  plugins: { [unicornNamespace]: unicorn },
  rules: {
    /**
     * Improve regexes by making them shorter, consistent, and safer.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const regex = /[0-9]/;
     * const regex = /[^0-9]/;
     * const regex = /[a-zA-Z0-9_]/;
     * const regex = /[a-z0-9_]/i;
     * const regex = /[^a-zA-Z0-9_]/;
     * const regex = /[^a-z0-9_]/i;
     * const regex = /[0-9]\.[a-zA-Z0-9_]\-[^0-9]/i;
     *
     * // ✅ Correct
     * const regex = /\d/;
     * const regex = /\D/;
     * const regex = /\w/;
     * const regex = /\w/i;
     * const regex = /\W/;
     * const regex = /\W/i;
     * const regex = /\d\.\w\-\D/i;
     * ```
     * @see [unicorn/better-regex](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/better-regex.md)
     */
    [`${unicornNamespace}/better-regex`]: ERROR,

    /**
     * Catch error must be named `error` or `somethingError` (`_` is allowed unless is used).
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * try {} catch (badName) {}
     * try {} catch (_) { console.log(_); } // `_` is not allowed if it's used
     * promise.catch(badName => {});
     * promise.then(undefined, badName => {});
     *
     * // ✅ Correct
     * try {} catch (error) {}
     * promise.catch(error => {});
     * promise.then(undefined, error => {});
     * try {} catch (_) { console.log(foo); } // `_` is allowed when it's not used
     * try {} catch (fsError) {}
     * ```
     * @see [unicorn/catch-error-name](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/catch-error-name.md)
     */
    [`${unicornNamespace}/catch-error-name`]: ERROR,

    /**
     * Use destructured variables over properties.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const { a } = foo;
     * console.log(a, foo.b);
     * console.log(foo.a);
     *
     * const { a: { b } } = foo;
     * console.log(foo.a.c);
     *
     * const { bar } = foo;
     * const { a } = foo.bar;
     *
     * // ✅ Correct
     * const { a } = foo;
     * console.log(a);
     * console.log(foo.a, foo.b);
     * console.log(a, foo.b());
     *
     * const { a } = foo.bar;
     * console.log(foo.bar);
     * ```
     * @see [unicorn/consistent-destructuring](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/consistent-destructuring.md)
     */
    [`${unicornNamespace}/consistent-destructuring`]: ERROR,

    /**
     * Prefer consistent types when spreading a ternary in an array literal.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const array = [a, ...(foo ? [b, c] : """)];
     * const array = [a, ...(foo ? "bc" : [])];
     *
     * // ✅ Correct
     * const array = [a, ...(foo ? [b, c] : [])];
     * const array = [a, ...(foo ? "bc" : "")];
     * ```
     * @see [unicorn/consistent-empty-array-spread](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/consistent-empty-array-spread.md)
     */
    [`${unicornNamespace}/consistent-empty-array-spread`]: ERROR,

    /**
     * Move function definitions to the highest possible scope.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const doFoo = foo => bar => bar === "bar";
     *
     * // ✅ Correct
     * const doBar = bar => bar === "bar";
     * const doFoo = foo => doBar;
     * ```
     * @see [unicorn/consistent-function-scoping](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/consistent-function-scoping.md)
     */
    [`${unicornNamespace}/consistent-function-scoping`]: ERROR,

    /**
     * Enforce passing a message value when creating a built-in error.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * throw Error();
     * throw Error("");
     * throw new TypeError();
     * const error = new AggregateError(errors);
     *
     * // ✅ Correct
     * throw Error("Unexpected property.");
     * throw new TypeError("Array expected.");
     * const error = new AggregateError(errors, "Promises rejected.");
     * ```
     * @see [unicorn/error-message](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/error-message.md)
     */
    [`${unicornNamespace}/error-message`]: ERROR,

    /**
     * Require escape sequences to use uppercase values.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = "\xa9";
     * const foo = "\ud834";
     * const foo = "\u{1d306}";
     * const foo = "\ca";
     *
     * // ✅ Correct
     * const foo = "\xA9";
     * const foo = "\uD834";
     * const foo = "\u{1D306}";
     * const foo = "\cA";
     * ```
     * @see [unicorn/escape-case](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/escape-case.md)
     */
    [`${unicornNamespace}/escape-case`]: ERROR,

    /**
     * Enforce specifying rules to disable in eslint-disable comments.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * console.log(message); // eslint-disable-line
     * // eslint-disable-next-line
     * console.log(message);
     *
     * // ✅ Correct
     * console.log(message); // eslint-disable-line no-console
     * // eslint-disable-next-line no-console
     * console.log(message);
     * ```
     * @see [unicorn/no-abusive-eslint-disable](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-abusive-eslint-disable.md)
     */
    [`${unicornNamespace}/no-abusive-eslint-disable`]: ERROR,

    /**
     * Disallow anonymous functions and classes as the default export.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * export default class {}
     * export default function () {}
     * export default () => {};
     *
     * // ✅ Correct
     * export default class Foo {}
     * export default function foo () {}
     * const foo = () => {};
     * export default foo;
     * ```
     * @see [unicorn/no-anonymous-default-export](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-anonymous-default-export.md)
     */
    [`${unicornNamespace}/no-anonymous-default-export`]: ERROR,

    /**
     * Disallow using await in Promise method parameters.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * Promise.all([await promise, anotherPromise]);
     * Promise.allSettled([await promise, anotherPromise]);
     * Promise.any([await promise, anotherPromise]);
     * Promise.race([await promise, anotherPromise]);
     *
     * // ✅ Correct
     * Promise.all([promise, anotherPromise]);
     * Promise.allSettled([promise, anotherPromise]);
     * Promise.any([promise, anotherPromise]);
     * Promise.race([promise, anotherPromise]);
     * ```
     * @see [unicorn/no-await-in-promise-methods](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-await-in-promise-methods.md)
     */
    [`${unicornNamespace}/no-await-in-promise-methods`]: ERROR,

    /**
     * Do not use leading/trailing space between `console.log` parameters.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * console.log("abc ", "def");
     * console.log("abc", " def");
     *
     * // ✅ Correct
     * console.log("abc", "def");
     * ```
     * @see [unicorn/no-console-spaces](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-console-spaces.md)
     */
    [`${unicornNamespace}/no-console-spaces`]: ERROR,

    /**
     * Enforce the use of Unicode escapes instead of hexadecimal escapes.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = "\x1B";
     *
     * // ✅ Correct
     * const foo = "\u001B";
     * ```
     * @see [unicorn/no-hex-escape](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-hex-escape.md)
     */
    [`${unicornNamespace}/no-hex-escape`]: ERROR,

    /**
     * Require `Array.isArray` instead of `instanceof Array`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * [1, 2, 3] instanceof Array;
     *
     * // ✅ Correct
     * Array.isArray([1, 2, 3]);
     * ```
     * @see [unicorn/no-instanceof-array](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-instanceof-array.md)
     */
    [`${unicornNamespace}/no-instanceof-array`]: ERROR,

    /**
     * Disallow invalid options in `fetch()` and new `Request()`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const response = await fetch("/", { body: "foo=bar" });
     * const request = new Request("/", { body: "foo=bar" });
     * const response = await fetch("/", { method: "GET", body: "foo=bar" });
     * const request = new Request("/", { method: "GET", body: "foo=bar" });
     *
     * // ✅ Correct
     * const response = await fetch("/", { method: "POST", body: "foo=bar" });
     * const request = new Request("/", { method: "POST", body: "foo=bar" });
     * ```
     * @see [unicorn/no-invalid-fetch-options](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-invalid-fetch-options.md)
     */
    [`${unicornNamespace}/no-invalid-fetch-options`]: ERROR,

    /**
     * Prevent calling `EventTarget#removeEventListener` with the result of an expression.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * window.removeEventListener("click", fn.bind(window));
     * window.removeEventListener("click", () => {});
     * window.removeEventListener("click", function () {});
     *
     * // ✅ Correct
     * window.removeEventListener("click", listener);
     * window.removeEventListener("click", getListener());
     * ```
     * @see [unicorn/no-invalid-remove-event-listener](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-invalid-remove-event-listener.md)
     */
    [`${unicornNamespace}/no-invalid-remove-event-listener`]: ERROR,

    /**
     * Disallow using `.length` as the end argument of
     * `{Array,String,TypedArray}#slice()`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = string.slice(1, string.length);
     * const foo = array.slice(1, array.length);
     *
     * // ✅ Correct
     * const foo = string.slice(1);
     * const foo = bar.slice(1, baz.length);
     * ```
     * @see [unicorn/no-length-as-slice-end](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-length-as-slice-end.md)
     */
    [`${unicornNamespace}/no-length-as-slice-end`]: ERROR,

    /**
     * Disallow a magic number as the `depth` argument in `Array#flat`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = array.flat(2);
     * const foo = array.flat(99);
     *
     * // ✅ Correct
     * const foo = array.flat();
     * const foo = array.flat(Number.POSITIVE_INFINITY);
     * const foo = array.flat(Infinity);
     * const foo = array.flat(depth);
     * ```
     * @see [unicorn/no-magic-array-flat-depth](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-magic-array-flat-depth.md)
     */
    [`${unicornNamespace}/no-magic-array-flat-depth`]: ERROR,

    /**
     * Disallow negated conditions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * !a ? c : b;
     *
     * // ✅ Correct
     * a ? b : c;
     * ```
     * @see [unicorn/no-negated-condition](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-negated-condition.md)
     */
    [`${unicornNamespace}/no-negated-condition`]: ERROR,

    /**
     * Disallow negated expression in equality check.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (!foo === bar) {}
     * if (!foo !== bar) {}
     *
     * // ✅ Correct
     * if (foo !== bar) {}
     * if (!(foo === bar)) {}
     * ```
     * @see [unicorn/no-negation-in-equality-check](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-negation-in-equality-check.md)
     */
    [`${unicornNamespace}/no-negation-in-equality-check`]: ERROR,

    /**
     * Disallow `new Array()`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const array = new Array(length);
     * const array = new Array(onlyElement);
     * const array = new Array(...unknownArgumentsList);
     *
     * // ✅ Correct
     * const array = Array.from({ length });
     * const array = [onlyElement];
     * const array = [...items];
     * ```
     * @see [unicorn/no-new-array](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-new-array.md)
     */
    [`${unicornNamespace}/no-new-array`]: ERROR,

    /**
     * Enforce the use of `Buffer.from` and `Buffer.alloc` instead of the deprecated `new Buffer()`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const buffer = new Buffer("7468697320697320612074c3a97374", "hex");
     * const buffer = new Buffer([0x62, 0x75, 0x66, 0x66, 0x65, 0x72]);
     * const buffer = new Buffer(10);
     *
     * // ✅ Correct
     * const buffer = Buffer.from("7468697320697320612074c3a97374", "hex");
     * const buffer = Buffer.from([0x62, 0x75, 0x66, 0x66, 0x65, 0x72]);
     * const buffer = Buffer.alloc(10);
     * ```
     * @see [unicorn/no-new-buffer](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-new-buffer.md)
     */
    [`${unicornNamespace}/no-new-buffer`]: ERROR,

    /**
     * Disallow the use of the null literal.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * let foo = null;
     * if (bar == null) {}
     *
     * // ✅ Correct
     * let foo;
     * const foo = Object.create(null);
     * if (foo === null) {}
     * ```
     * @see [Reasoning](https://lou.cx/articles/we-don-t-need-null/)
     * @see [unicorn/no-null](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-null.md)
     */
    [`${unicornNamespace}/no-null`]: WARN,

    /**
     * Disallow passing single-element arrays to Promise methods.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = await Promise.all([promise]);
     * const foo = await Promise.any([promise]);
     * const foo = await Promise.race([promise]);
     * const promise = Promise.all([nonPromise]);
     *
     * // ✅ Correct
     * const foo = await promise;
     * const promise = Promise.resolve(nonPromise);
     * const foo = await Promise.all(promises);
     * const foo = await Promise.any([promise, anotherPromise]);
     * const [{ value: foo, reason: error }] = await Promise.allSettled([promise]);
     * ```
     * @see [unicorn/no-single-promise-in-promise-methods](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-single-promise-in-promise-methods.md)
     */
    [`${unicornNamespace}/no-single-promise-in-promise-methods`]: ERROR,

    /**
     * Enforce the use of built-in methods instead of unnecessary polyfills.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const assign = require("object-assign");
     * ```
     * @see [unicorn/no-unnecessary-polyfills](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-unnecessary-polyfills.md)
     */
    [`${unicornNamespace}/no-unnecessary-polyfills`]: ERROR,

    /**
     * Disallow useless fallback when spreading in object literals.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const object = { ...(foo || {}) };
     * const object = { ...(foo ?? {}) };
     *
     * // ✅ Correct
     * const object = { ...foo };
     * const object = { ...(foo && {}) };
     * const array = [...(foo || [])];
     * ```
     * @see [unicorn/no-useless-fallback-in-spread](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-useless-fallback-in-spread.md)
     */
    [`${unicornNamespace}/no-useless-fallback-in-spread`]: ERROR,

    /**
     * Disallow useless array length check.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (array.length === 0 || array.every(Boolean));
     * if (array.length !== 0 && array.some(Boolean));
     * if (array.length > 0 && array.some(Boolean));
     * const isAllTrulyOrEmpty = array.length === 0 || array.every(Boolean);
     *
     * // ✅ Correct
     * if (array.every(Boolean));
     * if (array.some(Boolean));
     * const isAllTrulyOrEmpty = array.every(Boolean);
     * if (array.length === 0 || anotherCheck() || array.every(Boolean));
     * const isNonEmptyAllTrulyArray = array.length > 0 && array.every(Boolean);
     * const isEmptyArrayOrAllTruly = array.length === 0 || array.some(Boolean);
     * ```
     * @see [unicorn/no-useless-length-check](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-useless-length-check.md)
     */
    [`${unicornNamespace}/no-useless-length-check`]: ERROR,

    /**
     * Disallow unnecessary spread.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const array = [firstElement, ...[secondElement], thirdElement];
     * const object = { firstProperty, ...{ secondProperty }, thirdProperty };
     * foo(firstArgument, ...[secondArgument], thirdArgument);
     * const object = new Foo(firstArgument, ...[secondArgument], thirdArgument);
     * const set = new Set([...iterable]);
     * const results = await Promise.all([...iterable]);
     * for (const foo of [...set]);
     * const foo = function* () { yield* [...anotherGenerator()]; };
     * const foo = bar => [...bar.map(x => x * 2)];
     *
     * // ✅ Correct
     * const array = [firstElement, secondElement, thirdElement];
     * const object = { firstProperty, secondProperty, thirdProperty };
     * foo(firstArgument, secondArgument, thirdArgument);
     * const object = new Foo(firstArgument, secondArgument, thirdArgument);
     * const set = new Set(iterable);
     * const results = await Promise.all(iterable);
     * for (const foo of set);
     * const foo = function* () { yield* anotherGenerator; };
     * const foo = bar => bar.map(x => x * 2);
     * ```
     * @see [unicorn/no-useless-spread](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-useless-spread.md)
     */
    [`${unicornNamespace}/no-useless-spread`]: ERROR,

    /**
     * Disallow useless case in switch statements.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     *
     * // ✅ Correct
     * ```
     * @see [unicorn/no-useless-switch-case](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/no-useless-switch-case.md)
     */
    [`${unicornNamespace}/no-useless-switch-case`]: ERROR,

    /**
     * Enforce the style of numeric separators by correctly grouping digits.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * switch (foo) {
     *   case 1:
     *   default:
     *     handleDefaultCase();
     *     break;
     * }
     *
     * // ✅ Correct
     * switch (foo) {
     *   case 1:
     *   case 2:
     *     handleDefaultCase();
     *     break;
     * }
     *
     *
     * switch (foo) {
     *   case 1:
     *     handleCase1();
     *   default:
     *     handleDefaultCase();
     *     break;
     * }
     * ```
     * @see [unicorn/numeric-separators-style](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/numeric-separators-style.md)
     */
    [`${unicornNamespace}/numeric-separators-style`]: ERROR,

    /**
     * Prefer `Element#addEventListener` and `Element#removeEventListener` over on-functions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * foo.onclick = () => {};
     * foo.onkeydown = () => {};
     * foo.bar.onclick = onClick;
     * foo.onclick = null;
     *
     * // ✅ Correct
     * foo.addEventListener("click", () => {});
     * foo.addEventListener("keydown", () => {});
     * foo.bar.addEventListener("click", onClick);
     * foo.removeEventListener("click", onClick);
     * ```
     * @see [unicorn/prefer-add-event-listener](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-add-event-listener.md)
     */
    [`${unicornNamespace}/prefer-add-event-listener`]: ERROR,

    /**
     * Prefer `Array#find` and `Array#findLast` over the first or last element from `Array#filter`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const item = array.filter(isUnicorn)[0];
     * const item = array.filter(isUnicorn).at(-1);
     * const item = array.filter(isUnicorn).shift();
     * const item = array.filter(isUnicorn).pop();
     * const [item] = array.filter(isUnicorn);
     * [item] = array.filter(isUnicorn);
     *
     * // ✅ Correct
     * const item = array.find(isUnicorn);
     * item = array.find(isUnicorn);
     * const item = array.findLast(isUnicorn);
     * ```
     * @see [unicorn/prefer-array-find](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-array-find.md)
     */
    [`${unicornNamespace}/prefer-array-find`]: ERROR,

    /**
     * Prefer `Array#flatMap` over `Array#map(…).flat()`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = bar.map(unicorn).flat();
     * const foo = bar.map(unicorn).flat(1);
     *
     * // ✅ Correct
     * const foo = bar.flatMap(unicorn);
     * const foo = bar.map(unicorn).flat(2);
     * const foo = bar.map(unicorn).foo().flat();
     * const foo = bar.flat().map(unicorn);
     * ```
     * @see [unicorn/prefer-array-flat-map](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-array-flat-map.md)
     */
    [`${unicornNamespace}/prefer-array-flat-map`]: ERROR,

    /**
     * Prefer `Array#flat` over legacy techniques to flatten arrays.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = array.flatMap(x => x);
     * const foo = array.reduce((a, b) => a.concat(b), []);
     * const foo = array.reduce((a, b) => [...a, ...b], []);
     * const foo = [].concat(maybeArray);
     * const foo = [].concat(...array);
     * const foo = [].concat.apply([], array);
     * const foo = Array.prototype.concat.apply([], array);
     * const foo = Array.prototype.concat.call([], maybeArray);
     * const foo = Array.prototype.concat.call([], ...array);
     * const foo = _.flatten(array);
     * const foo = lodash.flatten(array);
     * const foo = underscore.flatten(array);
     *
     * // ✅ Correct
     * const foo = array.flat();
     * const foo = [maybeArray].flat();
     * ```
     * @see [unicorn/prefer-array-flat](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-array-flat.md)
     */
    [`${unicornNamespace}/prefer-array-flat`]: ERROR,

    /**
     * Prefer `Array#indexOf` and `Array#lastIndexOf` over `Array#findIndex` and `Array#findLastIndex` when looking for the index of an item.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const index = foo.findIndex(x => x === "foo");
     * const index = foo.findIndex(x => "foo" === x);
     * const index = foo.findLastIndex(x => x === "foo");
     * const index = foo.findLastIndex(x => "foo" === x);
     *
     * // ✅ Correct
     * const index = foo.indexOf("foo");
     * const index = foo.findIndex(x => x === undefined);
     * const index = foo.findIndex(x => x !== "foo");
     * const index = foo.findIndex((x, index) => x === index);
     * const index = foo.findIndex(x => (x === "foo") && isValid());
     * const index = foo.findIndex(x => y === "foo");
     * const index = foo.findIndex(x => y.x === "foo");
     * const index = foo.lastIndexOf("foo");
     * const index = foo.findLastIndex(x => x == undefined);
     * const index = foo.findLastIndex(x => x !== "foo");
     * const index = foo.findLastIndex((x, index) => x === index);
     * const index = foo.findLastIndex(x => (x === "foo") && isValid());
     * const index = foo.findLastIndex(x => y === "foo");
     * const index = foo.findLastIndex(x => y.x === "foo");
     * ```
     * @see [unicorn/prefer-array-index-of](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-array-index-of.md)
     */
    [`${unicornNamespace}/prefer-array-index-of`]: ERROR,

    /**
     * Prefer `Array#some` over `Array#filter(…).length` check and `Array#find` or `Array#findLast`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const hasUnicorn = array.filter(isUnicorn).length > 0;
     * const hasUnicorn = array.filter(isUnicorn).length !== 0;
     * const hasUnicorn = array.filter(isUnicorn).length >= 1;
     * const foo = array.find(isUnicorn) ? bar : baz;
     * const hasUnicorn = array.find(isUnicorn) !== undefined;
     * const hasUnicorn = array.find(isUnicorn) != null;
     * const foo = array.findLast(isUnicorn) ? bar : baz;
     * const hasUnicorn = array.findLast(isUnicorn) !== undefined;
     * const hasUnicorn = array.findLast(isUnicorn) != null;
     * const hasUnicorn = array.findIndex(isUnicorn) !== -1;
     * const hasUnicorn = array.findLastIndex(isUnicorn) !== -1;
     *
     * // ✅ Correct
     * const hasUnicorn = array.some(isUnicorn);
     * ```
     * @see [unicorn/prefer-array-some](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-array-some.md)
     */
    [`${unicornNamespace}/prefer-array-some`]: ERROR,

    /**
     * Prefer `Date.now` to get the number of milliseconds since the Unix Epoch.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = new Date().getTime();
     * const foo = new Date().valueOf();
     * const foo = +new Date;
     * const foo = Number(new Date());
     *
     * // ✅ Correct
     * const foo = Date.now();
     * ```
     * @see [unicorn/prefer-date-now](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-date-now.md)
     */
    [`${unicornNamespace}/prefer-date-now`]: ERROR,

    /**
     * Prefer default parameters over reassignment.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * function abc(foo) {
     *   foo ||= "bar";
     * }
     *
     * // ✅ Correct
     * function abc(foo = "bar") {}
     * ```
     * @see [unicorn/prefer-default-parameters](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-default-parameters.md)
     */
    [`${unicornNamespace}/prefer-default-parameters`]: ERROR,

    /**
     * Prefer `Node#append` over `Node#appendChild`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * foo.appendChild(bar);
     *
     * // ✅ Correct
     * foo.append(bar);
     * foo.append("bar");
     * foo.append(bar, "baz");
     * ```
     * @see [unicorn/prefer-dom-node-append](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-dom-node-append.md)
     */
    [`${unicornNamespace}/prefer-dom-node-append`]: ERROR,

    /**
     * Prefer using `Element.dataset` on DOM elements over calling attribute methods.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const unicorn = element.getAttribute("data-unicorn");
     * element.setAttribute("data-unicorn", "🦄");
     * element.removeAttribute("data-unicorn");
     * const hasUnicorn = element.hasAttribute("data-unicorn");
     *
     * // ✅ Correct
     * const { unicorn } = element.dataset;
     * element.dataset.unicorn = "🦄";
     * delete element.dataset.unicorn;
     * const hasUnicorn = Object.hasOwn(element.dataset, "unicorn");
     * ```
     * @see [unicorn/prefer-dom-node-dataset](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-dom-node-dataset.md)
     */
    [`${unicornNamespace}/prefer-dom-node-dataset`]: ERROR,

    /**
     * Prefer `ChildNode#remove` over `ParentNode#removeChild(childNode)`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * parentNode.removeChild(foo);
     * parentNode.removeChild(this);
     *
     * // ✅ Correct
     * foo.remove();
     * this.remove();
     * ```
     * @see [unicorn/prefer-dom-node-remove](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-dom-node-remove.md)
     */
    [`${unicornNamespace}/prefer-dom-node-remove`]: ERROR,

    /**
     * Prefer `Element#textContent` over `Element#innerText`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const text = foo.innerText;
     * const { innerText } = foo;
     * foo.innerText = "🦄";
     *
     * // ✅ Correct
     * const text = foo.textContent;
     * const { textContent } = foo;
     * foo.textContent = "🦄";
     * ```
     * @see [unicorn/prefer-dom-node-text-content](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-dom-node-text-content.md)
     */
    [`${unicornNamespace}/prefer-dom-node-text-content`]: ERROR,

    /**
     * Prefer `EventTarget` over `EventEmitter`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * import { EventEmitter } from "node:event";
     *
     * class Foo extends EventEmitter {}
     * const emitter = new EventEmitter();
     *
     * // ✅ Correct
     * class Foo extends EventTarget {}
     * const target = new EventTarget();
     * ```
     * @see [unicorn/prefer-event-target](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-event-target.md)
     */
    [`${unicornNamespace}/prefer-event-target`]: ERROR,

    /**
     * Prefer `export…from` when re-exporting.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * import defaultExport from "./foo.js";
     * export default defaultExport;
     *
     * import { named } from "./foo.js";
     * export { named };
     *
     * // ✅ Correct
     * export { default } from "./foo.js";
     * export { named } from "./foo.js";
     * ```
     * @see [unicorn/prefer-export-from](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-export-from.md)
     */
    [`${unicornNamespace}/prefer-export-from`]: ERROR,

    /**
     * Prefer `Array#includes` over `Array.indexOf` and `Array#some` when checking for existence or non-existence.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * array.indexOf("foo") !== -1;
     * string.indexOf("foo") !== -1;
     * array.lastIndexOf("foo") !== -1;
     * string.lastIndexOf("foo") !== -1;
     *
     * // ✅ Correct
     * array.includes("foo");
     * string.includes("foo");
     * array.includes("foo");
     * string.includes("foo");
     * ```
     * @see [unicorn/prefer-includes](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-includes.md)
     */
    [`${unicornNamespace}/prefer-includes`]: ERROR,

    /**
     * Prefer `KeyboardEvent#key` over `KeyboardEvent#keyCode`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * window.addEventListener("keydown", event => {
     *   console.log(event.keyCode);
     * });
     *
     * // ✅ Correct
     * window.addEventListener("click", event => {
     *   console.log(event.key);
     * });
     * ```
     * @see [unicorn/prefer-keyboard-event-key](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-keyboard-event-key.md)
     */
    [`${unicornNamespace}/prefer-keyboard-event-key`]: ERROR,

    /**
     * Prefer using a logical operator over a ternary.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * foo ? foo : bar;
     *
     * // ✅ Correct
     * foo ?? bar;
     * ```
     * @see [unicorn/prefer-logical-operator-over-ternary](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-logical-operator-over-ternary.md)
     */
    [`${unicornNamespace}/prefer-logical-operator-over-ternary`]: ERROR,

    /**
     * Modern DOM APIs enforcement:
     *
     * | Prefer…                                                                  | over…                                                              |
     * | ------------------------------------------------------------------------ | ------------------------------------------------------------------ |
     * | `Element#before`                                                         | `Element#insertBefore`                                             |
     * | `Element#replaceWith`                                                    | `Element#replaceChild`                                             |
     * | `Element#before`, `Element#after`, `Element#append` or `Element#prepend` | `Element#insertAdjacentText`() and `Element#insertAdjacentElement` |
     *
     * @see [unicorn/prefer-modern-dom-apis](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-modern-dom-apis.md)
     */
    [`${unicornNamespace}/prefer-modern-dom-apis`]: ERROR,

    /**
     * Prefer modern Math APIs over legacy patterns.
     *
     * | Prefer…         | over…                       |
     * | --------------- | --------------------------- |
     * | `Math.log10(x)` | `Math.log(x) * Math.LOG10E` |
     * | `Math.log2(x)`  | `Math.log(x) * Math.LOG2E`  |
     * | `Math.hypot(…)` | `Math.sqrt(a * a + b * b)`  |
     *
     * @see [unicorn/prefer-modern-math-apis](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-modern-math-apis.md)
     */
    [`${unicornNamespace}/prefer-modern-math-apis`]: ERROR,

    /**
     * Prefer using the `node:` protocol when importing Node.js builtin modules.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * import fs from "fs/promises";
     * export { strict as default } from "assert";
     *
     * // ✅ Correct
     * import fs from "node:fs/promises";
     * export { strict as default } from "node:assert";
     * ```
     * @see [unicorn/prefer-node-protocol](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-node-protocol.md)
     */
    [`${unicornNamespace}/prefer-node-protocol`]: ERROR,

    /**
     * Prefer using `Object.fromEntries` to transform a list of key-value pairs into an object.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const object = pairs.reduce(
     *   (object, [key, value]) => ({...object, [key]: value}),
     *   {}
     * );
     * const object = _.fromPairs(pairs);
     *
     * // ✅ Correct
     * const object = Object.fromEntries(pairs);
     * const object = new Map(pairs);
     * ```
     * @see [unicorn/prefer-object-from-entries](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-object-from-entries.md)
     */
    [`${unicornNamespace}/prefer-object-from-entries`]: ERROR,

    /**
     * Prefer omitting the catch binding parameter.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * try {} catch (notUsedError) {}
     * try {} catch ({ message }) {}
     *
     * // ✅ Correct
     * try {} catch {}
     *
     * try {} catch (error) {
     *   console.error(error);
     * }
     * ```
     * @see [unicorn/prefer-optional-catch-binding](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-optional-catch-binding.md)
     */
    [`${unicornNamespace}/prefer-optional-catch-binding`]: ERROR,

    /**
     * Query selector preference:
     *
     * | Prefer…                    | over…                                                               |
     * | -------------------------- | ------------------------------------------------------------------- |
     * | `Element#querySelector`    | `Element#getElementById`                                            |
     * | `Element#querySelectorAll` | `Element#getElementsByClassName` and `Element#getElementsByTagName` |
     *
     * @see [unicorn/prefer-query-selector](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-query-selector.md)
     */
    [`${unicornNamespace}/prefer-query-selector`]: ERROR,

    /**
     * Prefer the spread operator over `Array.from`, `Array#concat`, `Array#slice`, `Array#toSpliced` and `String#split`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * Array.from(set).map(element => foo(element));
     * const array = array1.concat(array2);
     * const copy = array.slice();
     * const copy = array.slice(0);
     * const copy = array.toSpliced();
     * const characters = string.split("");
     *
     * // ✅ Correct
     * [...set].map(element => foo(element));
     * const array = [...array1, ...array2];
     * const tail = array.slice(1);
     * const copy = [...array];
     * const characters = [...string];
     * ```
     * @see [unicorn/prefer-spread](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-spread.md)
     */
    [`${unicornNamespace}/prefer-spread`]: ERROR,

    /**
     * Prefer `String#replaceAll` over regex searches with the global flag.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * string.replace(/RegExp with global flag/igu, ");
     * string.replace(/RegExp without special symbols/g, ");
     *
     * // ✅ Correct
     * string.replace(/Non-global regexp/iu, ");
     * string.replace("Not a regex expression", ")
     * string.replaceAll("string", ");
     * ```
     * @see [unicorn/prefer-string-replace-all](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-string-replace-all.md)
     */
    [`${unicornNamespace}/prefer-string-replace-all`]: ERROR,

    /**
     * Prefer `String#slice` over `String#substr` and `String#substring`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * foo.substr(start, length);
     * foo.substring(indexStart, indexEnd);
     *
     * // ✅ Correct
     * foo.slice(beginIndex, endIndex);
     * ```
     * @see [unicorn/prefer-string-slice](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-string-slice.md)
     */
    [`${unicornNamespace}/prefer-string-slice`]: ERROR,

    /**
     * Prefer `String#startsWith` & `String#endsWith` over `RegExp#test`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = /^bar/.test(baz);
     * const foo = /bar$/.test(baz);
     *
     * // ✅ Correct
     * const foo = baz.startsWith("bar");
     * const foo = baz.endsWith("bar");
     * ```
     * @see [unicorn/prefer-string-starts-ends-with](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-string-starts-ends-with.md)
     */
    [`${unicornNamespace}/prefer-string-starts-ends-with`]: ERROR,

    /**
     * Prefer `String#trimStart` and `String#trimEnd` over `String#trimLeft` and `String#trimRight`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = bar.trimLeft();
     * const foo = bar.trimRight();
     *
     * // ✅ Correct
     * const foo = bar.trimStart();
     * const foo = bar.trimEnd();
     * ```
     * @see [unicorn/prefer-string-trim-start-end](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-string-trim-start-end.md)
     */
    [`${unicornNamespace}/prefer-string-trim-start-end`]: ERROR,

    /**
     * Prefer using `structuredClone` to create a deep clone.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const clone = JSON.parse(JSON.stringify(foo));
     * const clone = _.cloneDeep(foo);
     *
     * // ✅ Correct
     * const clone = structuredClone(foo);
     * ```
     * @see [unicorn/prefer-structured-clone](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-structured-clone.md)
     */
    [`${unicornNamespace}/prefer-structured-clone`]: ERROR,

    /**
     * Prefer ternary expressions over simple if-else statements.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const unicorn = () => {
     *   if (test) {
     *     return a;
     *   } else {
     *     return b;
     *   }
     * };
     *
     *
     * const unicorn = function* () {
     *   if (test) {
     *     yield a;
     *   } else {
     *     yield b;
     *   }
     * };
     *
     * const unicorn = async () => {
     *   if (test) {
     *     await a();
     *   } else {
     *     await b();
     *   }
     * };
     *
     * if (test) {
     *   throw new Error("foo");
     * } else {
     *   throw new Error("bar");
     * }
     *
     * let foo;
     * if (test) {
     *   foo = 1;
     * } else {
     *   foo = 2;
     * }
     *
     * // ✅ Correct
     * const unicorn = () => test ? a : b;
     *
     *
     * const unicorn = function* () {
     *   yield (test ? a : b);
     * };
     *
     * const unicorn = async () => {
     *   await (test ? a : b)();
     * };
     *
     * throw new Error(test ? "foo" : "bar");
     *
     * const foo = test ? 1 : 2;
     * ```
     * @see [unicorn/prefer-ternary](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-ternary.md)
     */
    [`${unicornNamespace}/prefer-ternary`]: ERROR,

    /**
     * Enforce throwing TypeError in type checking conditions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (Array.isArray(foo) === false) {
     *   throw new Error("Array expected");
     * }
     *
     * // ✅ Correct
     * if (Array.isArray(foo) === false) {
     *   throw new TypeError("Array expected");
     * }
     * ```
     * @see [unicorn/prefer-type-error](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/prefer-type-error.md)
     */
    [`${unicornNamespace}/prefer-type-error`]: ERROR,

    /**
     * Enforce using the separator argument with `Array#join`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const string = array.join();
     *
     * // ✅ Correct
     * const string = array.join(",");
     * ```
     * @see [unicorn/require-array-join-separator](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/require-array-join-separator.md)
     */
    [`${unicornNamespace}/require-array-join-separator`]: ERROR,

    /**
     * Enforce consistent case for text encoding identifiers.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * await fs.readFile(file, "UTF-8");
     * await fs.readFile(file, "ASCII");
     * const string = buffer.toString("utf-8");
     *
     * // ✅ Correct
     * await fs.readFile(file, "utf8");
     * await fs.readFile(file, "ascii");
     * const string = buffer.toString("utf8");
     * ```
     * @see [unicorn/text-encoding-identifier-case](https://github.com/sindresorhus/eslint-plugin-unicorn/blob/main/docs/rules/text-encoding-identifier-case.md)
     */
    [`${unicornNamespace}/text-encoding-identifier-case`]: ERROR,
  },
});
