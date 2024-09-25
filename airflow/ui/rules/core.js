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
import { ERROR, WARN } from "./levels.js";

const allExtensions = "*.{j,t}s{x,}";

/**
 * Core ESLint rules.
 * @see [ESLint core rules](https://eslint.org/docs/latest/rules)
 */
export const coreRules = /** @type {const} @satisfies {FlatConfig.Config} */ ({
  files: [
    // Files in the root and src directories
    `{rules,src}/**/${allExtensions}`,
    // Files in the root directory
    allExtensions,
  ],
  rules: {
    /**
     * Enforce getter and setter pairs in objects and classes.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const example = {
     *   set foo(value) {
     *     this.foo = value;
     *   }
     * }
     *
     * // ✅ Correct
     * const example = {
     *   get foo() {
     *     return this.foo;
     *   }
     *   set foo(value) {
     *     this.foo = value;
     *   }
     * }
     * ```
     * @see [accessor-pairs](https://eslint.org/docs/latest/rules/accessor-pairs)
     */
    "accessor-pairs": ERROR,

    /**
     * Mapping functions should always return a value explicitly.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const mapped = [1, 2, 3].map(item => {
     *   console.log(item * 2);
     * });
     *
     * // ✅ Correct
     * const mapped = [1, 2, 3].map(item => item * 2);
     * ```
     * @see [array-callback-return](https://eslint.org/docs/latest/rules/array-callback-return)
     */
    "array-callback-return": ERROR,

    /**
     * Only use braces around an arrow function body when needed.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const arrow = () => {
     *   return 1;
     * };
     *
     * // ✅ Correct
     * const arrow = () => 1;
     * ```
     * @see [arrow-body-style](https://eslint.org/docs/latest/rules/arrow-body-style)
     */
    "arrow-body-style": ERROR,

    /**
     * Limit cyclomatic complexity to a maximum of 10.
     *
     * @see [complexity](https://eslint.org/docs/latest/rules/complexity)
     */
    complexity: [WARN, 10],

    /**
     * Require curly around all control statements.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (true) console.log("Hello, world!");
     *
     * // ✅ Correct
     * if (true) {
     *   console.log("Hello, world!");
     * }
     * ```
     * @see [curly](https://eslint.org/docs/latest/rules/curly)
     */
    curly: ERROR,

    /**
     * Require `default` case in `switch` statements.
     *
     * @see [default-case](https://eslint.org/docs/latest/rules/default-case)
     */
    "default-case": ERROR,

    /**
     * Enforce `default` clauses in switch statements to be last.
     *
     * @see [default-case-last](https://eslint.org/docs/latest/rules/default-case-last)
     */
    "default-case-last": ERROR,

    /**
     * Require `===` and `!==` instead of `==` and `!=`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (a == b) // …
     * if (a != b) // …
     *
     * // ✅ Correct
     * if (a === b) // …
     * if (a !== b) // …
     * ```
     * @see [eqeqeq](https://eslint.org/docs/latest/rules/eqeqeq)
     */
    eqeqeq: ERROR,

    /**
     * Enforce "for" loop update clause moving the counter in the right direction.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * for (let index = 0; index < 10; index--) {}
     * for (let index = 10; index >= 0; index++) {}
     * for (let index = 0; index > 10; index++) {}
     * for (let index = 0; 10 > index; index--) {}
     *
     * // ✅ Correct
     * for (let index = 0; index < 10; index++) {}
     * for (let index = 0; 10 > index; index++) {}
     * ```
     * @see [for-direction](https://eslint.org/docs/latest/rules/for-direction)
     */
    "for-direction": ERROR,

    /**
     * Enforce the consistent use of `function` expressions assigned to variables.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * function foo() {}
     *
     * // ✅ Correct
     * const foo = function () {}
     * ```
     * @see [func-style](https://eslint.org/docs/latest/rules/func-style)
     */
    "func-style": ERROR,

    /**
     * Require grouped accessor pairs (`get` and `set`) in object literals and classes.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const example = {
     *   get value() { return this._value; },
     *   get other() { return this._other; },
     *   set value(value) { this._value = value; },
     *   set other(value) { this._other = value; },
     * };
     *
     * // ✅ Correct
     * const example = {
     *   get value() { return this._value; },
     *   set value(value) { this._value = value; },
     *   get other() { return this._other; },
     *   set other(value) { this._other = value; },
     * };
     * ```
     * @see [grouped-accessor-pairs](https://eslint.org/docs/latest/rules/grouped-accessor-pairs)
     */
    "grouped-accessor-pairs": ERROR,

    /**
     * If you really want to use `for..in` for some reason, guard it.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * for (const key in object) {
     *   console.log(key);
     * }
     *
     * // ✅ Correct
     * for (const key in object) {
     *   if (Object.hasOwn(object, key)) {
     *     console.log(key);
     *   }
     * }
     * ```
     * @see [guard-for-in](https://eslint.org/docs/latest/rules/guard-for-in)
     */
    "guard-for-in": ERROR,

    /**
     * Enforce minimum identifier length to be 2 characters long.
     *
     * Exceptions:
     *
     * -   `_`: Allow `_` as a common "placeholder".
     * -   `x`: Allow `x` for the x coordinate.
     * -   `y`: Allow `y` for the y coordinate.
     * -   `z`: Allow `z` for the z coordinate.
     *
     * @see [id-length](https://eslint.org/docs/latest/rules/id-length)
     */
    "id-length": [ERROR, { exceptions: ["_", "x", "y", "z"] }],

    /**
     * Require logical assignment operator shorthand.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * a = a || b;
     * a = a && b;
     * a = a ?? b;
     * a = a + b;
     *
     * // ✅ Correct
     * a ||= b;
     * a &&= b;
     * a ??= b;
     * a += b;
     * ```
     * @see [logical-assignment-operators](https://eslint.org/docs/latest/rules/logical-assignment-operators)
     */
    "logical-assignment-operators": ERROR,

    /**
     * Enforce a maximum of 1 `class` per file.
     *
     * @see [max-classes-per-file](https://eslint.org/docs/latest/rules/max-classes-per-file)
     */
    "max-classes-per-file": [ERROR, 1],

    /**
     * Enforce a maximum depth that blocks can be nested fo 3.
     *
     * @see [max-depth](https://eslint.org/docs/latest/rules/max-depth)
     */
    "max-depth": [ERROR, { max: 3 }],

    /**
     * Enforce the maximum of a file length to 250 lines.
     * Need more? Move it to another file.
     *
     * @see [max-lines](https://eslint.org/docs/latest/rules/max-lines)
     */
    "max-lines": [
      ERROR,
      { max: 250, skipBlankLines: true, skipComments: true },
    ],

    /**
     * Enforce a maximum number of 100 lines of code in a function.
     * Need more? Move it to another function.
     *
     * @see [max-lines-per-function](https://eslint.org/docs/latest/rules/max-lines-per-function)
     */
    "max-lines-per-function": [
      ERROR,
      { max: 100, skipBlankLines: true, skipComments: true },
    ],

    /**
     * Enforce a maximum depth that callbacks can be nested to 3.
     *
     * @see [max-nested-callbacks](https://eslint.org/docs/latest/rules/max-nested-callbacks)
     */
    "max-nested-callbacks": [ERROR, { max: 3 }],

    /**
     * Enforce a maximum number of statements allowed in function blocks to 10.
     *
     * @see [max-statements](https://eslint.org/docs/latest/rules/max-statements)
     */
    "max-statements": [WARN, { max: 10 }],

    /**
     * Disallow use of `alert`, `confirm`, and `prompt`.
     *
     * @see [no-alert](https://eslint.org/docs/latest/rules/no-alert)
     */
    "no-alert": ERROR,

    /**
     * Disallow `await` inside of loops.
     *
     * @see [no-await-in-loop](https://eslint.org/docs/latest/rules/no-await-in-loop)
     */
    "no-await-in-loop": ERROR,

    /**
     * Warn about bitwise that might be wrong usages of logic operators.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const example = 1 & 2;
     * const example = 1 | 2;
     *
     * // ✅ Correct
     * const example = 1 && 2;
     * const example = 1 || 2;
     * ```
     * @see [no-bitwise](https://eslint.org/docs/latest/rules/no-bitwise)
     */
    "no-bitwise": WARN,

    /**
     * Disallow use of `caller` and `callee`.
     *
     * @see [no-caller](https://eslint.org/docs/latest/rules/no-caller)
     */
    "no-caller": ERROR,

    /**
     * Disallow comparing against -0.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * value === -0;
     *
     * // ✅ Correct
     * value === 0;
     * Object.is(value, -0);
     * ```
     * @see [no-compare-neg-zero](https://eslint.org/docs/latest/rules/no-compare-neg-zero)
     */
    "no-compare-neg-zero": ERROR,

    /**
     * Warn about `console` usages.
     *
     * @see [no-console](https://eslint.org/docs/latest/rules/no-console)
     */
    "no-console": WARN,

    /**
     * Disallow returning value in constructor.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Example {
     *   constructor() {
     *     return "Hello, world!";
     *   }
     * }
     * ```
     * @see [no-constructor-return](https://eslint.org/docs/latest/rules/no-constructor-return)
     */
    "no-constructor-return": ERROR,

    /**
     * Disallow `continue` statements.
     *
     * @see [no-continue](https://eslint.org/docs/latest/rules/no-continue)
     */
    "no-continue": ERROR,

    /**
     * Disallow control characters in regular expressions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const pattern1 = /\x00/;
     * const pattern2 = /\x0C/;
     * const pattern3 = /\x1F/;
     * const pattern4 = /\u000C/;
     * const pattern5 = /\u{C}/u;
     *
     * // ✅ Correct
     * const pattern1 = /\x20/;
     * const pattern2 = /\u0020/;
     * const pattern3 = /\u{20}/u;
     * const pattern4 = /\t/;
     * const pattern5 = /\n/;
     * ```
     * @see [no-control-regex](https://eslint.org/docs/latest/rules/no-control-regex)
     */
    "no-control-regex": ERROR,

    /**
     * Disallow the use of `debugger`.
     *
     * @see [no-debugger](https://eslint.org/docs/latest/rules/no-debugger)
     */
    "no-debugger": ERROR,

    /**
     * Disallow duplicate case labels.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * switch (example) {
     *   case 1: break;
     *   case 2: break;
     *   case 1: break; // duplicate test expression
     *   default break;
     * }
     *
     * // ✅ Correct
     * switch (example) {
     *   case 1: break;
     *   case 2: break;
     *   case 3: break;
     *   default break;
     * }
     * ```
     * @see [no-duplicate-case](https://eslint.org/docs/latest/rules/no-duplicate-case)
     */
    "no-duplicate-case": ERROR,

    /**
     * Disallow empty block statements.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (example) {}
     *
     * // ✅ Correct
     * if (example) {
     *   // At least this one has a comment
     * }
     * ```
     * @see [no-empty](https://eslint.org/docs/latest/rules/no-empty)
     */
    "no-empty": ERROR,

    /**
     * Disallow empty character classes in regular expressions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * /^abc[]/u;
     *
     * // ✅ Correct
     * /^abc/u;
     * ```
     * @see [no-empty-character-class](https://eslint.org/docs/latest/rules/no-empty-character-class)
     */
    "no-empty-character-class": ERROR,

    /**
     * Disallow empty functions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = function() {}
     *
     * // ✅ Correct
     * const foo = () {
     *   // At least this one has a comment
     * }
     * ```
     * @see [no-empty-function](https://eslint.org/docs/latest/rules/no-empty-function)
     */
    "no-empty-function": ERROR,

    /**
     * Disallow empty destructuring patterns.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const { foo: {} } = example; // Doesn't create any variables
     *
     * // ✅ Correct
     * const { foo: { bar } } = example;
     * ```
     * @see [no-empty-pattern](https://eslint.org/docs/latest/rules/no-empty-pattern)
     */
    "no-empty-pattern": ERROR,

    /**
     * Disallow empty static blocks.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Example {
     *   static {}
     * }
     *
     * // ✅ Correct
     * class Example {
     *   static {
     *     // At least this one has a comment
     *   }
     * }
     * ```
     * @see [no-empty-static-block](https://eslint.org/docs/latest/rules/no-empty-static-block)
     */
    "no-empty-static-block": ERROR,

    /**
     * Disallow `null` comparisons without type-checking operators.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (foo == null) { bar(); }
     *
     * // ✅ Correct
     * if (foo === null) { bar(); }
     * ```
     * @see [no-eq-null](https://eslint.org/docs/latest/rules/no-eq-null)
     */
    "no-eq-null": ERROR,

    /**
     * Disallow `eval`.
     *
     * @see [no-eval](https://eslint.org/docs/latest/rules/no-eval)
     */
    "no-eval": ERROR,

    /**
     * Disallow reassigning exceptions in `catch` clauses.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * try {
     *   // something
     * } catch (error) {
     *   error = "Why would you ever want to do this?";
     * }
     *
     * // ✅ Correct
     * try {
     *   // something
     * } catch (error) {
     *   const message = "This is better";
     * }
     * ```
     * @see [no-ex-assign](https://eslint.org/docs/latest/rules/no-ex-assign)
     */
    "no-ex-assign": ERROR,

    /**
     * Disallow extending of native objects (`prototype` of native).
     *
     * @see [no-extend-native](https://eslint.org/docs/latest/rules/no-extend-native)
     */
    "no-extend-native": ERROR,

    /**
     * Disallow unnecessary function binding.
     *
     * @see [no-extra-bind](https://eslint.org/docs/latest/rules/no-extra-bind)
     */
    "no-extra-bind": ERROR,

    /**
     * Disallow the type conversion with short notation (too "clever").
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const boolean = !!value;
     * const number = +value;
     *
     * // ✅ Correct
     * const boolean = Boolean(value);
     * const number = parseFloat(value);
     * ```
     * @see [no-implicit-coercion](https://eslint.org/docs/latest/rules/no-implicit-coercion)
     */
    "no-implicit-coercion": ERROR,

    /**
     * Disallow assigning to imported bindings.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * import { value } from "./module.js";
     *
     * value = 1;
     * ```
     * @see [no-import-assign](https://eslint.org/docs/latest/rules/no-import-assign)
     */
    "no-import-assign": ERROR,

    /**
     * Disallow invalid regular expression strings in `RegExp` constructors.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * new RegExp('[');
     * new RegExp('.', 'z');
     * new RegExp('\\');
     * ```
     * @see [no-invalid-regexp](https://eslint.org/docs/latest/rules/no-invalid-regexp)
     */
    "no-invalid-regexp": ERROR,

    /**
     * Disallow the use of the `__iterator__` property.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * Foo.prototype.__iterator__ = function() {
     *   return new FooIterator(this);
     * };
     * ```
     * @see [no-iterator](https://eslint.org/docs/latest/rules/no-iterator)
     */
    "no-iterator": ERROR,

    /**
     * Disallow unnecessary nested blocks.
     *
     * @see [no-lone-blocks](https://eslint.org/docs/latest/rules/no-lone-blocks)
     */
    "no-lone-blocks": ERROR,

    /**
     * Disallow literal numbers that lose precision.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const a = 9007199254740993;
     * const b = 5123000000000000000000000000001;
     * const c = 1230000000000000000000000.0;
     * const d = .1230000000000000000000000;
     * const e = 0X20000000000001;
     * const f = 0X2_000000000_0001;;
     *
     * // ✅ Correct
     * const a = 12345;
     * const b = 123.456;
     * const c = 123e34;
     * const d = 12300000000000000000000000;
     * const e = 0x1FFFFFFFFFFFFF;
     * const f = 9007199254740991;
     * const g = 9007_1992547409_91;
     * ```
     * @see [no-loss-of-precision](https://eslint.org/docs/latest/rules/no-loss-of-precision)
     */
    "no-loss-of-precision": ERROR,

    /**
     * Disallow multiline strings (use template strings instead).
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const string = "Hello, \
     *   world!";
     *
     * // ✅ Correct
     * const string = `Hello,
     *   world!`;
     * ```
     * @see [no-multi-str](https://eslint.org/docs/latest/rules/no-multi-str)
     */
    "no-multi-str": ERROR,

    /**
     * Disallow negated conditions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * !a ? b : c;
     *
     * // ✅ Correct
     * a ? c : b;
     * ```
     * @see [no-negated-condition](https://eslint.org/docs/latest/rules/no-negated-condition)
     */
    "no-negated-condition": ERROR,

    /**
     * Disallow `new` operators outside of assignments or comparisons.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * new Thing();
     *
     * // ✅ Correct
     * const thing = new Thing();
     * ```
     * @see [no-new](https://eslint.org/docs/latest/rules/no-new)
     */
    "no-new": ERROR,

    /**
     * Disallow `Function` constructor (write the function instead).
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const Example = new Function("a", "b", "return a + b");
     *
     * // ✅ Correct
     * const Example = (a, b) => a + b;
     * ```
     * @see [no-new-func](https://eslint.org/docs/latest/rules/no-new-func)
     */
    "no-new-func": ERROR,

    /**
     * Disallow `new` operators with global non-constructor functions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = new Symbol("foo");
     * const bar = new BigInt(9007199254740991);
     *
     * // ✅ Correct
     * const foo = Symbol("foo");
     * const bar = BigInt(9007199254740991);
     * ```
     * @see [no-new-native-nonconstructor](https://eslint.org/docs/latest/rules/no-new-native-nonconstructor)
     */
    "no-new-native-nonconstructor": ERROR,

    /**
     * Disallow primitive wrapper instances (`new String()`, `new Boolean()`, etc.).
     *
     * @see [no-new-wrappers](https://eslint.org/docs/latest/rules/no-new-wrappers)
     */
    "no-new-wrappers": ERROR,

    /**
     * Disallow calls to the `Object` constructor without an argument.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * Object();
     * new Object();
     *
     * // ✅ Correct
     * Object("foo");
     * const object = { foo: 1, bar: 2 };
     * ```
     * @see [no-object-constructor](https://eslint.org/docs/latest/rules/no-object-constructor)
     */
    "no-object-constructor": ERROR,

    /**
     * Disallow old octal literals.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const num = 071;
     * const result = 5 + 07;
     *
     * // ✅ Correct
     * const num = 0o71;
     * const result = 5 + 0o7;
     * ```
     * @see [no-octal](https://eslint.org/docs/latest/rules/no-octal)
     */
    "no-octal": ERROR,

    /**
     * Disallow reassignment of function parameters.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const example = (value) => {
     *   value = value * 2;
     * };
     *
     * // ✅ Correct
     * const example = (value) => {
     *   const newValue = value * 2;
     * };
     * ```
     * @see [no-param-reassign](https://eslint.org/docs/latest/rules/no-param-reassign)
     */
    "no-param-reassign": ERROR,

    /**
     * Disallow the unary operators `++` and `--`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * let a = 0;
     * a++;
     *
     * // ✅ Correct
     * let a = 0;
     * a += 1;
     * ```
     * @see [no-plusplus](https://eslint.org/docs/latest/rules/no-plusplus)
     */
    "no-plusplus": ERROR,

    /**
     * Disallow use of `__proto__` (unsafe and deprecated).
     *
     * @see [no-proto](https://eslint.org/docs/latest/rules/no-proto)
     */
    "no-proto": ERROR,

    /**
     * Disallow calling some `Object.prototype` methods directly on objects.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const hasBarProperty = foo.hasOwnProperty("bar");
     * const isPrototypeOfBar = foo.isPrototypeOf(bar);
     * const barIsEnumerable = foo.propertyIsEnumerable("bar");
     *
     * // ✅ Correct
     * const hasBarProperty = Object.hasOwn("bar");
     * const isPrototypeOfBar = foo instanceof Bar;
     * const barIsEnumerable = Object.getOwnPropertyDescriptor(foo, "bar").enumerable;
     * ```
     * @see [no-prototype-builtins](https://eslint.org/docs/latest/rules/no-prototype-builtins)
     */
    "no-prototype-builtins": ERROR,

    /**
     * Disallow multiple spaces in regular expressions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const expression = /foo   bar/;
     *
     * // ✅ Correct
     * const expression = /foo {3}bar/;
     * ```
     * @see [no-regex-spaces](https://eslint.org/docs/latest/rules/no-regex-spaces)
     */
    "no-regex-spaces": ERROR,

    /**
     * Disallow specified global variables. Disabled globals are:
     * -   `window`.
     *
     * @see [no-restricted-globals](https://eslint.org/docs/latest/rules/no-restricted-globals)
     */
    "no-restricted-globals": [
      ERROR,
      { message: "Use `globalThis` instead.", name: "window" },
    ],

    /**
     * Disallow assignment operators in `return` statements.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * function doSomething() {
     *   return foo = bar + 2;
     * }
     *
     * // ✅ Correct
     * function doSomething() {
     *   const foo = bar + 2;
     *
     *   return foo;
     * }
     * ```
     * @see [no-return-assign](https://eslint.org/docs/latest/rules/no-return-assign)
     */
    "no-return-assign": [ERROR, "always"],

    /**
     * Disallow assignments where both sides are exactly the same.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * example = example;
     * ```
     * @see [no-self-assign](https://eslint.org/docs/latest/rules/no-self-assign)
     */
    "no-self-assign": ERROR,

    /**
     * Disallow self compare (comparing a value to itself).
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (value === value) // …
     * ```
     * @see [no-self-compare](https://eslint.org/docs/latest/rules/no-self-compare)
     */
    "no-self-compare": ERROR,

    /**
     * Disallow identifiers from shadowing restricted names.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const NaN = () => undefined;
     * const something = Infinity => {};
     * const undefined = 5;
     * try {} catch (eval) {}
     * ```
     * @see [no-shadow-restricted-names](https://eslint.org/docs/latest/rules/no-shadow-restricted-names)
     */
    "no-shadow-restricted-names": ERROR,

    /**
     * Disallow sparse arrays.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const items = [, ,];
     * const colors = ["red", , "blue"];
     * ```
     * @see [no-sparse-arrays](https://eslint.org/docs/latest/rules/no-sparse-arrays)
     */
    "no-sparse-arrays": ERROR,

    /**
     * Disallow template literal placeholder syntax in regular strings.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const example = "Hello, ${world}!";
     *
     * // ✅ Correct
     * const example = `Hello, ${world}!`;
     * ```
     * @see [no-template-curly-in-string](https://eslint.org/docs/latest/rules/no-template-curly-in-string)
     */
    "no-template-curly-in-string": ERROR,

    /**
     * Disallow unmodified conditions of loops.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * while (node) {
     *   someFunction(node);
     * }
     *
     * // ✅ Correct
     * while (node) {
     *   someFunction(node);
     *   node = node.next;
     * }
     * ```
     * @see [no-unmodified-loop-condition](https://eslint.org/docs/latest/rules/no-unmodified-loop-condition)
     */
    "no-unmodified-loop-condition": ERROR,

    /**
     * Disallow unnecessary ternaries like: `condition ? true : false`.
     *
     * @see [no-unneeded-ternary](https://eslint.org/docs/latest/rules/no-unneeded-ternary)
     */
    "no-unneeded-ternary": ERROR,

    /**
     * Disallow loops with a body that allows only one iteration.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * for (let i = 0; i < 100; i++) {
     *   if (logic) // …
     *   break; // Will always break on first loop
     * }
     *
     * // ✅ Correct
     * for (let i = 0; i < 100; i++) {
     *   if (logic) {
     *     // …
     *     break;
     *   }
     * }
     * ```
     * @see [no-unreachable-loop](https://eslint.org/docs/latest/rules/no-unreachable-loop)
     */
    "no-unreachable-loop": ERROR,

    /**
     * Disallow control flow statements in `finally` blocks.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * (() => {
     *   try {
     *     return 1; // 1 is returned but suspended until finally block ends
     *   } catch {
     *     return 2;
     *   } finally {
     *     return 3; // 3 is returned before 1, which we did not expect
     *   }
     * })();
     *
     * // ✅ Correct
     * (() => {
     *   try {
     *     return 1;
     *   } catch {
     *     return 2;
     *   } finally {
     *     console.log("Done!");
     *   }
     * })();
     * ```
     * @see [no-unsafe-finally](https://eslint.org/docs/latest/rules/no-unsafe-finally)
     */
    "no-unsafe-finally": ERROR,

    /**
     * Disallow variable assignments when the value is not used.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const function1 = () => {
     *   let v = 'used';
     *   doSomething(v);
     *   v = 'unused';
     * }
     *
     * // ✅ Correct
     * const function1 = () => {
     *   let v = 'used';
     *   doSomething(v);
     *   v = 'unused';
     * }
     * ```
     * @see [no-useless-assignment](https://eslint.org/docs/latest/rules/no-useless-assignment)
     */
    "no-useless-assignment": ERROR,

    /**
     * Disallow unnecessary `.call()` and `.apply()`.
     *
     * @see [no-useless-call](https://eslint.org/docs/latest/rules/no-useless-call)
     */
    "no-useless-call": ERROR,

    /**
     * Disallow unnecessary `catch` clauses.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * try {
     *   doSomethingThatMightThrow();
     * } catch (error) {
     *   throw error;
     * }
     *
     * // ✅ Correct
     * try {
     *   doSomethingThatMightThrow();
     * } catch (error) {
     *   doSomethingBeforeRethrow();
     *   throw error;
     * }
     * ```
     * @see [no-useless-catch](https://eslint.org/docs/latest/rules/no-useless-catch)
     */
    "no-useless-catch": ERROR,

    /**
     * Disallow unnecessary computed property keys in objects and classes.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = { ["a"]: "b" };
     *
     * // ✅ Correct
     * const foo = { a: "b" };
     * ```
     * @see [no-useless-catch](https://eslint.org/docs/latest/rules/no-useless-catch)
     */
    "no-useless-computed-key": ERROR,

    /**
     * Disallow unnecessary concatenation of strings.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const example = "Hello, " + "world!";
     *
     * // ✅ Correct
     * const example = "Hello, world!";
     * ```
     * @see [no-useless-concat](https://eslint.org/docs/latest/rules/no-useless-concat)
     */
    "no-useless-concat": ERROR,

    /**
     * Disallow unnecessary escape characters.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * "\'";
     * '\"';
     * "\#";
     * "\e";
     * `\"`;
     * `\"${foo}\"`;
     * `\#{foo}`;
     *
     * // ✅ Correct
     * "\"";
     * '\'';
     * "\x12";
     * "\u00a9";
     * "\371";
     * "xs\u2111";
     * `\``;
     * ```
     * @see [no-useless-escape](https://eslint.org/docs/latest/rules/no-useless-escape)
     */
    "no-useless-escape": ERROR,

    /**
     * Disallow renaming import, export, and destructured assignments to
     * the same name.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * import { foo as foo } from "bar";
     * export { foo as foo };
     * const { foo: foo } = bar;
     *
     * // ✅ Correct
     * import { foo } from "bar";
     * export { foo };
     * const { foo } = bar;
     * ```
     * @see [no-useless-rename](https://eslint.org/docs/latest/rules/no-useless-rename)
     */
    "no-useless-rename": ERROR,

    /**
     * Disallow redundant return statements.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const example = () => {
     *   console.log("Hello, world!");
     *   return;
     * };
     *
     * // ✅ Correct
     * const example = () => console.log("Hello, world!");
     * ```
     * @see [no-useless-return](https://eslint.org/docs/latest/rules/no-useless-return)
     */
    "no-useless-return": ERROR,

    /**
     * Require `let` or `const` instead of `var`.
     *
     * @see [no-var](https://eslint.org/docs/latest/rules/no-var)
     */
    "no-var": ERROR,

    /**
     * Require object literal shorthand syntax.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const example = { value: value };
     *
     * // ✅ Correct
     * const example = { value };
     * ```
     * @see [object-shorthand](https://eslint.org/docs/latest/rules/object-shorthand)
     */
    "object-shorthand": ERROR,

    /**
     * Enforce variables to be declared separately.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * let a, b;
     *
     * // ✅ Correct
     * let a;
     * let b;
     * ```
     * @see [one-var](https://eslint.org/docs/latest/rules/one-var)
     */
    "one-var": [ERROR, "never"],

    /**
     * Require assignment operator shorthand where possible.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * x = x + y;
     * x = x - y;
     * x = x * y;
     * x = x / y;
     * x = x % y;
     * x = x ** y;
     * x = x << y;
     * x = x >> y;
     * x = x >>> y;
     * x = x & y;
     * x = x ^ y;
     * x = x | y;
     *
     * // ✅ Correct
     * x += y
     * x -= y
     * x *= y
     * x /= y
     * x %= y
     * x **= y
     * x <<= y
     * x >>= y
     * x >>>= y
     * x &= y
     * x ^= y
     * x |= y
     * ```
     * @see [operator-assignment](https://eslint.org/docs/latest/rules/operator-assignment)
     */
    "operator-assignment": ERROR,

    /**
     * Require using arrow functions for callbacks.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * [1, 2, 3].map(function (value) {
     *   return value * 2;
     * });
     *
     * // ✅ Correct
     * [1, 2, 3].map(value => value * 2);
     * ```
     * @see [prefer-arrow-callback](https://eslint.org/docs/latest/rules/prefer-arrow-callback)
     */
    "prefer-arrow-callback": ERROR,

    /**
     * Suggest using `const` over `let` when value is not reassigned.
     *
     * @see [prefer-const](https://eslint.org/docs/latest/rules/prefer-const)
     */
    "prefer-const": ERROR,

    /**
     * Warn about the use of `Math.pow` in favor of the `**` operator.
     *
     * @see [prefer-exponentiation-operator](https://eslint.org/docs/latest/rules/prefer-exponentiation-operator)
     */
    "prefer-exponentiation-operator": WARN,

    /**
     * Suggest using named capture group in regular expressions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const regex = /(\d{4})-(\d{2})-(\d{2})/u;
     *
     * // ✅ Correct
     * const regex = /(?<year>\d{4})-(?<month>\d{2})-(?<day>\d{2})/u;
     * ```
     * @see [prefer-named-capture-group](https://eslint.org/docs/latest/rules/prefer-named-capture-group)
     */
    "prefer-named-capture-group": WARN,

    /**
     * Disallow use of `Object.prototype.hasOwnProperty.call()` and prefer
     * use of `Object.hasOwn()`.
     *
     * @see [prefer-object-has-own](https://eslint.org/docs/latest/rules/prefer-object-has-own)
     */
    "prefer-object-has-own": ERROR,

    /**
     * Prefer use of an object spread over `Object.assign`.
     *
     * @see [prefer-object-spread](https://eslint.org/docs/latest/rules/prefer-object-spread)
     */
    "prefer-object-spread": WARN,

    /**
     * Disallow use of the `RegExp` in favor of regular expression literals.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const regex = new RegExp("Hello, world!");
     *
     * // ✅ Correct
     * const regex = /Hello, world!/;
     * ```
     * @see [prefer-regex-literals](https://eslint.org/docs/latest/rules/prefer-regex-literals)
     */
    "prefer-regex-literals": ERROR,

    /**
     * Use the rest parameters instead of `arguments`.
     *
     * @see [prefer-rest-params](https://eslint.org/docs/latest/rules/prefer-rest-params)
     */
    "prefer-rest-params": ERROR,

    /**
     * Use spread syntax instead of `.apply()`.
     *
     * @see [prefer-spread](https://eslint.org/docs/latest/rules/prefer-spread)
     */
    "prefer-spread": ERROR,

    /**
     * Use template literals instead of string concatenation.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const string = "Hello" + name + "!";
     *
     * // ✅ Correct
     * const string = `Hello, ${name}!`;
     * ```
     * @see [prefer-template](https://eslint.org/docs/latest/rules/prefer-template)
     */
    "prefer-template": ERROR,

    /**
     * Require radix parameter in `parseInt`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const value = parseInt("10");
     *
     * // ✅ Correct
     * const value = parseInt("10", 10);
     * ```
     * @see [radix](https://eslint.org/docs/latest/rules/radix)
     */
    radix: ERROR,

    /**
     * Disallow assignments that can lead to race conditions due to usage of
     * `await` or `yield`.
     *
     * @see [require-atomic-updates](https://eslint.org/docs/latest/rules/require-atomic-updates)
     */
    "require-atomic-updates": ERROR,

    /**
     * Enforce the use of `u` flag on `RegExp` (to support unicode).
     *
     * @see [require-unicode-regexp](https://eslint.org/docs/latest/rules/require-unicode-regexp)
     */
    "require-unicode-regexp": ERROR,

    /**
     * Require generator functions to contain `yield`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * function* foo() {
     *   return 13;
     * }
     *
     * // ✅ Correct
     * function* foo() {
     *   yield 10;
     *   return 13;
     * }
     * ```
     * @see [require-yield](https://eslint.org/docs/latest/rules/require-yield)
     */
    "require-yield": ERROR,

    /**
     * Require symbol descriptions.
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const symbol = Symbol();
     *
     * // ✅ Correct
     * const symbol = Symbol("Description here");
     * ```
     * @see [symbol-description](https://eslint.org/docs/latest/rules/symbol-description)
     */
    "symbol-description": ERROR,

    /**
     * Disallow yoda conditions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (1 === value) // …
     *
     * // ✅ Correct
     * if (value === 1) // …
     * ```
     * @see [yoda](https://eslint.org/docs/latest/rules/yoda)
     */
    yoda: ERROR,
  },
});
