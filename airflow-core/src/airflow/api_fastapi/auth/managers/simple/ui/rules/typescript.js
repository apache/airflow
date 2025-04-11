/* eslint-disable max-lines */

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
import typescript from "@typescript-eslint/eslint-plugin";
import typescriptParser from "@typescript-eslint/parser";

import { ERROR, OFF, WARN } from "./levels.js";
import { off } from "./off.js";

/**
 * ESLint TypeScript namespace.
 */
export const typescriptNamespace = "@typescript-eslint";

/**
 * ESLint TypeScript rules.
 * @see [@typescript-eslint/eslint-plugin](https://typescript-eslint.io/rules/)
 */
export const typescriptRules = /** @type {const} @satisfies {FlatConfig.Config} */ ({
  languageOptions: {
    parser: typescriptParser,
    parserOptions: {
      ecmaFeatures: {
        /**
         * Enable global strict mode.
         */
        impliedStrict: true,

        /**
         * JSX enabled by default (even if it's not a React project).
         */
        jsx: true,
      },
      /**
       * Self explanatory. Use the latest ECMAScript version.
       */
      ecmaVersion: "latest",

      /**
       * Get `tsconfig.json` from the root directory.
       */
      project: [
        `${import.meta.dirname}/../tsconfig.app.json`,
        `${import.meta.dirname}/../tsconfig.dev.json`,
        `${import.meta.dirname}/../tsconfig.node.json`,
      ],

      /**
       * Default to ESM.
       */
      sourceType: "module",
    },
  },
  plugins: { [typescriptNamespace]: typescript },
  rules: {
    /**
     * Grouping overloaded members together to improve readability.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * type Foo = {
     *   foo(s: string): void;
     *   foo(n: number): void;
     *   bar(): void;
     *   foo(sn: string | number): void;
     * };
     *
     * // ✅ Correct
     * type Foo = {
     *   foo(s: string): void;
     *   foo(n: number): void;
     *   foo(sn: string | number): void;
     *   bar(): void;
     * };
     * ```
     * @see [@typescript-eslint/adjacent-overload-signatures](https://typescript-eslint.io/rules/adjacent-overload-signatures/)
     */
    [`${typescriptNamespace}/adjacent-overload-signatures`]: ERROR,

    /**
     * Consistent array types using generics `Array` and `ReadonlyArray`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo: string[] = [];
     * const bar: readonly string[] = [];
     *
     * // ✅ Correct
     * const foo: Array<string> = [];
     * const bar: ReadonlyArray<string> = [];
     * ```
     * @see [@typescript-eslint/array-type](https://typescript-eslint.io/rules/array-type/)
     */
    [`${typescriptNamespace}/array-type`]: [ERROR, { default: "generic" }],

    /**
     * Avoid await on non thenable values.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = await 42;
     *
     * // ✅ Correct
     * const bar = await Promise.resolve(42);
     * ```
     * @see [@typescript-eslint/await-thenable](https://typescript-eslint.io/rules/await-thenable/)
     */
    [`${typescriptNamespace}/await-thenable`]: ERROR,

    /**
     * `@ts-comment` rules:
     *
     * -   `@ts-check` is allowed.
     * -   `@ts-expect-error` is allowed, but only with a description.
     * -   `@ts-ignore` is not allowed.
     * -   `@ts-nocheck` is not allowed.
     *
     * @see [@typescript-eslint/ban-ts-comment](https://typescript-eslint.io/rules/ban-ts-comment/)
     */
    [`${typescriptNamespace}/ban-ts-comment`]: [
      ERROR,
      {
        minimumDescriptionLength: 10,
        "ts-check": false,
        "ts-expect-error": "allow-with-description",
        "ts-ignore": true,
        "ts-nocheck": true,
      },
    ],

    /**
     * Enforce that literals on classes are exposed in a consistent style.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Example {
     *   readonly myField1 = 1;
     *   readonly myField2 = `hello world`;
     *   private readonly myField3 = 'hello world';
     * }
     *
     * // ✅ Correct
     * class Example {
     *   // no readonly modifier
     *   public myField1 = 'hello';
     *
     *   // not a literal
     *   public readonly myField2 = [1, 2, 3];
     *
     *   public static get myField3() {
     *     return 1;
     *   }
     *
     *   private get ['myField4']() {
     *     return 'hello world';
     *   }
     * }
     * ```
     * @see [@typescript-eslint/class-literal-property-style](https://typescript-eslint.io/rules/class-literal-property-style/)
     */
    [`${typescriptNamespace}/class-literal-property-style`]: [ERROR, "getters"],

    /**
     * Enforce that class methods utilize `this`. If not then it should be a function.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Example {
     *   private message = "Hello, world!";
     *   public log() {
     *     console.log(this.message);
     *   }
     *   public goodbye() {
     *     console.log("Goodbye, world!");
     *   }
     * }
     *
     * // ✅ Correct
     * const goodbye = () => console.log("Goodbye, world!");
     *
     * class Example {
     *   private message = "Hello, world!";
     *   public log() {
     *     console.log(this.message);
     *   }
     * }
     * ```
     * @see [@typescript-eslint/class-methods-use-this](https://typescript-eslint.io/rules/class-methods-use-this/)
     * @see [class-methods-use-this](https://eslint.org/docs/latest/rules/class-methods-use-this)
     */
    [`${typescriptNamespace}/class-methods-use-this`]: ERROR,

    /**
     * Enforce specifying generic type arguments on constructor name of
     * a constructor call.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const map: Map<string, number> = new Map();
     * const set: Set<string> = new Set();
     *
     * // ✅ Correct
     * const map = new Map<string, number>();
     * const set = new Set<string>();
     * ```
     * @see [@typescript-eslint/consistent-generic-constructors](https://typescript-eslint.io/rules/consistent-generic-constructors/)
     */
    [`${typescriptNamespace}/consistent-generic-constructors`]: [ERROR, "constructor"],

    /**
     * Use `Record` instead of index signature.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * type Foo = { [key: string]: string; };
     * type Bar = { [key: string]: string; bar: string; };
     *
     * // ✅ Correct
     * type Foo = Record<string, string>;
     * type Bar = Record<string, string> & { bar: string; };
     * ```
     * @see [@typescript-eslint/consistent-indexed-object-style](https://typescript-eslint.io/rules/consistent-indexed-object-style/)
     */
    [`${typescriptNamespace}/consistent-indexed-object-style`]: [ERROR, "record"],

    /**
     * Require `return` statements to either always or never specify values.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = (): undefined => {}
     * const bar = (flag: boolean): undefined => {
     *   if (flag) {
     *     return foo()
     *   }
     *   return;
     * }
     * const baz = async (flag: boolean): Promise<undefined> {
     *   if (flag) {
     *     return;
     *  }
     *   return foo();
     * }
     *
     * // ✅ Correct
     * const foo = (): void => {}
     * const bar = (flag: boolean): void => {
     *   if (flag) {
     *     return foo()
     *   }
     *   return;
     * }
     * const baz = async (flag: boolean): Promise<void | number> {
     *   if (flag) {
     *     return 42;
     *  }
     *   return;
     * }
     * ```
     * @see [@typescript-eslint/consistent-return](https://typescript-eslint.io/rules/consistent-return)
     * @see [consistent-return](https://eslint.org/docs/latest/rules/consistent-return)
     */
    [`${typescriptNamespace}/consistent-return`]: ERROR,

    /**
     * Use `as` assertion.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = <string>bar;
     *
     * // ✅ Correct
     * const foo = bar as string;
     * ```
     * @see [@typescript-eslint/consistent-type-assertions](https://typescript-eslint.io/rules/consistent-type-assertions/)
     */
    [`${typescriptNamespace}/consistent-type-assertions`]: [
      ERROR,
      { assertionStyle: "as", objectLiteralTypeAssertions: "allow" },
    ],

    /**
     * Use `type` for type definitions (instead of interfaces).
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * interface Foo { bar: string; }
     *
     * // ✅ Correct
     * type Foo = { bar: string; };
     * ```
     * @see [@typescript-eslint/consistent-type-definitions](https://typescript-eslint.io/rules/consistent-type-definitions/)
     */
    [`${typescriptNamespace}/consistent-type-definitions`]: [ERROR, "type"],

    /**
     * Enforce specifying generic type arguments on constructor name of
     * a constructor call.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const x = 1;
     * type T = number;
     * export { x, T };
     *
     * // ✅ Correct
     * const x = 1;
     * type T = number;
     * export { x, type T };
     * ```
     * @see [@typescript-eslint/consistent-type-exports](https://typescript-eslint.io/rules/consistent-type-exports/)
     */
    [`${typescriptNamespace}/consistent-type-exports`]: [
      ERROR,
      { fixMixedExportsWithInlineTypeSpecifier: true },
    ],

    /**
     * Enforce "typed imports".
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * import { Foo } from "./types/Foo.js";
     *
     * // ✅ Correct
     * import type { Foo } from "./types/Foo.js";
     * import { type Foo } from "./types/Foo.js";
     * ```
     * @see [@typescript-eslint/consistent-type-imports](https://typescript-eslint.io/rules/consistent-type-imports/)
     */
    [`${typescriptNamespace}/consistent-type-imports`]: [ERROR, { fixStyle: "inline-type-imports" }],

    /**
     * Enforce `default` parameters to be last.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const example = (a = 1, b) => a + b;
     *
     * // ✅ Correct
     * const example = (b, a = 1) => a + b;
     * ```
     * @see [@typescript-eslint/default-param-last](https://typescript-eslint.io/rules/default-param-last/)
     * @see [default-param-last](https://eslint.org/docs/latest/rules/default-param-last)
     */
    [`${typescriptNamespace}/default-param-last`]: ERROR,

    /**
     * Enforce `dot.notation` instead of `square["bracket"]["notation"]`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = bar["baz"];
     *
     * // ✅ Correct
     * const foo = bar.baz;
     * const bar = foo[foo]; // Dynamic access is allowed.
     * ```
     * @see [@typescript-eslint/dot-notation](https://typescript-eslint.io/rules/dot-notation/)
     */
    [`${typescriptNamespace}/dot-notation`]: ERROR,

    /**
     * Rely on inference instead of making return type explicit.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = (): string => "bar";
     *
     * // ✅ Correct
     * const foo = () => "bar";
     * ```
     * @see [@typescript-eslint/explicit-function-return-type](https://typescript-eslint.io/rules/explicit-function-return-type/)
     */
    [`${typescriptNamespace}/explicit-function-return-type`]: OFF,

    /**
     * When working with classes, let's be explicit about accessibility.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Foo {
     *   constructor() {}
     * }
     *
     * // ✅ Correct
     * class Foo {
     *   public constructor() {}
     * }
     * @see [@typescript-eslint/explicit-member-accessibility](https://typescript-eslint.io/rules/explicit-member-accessibility/)
     */
    [`${typescriptNamespace}/explicit-member-accessibility`]: [
      ERROR,
      {
        accessibility: "explicit",
        overrides: {
          accessors: "explicit",
          constructors: "explicit",
        },
      },
    ],

    /**
     * Rely on inference for boundary types as well.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = (): string => "bar";
     *
     * // ✅ Correct
     * const foo = () => "bar";
     * ```
     * @see [@typescript-eslint/explicit-module-boundary-types](https://typescript-eslint.io/rules/explicit-module-boundary-types/)
     */
    [`${typescriptNamespace}/explicit-module-boundary-types`]: OFF,

    /**
     * We allow declaring variables without values in favor of `no-useless-assignment`
     *
     * @see [@typescript-eslint/init-declarations](https://typescript-eslint.io/rules/init-declarations/)
     * @see [init-declarations](https://eslint.org/docs/latest/rules/init-declarations)
     * @see [no-useless-assignment](https://eslint.org/docs/latest/rules/no-useless-assignment)
     */
    [`${typescriptNamespace}/init-declarations`]: OFF,

    /**
     * Max amount of parameters set to 3. More than that is too much.
     *
     * @see [@typescript-eslint/max-params](https://typescript-eslint.io/rules/max-params/)
     * @see [max-params](https://eslint.org/docs/latest/rules/max-params)
     */
    [`${typescriptNamespace}/max-params`]: [ERROR, { max: 3 }],

    /**
     * Classes? Well, let's make those methods look like arrow functions at least.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Foo {
     *   public bar() {
     *     return "baz";
     *   }
     * }
     *
     * // ✅ Correct
     * class Foo {
     *   public bar = () => "baz";
     * }
     * ```
     * @see [@typescript-eslint/method-signature-style](https://typescript-eslint.io/rules/method-signature-style/)
     */
    [`${typescriptNamespace}/method-signature-style`]: [ERROR, "property"],

    /**
     * Consistent naming:
     *
     * -   `camelCase`, `PascalCase` and `UPPER_CASE` for variables and enum members.
     * -   `camelCase` and `PascalCase` for functions.
     * -   `camelCase` for parameters, class properties, and class methods.
     * -   `PascalCase` for classes, enums, interfaces, type aliases, type literals and type parameters.
     *
     * @see [@typescript-eslint/naming-convention](https://typescript-eslint.io/rules/naming-convention/)
     */
    [`${typescriptNamespace}/naming-convention`]: [
      ERROR,
      {
        format: null, // eslint-disable-line unicorn/no-null
        leadingUnderscore: "allow",
        selector: "default",
        trailingUnderscore: "forbid",
      },
      {
        format: ["camelCase", "PascalCase", "UPPER_CASE"],
        selector: ["variable", "enumMember"],
      },
      {
        format: ["camelCase", "PascalCase"],
        selector: "function",
      },
      {
        format: ["camelCase"],
        leadingUnderscore: "allow",
        selector: ["autoAccessor", "parameter", "classProperty", "classMethod"],
        trailingUnderscore: "forbid",
      },
      {
        format: ["PascalCase"],
        leadingUnderscore: "allow",
        selector: ["class", "enum", "interface", "typeAlias", "typeLike", "typeParameter"],
      },
    ],

    /**
     * Just use `[]` instead of `new Array()`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = new Array<string>();
     *
     * // ✅ Correct
     * const bar = [] as Array<string>;
     * ```
     * @see [@typescript-eslint/no-array-constructor](https://typescript-eslint.io/rules/no-array-constructor/)
     */
    [`${typescriptNamespace}/no-array-constructor`]: ERROR,

    /**
     * Disallow using the `delete` operator on array values.
     *
     * @see [@typescript-eslint/no-array-delete](https://typescript-eslint.io/rules/no-array-delete/)
     */
    [`${typescriptNamespace}/no-array-delete`]: ERROR,

    /**
     * Avoid `.toString()` without a useful return type.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = ({}).toString();
     *
     * // ✅ Correct
     * const foo = (42).toString();
     * ```
     * @see [@typescript-eslint/no-base-to-string](https://typescript-eslint.io/rules/no-base-to-string/)
     */
    [`${typescriptNamespace}/no-base-to-string`]: ERROR,

    /**
     * Require expressions of type void to appear in statement position.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const response = alert('Are you sure?');
     * console.log(alert('Are you sure?'));
     *
     * // ✅ Correct
     * alert('Hello, world!');
     * ```
     * @see [@typescript-eslint/no-confusing-void-expression](https://typescript-eslint.io/rules/no-confusing-void-expression/)
     */
    [`${typescriptNamespace}/no-confusing-void-expression`]: [
      ERROR,
      { ignoreArrowShorthand: true, ignoreVoidOperator: true },
    ],

    /**
     * Disallow duplicate enum member values.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const enum E {
     *   A = 0,
     *   B = 0,
     * }
     *
     * // ✅ Correct
     * const enum E {
     *   A = 0,
     *   B = 1,
     * }
     * ```
     * @see [@typescript-eslint/no-duplicate-enum-values](https://typescript-eslint.io/rules/no-duplicate-enum-values/)
     */
    [`${typescriptNamespace}/no-duplicate-enum-values`]: ERROR,

    /**
     * Disallow duplicate constituents of union and intersection types.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * type StringOrNumber = string | string | number;
     * type ThisOrThat = { that: string } & { that: string };
     *
     * // ✅ Correct
     * type StringOrNumber = string | number;
     * type ThisOrThat = { this: string } & { that: string };
     * ```
     * @see [@typescript-eslint/no-duplicate-type-constituents](https://typescript-eslint.io/rules/no-duplicate-type-constituents/)
     */
    [`${typescriptNamespace}/no-duplicate-type-constituents`]: ERROR,

    /**
     * Avoid `delete` of dynamic properties.
     *
     * @see [@typescript-eslint/no-dynamic-delete](https://typescript-eslint.io/rules/no-dynamic-delete/)
     */
    [`${typescriptNamespace}/no-dynamic-delete`]: ERROR,

    /**
     * Empty functions don't make any sense, but still we should avoid confusing `() => {}`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = () => {};
     * const bar = function() {};
     *
     * // ✅ Correct
     * const foo = () => undefined;
     * const bar = function() { return undefined; };
     * ```
     * @see [@typescript-eslint/no-empty-function](https://typescript-eslint.io/rules/no-empty-function/)
     */
    [`${typescriptNamespace}/no-empty-function`]: ERROR,

    /**
     * An empty interface is useless.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * let fooValue: {};
     * interface FooInterface {}
     * type FooType = {};
     *
     * // ✅ Correct
     * let fooValue: object;
     * interface FooInterface {}
     * interface FooInterface {
     *   bar: string;
     * };
     * type FooType = object;
     * ```
     * @see [@typescript-eslint/no-empty-object-type](https://typescript-eslint.io/rules/no-empty-object-type/)
     */
    [`${typescriptNamespace}/no-empty-object-type`]: ERROR,

    /**
     * `any` is a really bad abstraction. Use `unknown` instead.
     *
     * @see [@typescript-eslint/no-explicit-any](https://typescript-eslint.io/rules/no-explicit-any/)
     */
    [`${typescriptNamespace}/no-explicit-any`]: ERROR,

    /**
     * A class with all statics can be turned into an object.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Foo {
     *   public static bar = "baz";
     * }
     *
     * // ✅ Correct
     * const Foo = {
     *   bar: "baz",
     * };
     * @see [@typescript-eslint/no-extraneous-class](https://typescript-eslint.io/rules/no-extraneous-class/)
     */
    [`${typescriptNamespace}/no-extraneous-class`]: ERROR,

    /**
     * Let's avoid floating (unhandled) promises.
     *
     * @example
     * ```typescript
     * const example = async () => "foo";
     *
     * // ❌ Incorrect
     * example();
     *
     * // ✅ Correct
     * void example();
     * example().then(console.log).catch(console.error);`
     * ```
     * @see [@typescript-eslint/no-floating-promises](https://typescript-eslint.io/rules/no-floating-promises/)
     */
    [`${typescriptNamespace}/no-floating-promises`]: ERROR,

    /**
     * Use `for/of`, or better yet `map` or `forEach`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * for (const key in foo) {
     *   console.log(key);
     * }
     *
     * // ✅ Correct
     * Object.keys(foo).forEach(console.log);
     *
     * for (const key of Object.keys(foo)) {
     *   console.log(key);
     * }
     * ```
     * @see [@typescript-eslint/no-for-in-array](https://typescript-eslint.io/rules/no-for-in-array/)
     */
    [`${typescriptNamespace}/no-for-in-array`]: ERROR,

    /**
     * This is super insecure, avoid it at all costs.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const timeout = setTimeout("alert(`Hi!`);", 100);
     * const fn = new Function("a", "b", "return a + b");
     *
     * // ✅ Correct
     * const timeout = setTimeout(() => alert(`Hi!`), 100);
     * const fn = (a, b) => a + b;
     * ```
     * @see [@typescript-eslint/no-implied-eval](https://typescript-eslint.io/rules/no-implied-eval/)
     */
    [`${typescriptNamespace}/no-implied-eval`]: ERROR,

    /**
     * Enforce the use of top-level import type qualifier when an import only has specifiers with inline type qualifiers.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * import { type A, type B } from 'library';
     *
     * // ✅ Correct
     * import type { A, B } from 'library';
     * ```
     * @see [@typescript-eslint/no-import-type-side-effects](https://typescript-eslint.io/rules/no-import-type-side-effects/)
     */
    [`${typescriptNamespace}/no-import-type-side-effects`]: ERROR,

    /**
     * We want to rely on inference.
     *
     * @see [@typescript-eslint/no-inferrable-types](https://typescript-eslint.io/rules/no-inferrable-types/)
     */
    [`${typescriptNamespace}/no-inferrable-types`]: OFF,

    /**
     * Avoid using `this` outside a class.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * function foo() {
     *   console.log(this);
     * }
     *
     * // ✅ Correct
     * class Foo {
     *   public bar() {
     *     console.log(this);
     *   }
     * }
     * ```
     * @see [@typescript-eslint/no-invalid-this](https://typescript-eslint.io/rules/no-invalid-this/)
     */
    [`${typescriptNamespace}/no-invalid-this`]: ERROR,

    /**
     * Avoid `void` for types, use `undefined` instead.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo: number | void = undefined;
     *
     * // ✅ Correct
     * const foo: number | undefined = undefined;
     * ```
     * @see [@typescript-eslint/no-invalid-void-type](https://typescript-eslint.io/rules/no-invalid-void-type/)
     */
    [`${typescriptNamespace}/no-invalid-void-type`]: ERROR,

    /**
     * Avoid defining functions inside loops.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * for (const i = 0; i < 10; i++) {
     *     const add2 = (n: number) => n + 2;
     *     console.log(add2(i));
     * }
     *
     * // ✅ Correct
     * const add2 = (n: number) => n + 2;
     *
     * for (const i = 0; i < 10; i++) {
     *     console.log(add2(i));
     * }
     * ```
     * @see [@typescript-eslint/no-loop-func](https://typescript-eslint.io/rules/no-loop-func/)
     */
    [`${typescriptNamespace}/no-loop-func`]: ERROR,

    /**
     * Disallow the `void` operator except when used to discard a value.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * void (() => undefined)();
     *
     * // ✅ Correct
     * void (() => "value")();
     * ```
     * @see [@typescript-eslint/no-meaningless-void-operator](https://typescript-eslint.io/rules/no-meaningless-void-operator/)
     */
    [`${typescriptNamespace}/no-meaningless-void-operator`]: ERROR,

    /**
     * Avoid missuses of the `new` declaration.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * declare class Foo {
     *   new(): Foo;
     * }
     *
     * // ✅ Correct
     * declare class Foo {
     *   constructor();
     * }
     * ```
     * @see [@typescript-eslint/no-misused-new](https://typescript-eslint.io/rules/no-misused-new/)
     */
    [`${typescriptNamespace}/no-misused-new`]: ERROR,

    /**
     * Avoid missuses of promises.
     *
     * @example
     * ```typescript
     * const aPromise = Promise.resolve("foo");
     *
     * // ❌ Incorrect
     * aPromise ? "foo" : "bar";
     *
     * // ✅ Correct
     * (await aPromise) ? "foo" : "bar";
     * ```
     * @see [@typescript-eslint/no-misused-promises](https://typescript-eslint.io/rules/no-misused-promises/)
     */
    [`${typescriptNamespace}/no-misused-promises`]: ERROR,

    /**
     * Disallow enums from having both number and string members.
     *
     * @example
     * ```typescript
     * const aPromise = Promise.resolve("foo");
     *
     * // ❌ Incorrect
     * const enum Status {
     *   Unknown,
     *   Closed = 1,
     *   Open = 'open',
     * }
     *
     * // ✅ Correct
     * const enum Status {
     *   Unknown = 0,
     *   Closed = 2,
     *   Open = 4,
     * }
     * ```
     * @see [@typescript-eslint/no-mixed-enums](https://typescript-eslint.io/rules/no-mixed-enums/)
     */
    [`${typescriptNamespace}/no-mixed-enums`]: ERROR,

    /**
     * Old TypeScript.
     *
     * @see [@typescript-eslint/no-namespace](https://typescript-eslint.io/rules/no-namespace/)
     */
    [`${typescriptNamespace}/no-namespace`]: ERROR,

    /**
     * Prevents using non-null assertion with nullish coalescing.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = bar! ?? "foo";
     *
     * // ✅ Correct
     * const foo = bar ?? "foo";
     * ```
     * @see [@typescript-eslint/no-non-null-asserted-nullish-coalescing](https://typescript-eslint.io/rules/no-non-null-asserted-nullish-coalescing/)
     */
    [`${typescriptNamespace}/no-non-null-asserted-nullish-coalescing`]: ERROR,

    /**
     * Avoid null assertion (`value!`), a really unsafe TypeScript operator.
     *
     * @see [@typescript-eslint/no-non-null-assertion](https://typescript-eslint.io/rules/no-non-null-assertion/)
     */
    [`${typescriptNamespace}/no-non-null-assertion`]: WARN,

    /**
     * Disallow members of unions and intersections that do nothing or override type information.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * type UnionAny = any | 'foo';
     * type UnionUnknown = unknown | 'foo';
     * type UnionNever = never | 'foo';
     * type UnionBooleanLiteral = boolean | false;
     * type UnionNumberLiteral = number | 1;
     * type UnionStringLiteral = string | 'foo';
     * type IntersectionAny = any & 'foo';
     * type IntersectionUnknown = string & unknown;
     * type IntersectionNever = string | never;
     * type IntersectionBooleanLiteral = boolean & false;
     * type IntersectionNumberLiteral = number & 1;
     * type IntersectionStringLiteral = string & 'foo';
     * ```
     * @see [@typescript-eslint/no-redundant-type-constituents](https://typescript-eslint.io/rules/no-redundant-type-constituents/)
     */
    [`${typescriptNamespace}/no-redundant-type-constituents`]: ERROR,

    /**
     * Use ECMAScript `import` and `export` instead of `require` and `module.exports`.
     *
     * @see [@typescript-eslint/no-require-imports](https://typescript-eslint.io/rules/no-require-imports/)
     */
    [`${typescriptNamespace}/no-require-imports`]: ERROR,

    /**
     * Disallow specified modules when loaded by `import`. Disabled imports
     * are:
     * -   jQuery.
     * -   jQuery UI.
     * -   Lodash.
     * -   Moment.
     * -   Underscore.
     *
     * @see [@typescript-eslint/no-restricted-imports](https://typescript-eslint.io/rules/no-restricted-imports/)
     * @see [no-restricted-imports](https://eslint.org/docs/latest/rules/no-restricted-imports)
     */
    [`${typescriptNamespace}/no-restricted-imports`]: [
      ERROR,
      {
        patterns: [
          {
            group: ["jquery", "lodash", "lodash-es", "lodash.*", "lodash/*", "underscore"],
            message: "Just use vanilla JavaScript.",
          },
          {
            group: ["jquery-ui", "moment"],
            message: "Use a modern dependency instead.",
          },
        ],
      },
    ],

    /**
     * Avoid name shadowing (`_` is allowed).
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const bar = "foo";
     * const foo = (bar: string) => bar;
     *
     * // ✅ Correct
     * const bar = "foo";
     * const foo = (baz: string) => baz;
     * ```
     * @see [@typescript-eslint/no-shadow](https://typescript-eslint.io/rules/no-shadow/)
     */
    [`${typescriptNamespace}/no-shadow`]: [ERROR, { allow: ["_"], hoist: "all" }],

    /**
     * Just use arrow functions, _this/that aliases are no longer needed.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = function () {
     *   const that = this;
     *   return function() {
     *     return that;
     *   };
     * }
     *
     * // ✅ Correct
     * const foo = function() {
     *   return () => this;
     * };
     * ```
     * @see [@typescript-eslint/no-this-alias](https://typescript-eslint.io/rules/no-this-alias/)
     */
    [`${typescriptNamespace}/no-this-alias`]: ERROR,

    /**
     * If it's a `boolean`, use it as such.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (foo === true) // …
     *
     * // ✅ Correct
     * if (foo) // …
     * ```
     * @see [@typescript-eslint/no-unnecessary-boolean-literal-compare](https://typescript-eslint.io/rules/no-unnecessary-boolean-literal-compare/)
     */
    [`${typescriptNamespace}/no-unnecessary-boolean-literal-compare`]: ERROR,

    /**
     * Avoid conditions with values that can't be falsy.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const check = (value: "foo" | "bar") => {
     *   if (value) // value will never be falsy
     * }
     *
     * // ✅ Correct
     * const check = (value: string) => {
     *   if (value) // Necessary, since value might be ""
     * }
     * ```
     * @see [@typescript-eslint/no-unnecessary-condition](https://typescript-eslint.io/rules/no-unnecessary-condition/)
     */
    [`${typescriptNamespace}/no-unnecessary-condition`]: ERROR,

    /**
     * Disallow unnecessary assignment of constructor property parameter.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Foo {
     *   constructor(public bar: string) {
     *     this.bar = bar;
     *   }
     * }
     *
     * // ✅ Correct
     * class Foo {
     *   constructor(public bar: string) {}
     * }
     * ```
     * @see [@typescript-eslint/no-unnecessary-parameter-property-assignment](https://typescript-eslint.io/rules/no-unnecessary-parameter-property-assignment/)
     */
    [`${typescriptNamespace}/no-unnecessary-parameter-property-assignment`]: ERROR,

    /**
     * Disallow unnecessary namespace qualifiers.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * enum A {
     *   B,
     *   C = A.B,
     * }
     *
     * // ✅ Correct
     * enum A {
     *   B,
     *   C = B,
     * }
     * ```
     * @see [@typescript-eslint/no-unnecessary-qualifier](https://typescript-eslint.io/rules/no-unnecessary-qualifier/)
     */
    [`${typescriptNamespace}/no-unnecessary-qualifier`]: ERROR,

    /**
     * Disallow unnecessary template expressions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * `${'a'}${'b'}`
     *
     * // ✅ Correct
     * "ab"
     * ```
     * @see [@typescript-eslint/no-unnecessary-template-expression](https://typescript-eslint.io/rules/no-unnecessary-template-expression/)
     */
    [`${typescriptNamespace}/no-unnecessary-template-expression`]: ERROR,

    /**
     * If the type assertion is the same, skip it.
     *
     * @example
     * ```typescript
     * const example = <Value = string>(value: Value) => value;
     *
     * // ❌ Incorrect
     * example<string>("hello");
     *
     * // ✅ Correct
     * example("hello");
     * ```
     * @see [@typescript-eslint/no-unnecessary-type-arguments](https://typescript-eslint.io/rules/no-unnecessary-type-arguments/)
     */
    [`${typescriptNamespace}/no-unnecessary-type-arguments`]: ERROR,

    /**
     * Don't assert something that doesn't need assertion.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = "bar" as string;
     * ```
     * @see [@typescript-eslint/no-unnecessary-type-assertion](https://typescript-eslint.io/rules/no-unnecessary-type-assertion/)
     */
    [`${typescriptNamespace}/no-unnecessary-type-assertion`]: ERROR,

    /**
     * Don't do `extends any` or `extends unknown`. That's the default.
     *
     * @see [@typescript-eslint/no-unnecessary-type-constraint](https://typescript-eslint.io/rules/no-unnecessary-type-constraint/)
     */
    [`${typescriptNamespace}/no-unnecessary-type-constraint`]: ERROR,

    /**
     * Disallow type parameters that only appear once.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const second = <A, B>(a: A, b: B): B => b;
     *
     * // ✅ Correct
     * const second = <B>(a: unknown, b: B): B => b;
     * ```
     * @see [@typescript-eslint/no-unnecessary-type-parameters](https://typescript-eslint.io/rules/no-unnecessary-type-parameters/)
     */
    [`${typescriptNamespace}/no-unnecessary-type-parameters`]: ERROR,

    /**
     * Disallows calling an function with an `any` type value.
     *
     * @see [@typescript-eslint/no-unsafe-argument](https://typescript-eslint.io/rules/no-unsafe-argument/)
     */
    [`${typescriptNamespace}/no-unsafe-argument`]: ERROR,

    /**
     * Avoid `any` assignments.
     *
     * @see [@typescript-eslint/no-unsafe-assignment](https://typescript-eslint.io/rules/no-unsafe-assignment/)
     */
    [`${typescriptNamespace}/no-unsafe-assignment`]: ERROR,

    /**
     * Avoid calling `any`.
     *
     * @see [@typescript-eslint/no-unsafe-call](https://typescript-eslint.io/rules/no-unsafe-call/)
     */
    [`${typescriptNamespace}/no-unsafe-call`]: ERROR,

    /**
     * Disallow unsafe declaration merging.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * interface Foo {}
     * class Foo {}
     * ```
     * @see [@typescript-eslint/no-unsafe-declaration-merging](https://typescript-eslint.io/rules/no-unsafe-declaration-merging/)
     */
    [`${typescriptNamespace}/no-unsafe-declaration-merging`]: ERROR,

    /**
     * Disallow comparing an enum value with a non-enum value.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const enum Fruit {
     *   Apple = 0,
     * }
     *
     * declare let fruit: Fruit;
     *
     * fruit === 0;
     *
     * // ✅ Correct
     * const enum Fruit {
     *   Apple = 0,
     * }
     *
     * declare let fruit: Fruit;
     *
     * fruit === Fruit.Apple;
     * ```
     * @see [@typescript-eslint/no-unsafe-enum-comparison](https://typescript-eslint.io/rules/no-unsafe-enum-comparison/)
     */
    [`${typescriptNamespace}/no-unsafe-enum-comparison`]: ERROR,

    /**
     * Avoid accessing `any` members.
     *
     * @see [@typescript-eslint/no-unsafe-member-access](https://typescript-eslint.io/rules/no-unsafe-member-access/)
     */
    [`${typescriptNamespace}/no-unsafe-member-access`]: ERROR,

    /**
     * Avoid returning `any`.
     *
     * @see [@typescript-eslint/no-unsafe-return](https://typescript-eslint.io/rules/no-unsafe-return/)
     */
    [`${typescriptNamespace}/no-unsafe-return`]: ERROR,

    /**
     * Require unary negation to take a number.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * declare const a: string;
     * -a;
     *
     * // ✅ Correct
     * declare const a: number;
     * -a;
     * ```
     * @see [@typescript-eslint/no-unsafe-unary-minus](https://typescript-eslint.io/rules/no-unsafe-unary-minus/)
     */
    [`${typescriptNamespace}/no-unsafe-unary-minus`]: ERROR,

    /**
     * Don't just leave expressions lying around! Use them!
     *
     * @see [no-unused-expressions](https://eslint.org/docs/latest/rules/no-unused-expressions)
     * @see [@typescript-eslint/no-unused-expressions](https://typescript-eslint.io/rules/no-unused-expressions/)
     */
    [`${typescriptNamespace}/no-unused-expressions`]: ERROR,

    /**
     * Avoid using something before is defined.
     *
     * @see [no-use-before-define](https://eslint.org/docs/latest/rules/no-use-before-define)
     * @see [@typescript-eslint/no-use-before-define](https://typescript-eslint.io/rules/no-use-before-define/)
     */
    [`${typescriptNamespace}/no-use-before-define`]: ERROR,

    /**
     * When working with classes, let's not define useless constructors (constructors that only call `super`).
     *
     * @see [no-useless-constructor](https://eslint.org/docs/latest/rules/no-useless-constructor)
     * @see [@typescript-eslint/no-useless-constructor](https://typescript-eslint.io/rules/no-useless-constructor/)
     */
    [`${typescriptNamespace}/no-useless-constructor`]: ERROR,

    /**
     * Disallow empty exports that don't change anything in a module file.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * export const value = 'Hello, world!';
     * export {};
     *
     * // ✅ Correct
     * export const value = 'Hello, world!';
     * ```
     * @see [@typescript-eslint/no-useless-empty-export](https://typescript-eslint.io/rules/no-useless-empty-export/)
     */
    [`${typescriptNamespace}/no-useless-empty-export`]: ERROR,

    /**
     * Disallow using confusing built-in primitive class wrappers.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * let myBigInt: BigInt;
     * let myBoolean: Boolean;
     * let myNumber: Number;
     * let myString: String;
     * let mySymbol: Symbol;
     * let myObject: Object;
     *
     * // ✅ Correct
     * let myBigInt: bigint;
     * let myBoolean: boolean;
     * let myNumber: number;
     * let myString: string;
     * let mySymbol: symbol;
     * let myObject: object;
     * ```
     * @see [@typescript-eslint/no-wrapper-object-types](https://typescript-eslint.io/rules/no-wrapper-object-types/)
     */
    [`${typescriptNamespace}/no-wrapper-object-types`]: ERROR,

    /**
     * If you'll throw, throw errors, not literals.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * throw 'foo';
     *
     * // ✅ Correct
     * throw new Error('foo');
     * ```
     * @see [@typescript-eslint/only-throw-error](https://typescript-eslint.io/rules/only-throw-error/)
     * @see [no-throw-literal](https://eslint.org/docs/latest/rules/no-throw-literal)
     */
    [`${typescriptNamespace}/only-throw-error`]: ERROR,

    /**
     * Disallow parameter properties in class constructors.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Example {
     *   constructor (private name: string) {}
     * }
     *
     * // ✅ Correct
     * class Example {
     *   private name: string;
     *   constructor (name: string) {
     *     this.name = name;
     *   }
     * }
     * ```
     * @see [@typescript-eslint/parameter-properties](https://typescript-eslint.io/rules/parameter-properties/)
     */
    [`${typescriptNamespace}/parameter-properties`]: [ERROR, { prefer: "class-property" }],

    /**
     * Use `as const` instead of writing `"value" as "value"`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = "foo" as "foo";
     * const bar: "bar" = "bar";
     *
     * // ✅ Correct
     * const foo = "foo" as const;
     * const bar = "bar" as const;
     * ```
     * @see [@typescript-eslint/prefer-as-const](https://typescript-eslint.io/rules/prefer-as-const/)
     */
    [`${typescriptNamespace}/prefer-as-const`]: ERROR,

    /**
     * Require destructuring from arrays and/or objects.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = array[0];
     * const bar = array[5];
     *
     * // ✅ Correct
     * const { [0]: foo, [5]: bar } = array;
     * ```
     * @see [@typescript-eslint/prefer-destructuring](https://typescript-eslint.io/rules/prefer-destructuring/)
     */
    [`${typescriptNamespace}/prefer-destructuring`]: ERROR,

    /**
     * Require each enum member value to be explicitly initialized.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const enum Status {
     *   Open = 1,
     *   Close,
     * }
     *
     * const enum Direction {
     *   Up,
     *   Down,
     * }
     *
     * // ✅ Correct
     * const enum Status {
     *   Open = 0,
     *   Close = 1,
     * }
     *
     * const enum Direction {
     *   Up = 0,
     *   Down = 1,
     * }
     * ```
     * @see [@typescript-eslint/prefer-enum-initializers](https://typescript-eslint.io/rules/prefer-enum-initializers/)
     */
    [`${typescriptNamespace}/prefer-enum-initializers`]: ERROR,

    /**
     * Enforce the use of `Array#find` over `Array#filter` followed by when looking for a single result.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * [1, 2, 3].filter(x => x > 1)[0];
     *
     * // ✅ Correct
     * [1, 2, 3].find(x => x > 1);
     * ```
     * @see [@typescript-eslint/prefer-find](https://typescript-eslint.io/rules/prefer-find/)
     */
    [`${typescriptNamespace}/prefer-find`]: ERROR,

    /**
     * If you'll use a `for` loop on an array, use `for/of`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * for (let i = 0; i < foo.length; i++) {
     *   console.log(foo[i]);
     * }
     *
     * // ✅ Correct
     * for (const value of foo) {
     *   console.log(value);
     * }
     * ```
     * @see [@typescript-eslint/prefer-for-of](https://typescript-eslint.io/rules/prefer-for-of/)
     */
    [`${typescriptNamespace}/prefer-for-of`]: ERROR,

    /**
     * Use `() => Type` instead of other verbose alternatives.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * interface Foo {
     *   (): string;
     * }
     *
     * // ✅ Correct
     * type Foo = () => string;
     * ```
     * @see [@typescript-eslint/prefer-function-type](https://typescript-eslint.io/rules/prefer-function-type/)
     */
    [`${typescriptNamespace}/prefer-function-type`]: ERROR,

    /**
     * Avoid `indexOf` and use `includes` instead.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * foo.indexOf("bar") !== -1;
     *
     * // ✅ Correct
     * foo.includes("bar");
     * ```
     * @see [@typescript-eslint/prefer-includes](https://typescript-eslint.io/rules/prefer-includes/)
     */
    [`${typescriptNamespace}/prefer-includes`]: ERROR,

    /**
     * Require all enum members to be literal values.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const str = "Test";
     * const enum Invalid {
     *   A = str, // Variable assignment
     *   B = {}, // Object assignment
     *   C = `A template literal string`, // Template literal
     *   D = new Set(1, 2, 3), // Constructor in assignment
     *   E = 2 + 2, // Expression assignment
     * }
     *
     * // ✅ Correct
     * const enum Valid {
     *   A,
     *   B = "TestStr", // A regular string
     *   C = 4, // A number
     *   D = null,
     *   E = /some_regex/,
     * }
     * ```
     * @see [@typescript-eslint/prefer-includes](https://typescript-eslint.io/rules/prefer-includes/)
     */
    [`${typescriptNamespace}/prefer-literal-enum-member`]: [ERROR, { allowBitwiseExpressions: true }],

    /**
     * Old TypeScript. Use `namespace` instead of `module`.
     *
     * @see [@typescript-eslint/prefer-namespace-keyword](https://typescript-eslint.io/rules/prefer-namespace-keyword/)
     */
    [`${typescriptNamespace}/prefer-namespace-keyword`]: ERROR,

    /**
     * Use `??` instead of a ternary.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = bar !== null && bar !== undefined ? bar : "baz";
     *
     * // ✅ Correct
     * const foo = bar ?? "baz";
     * ```
     * @see [@typescript-eslint/prefer-nullish-coalescing](https://typescript-eslint.io/rules/prefer-nullish-coalescing/)
     */
    [`${typescriptNamespace}/prefer-nullish-coalescing`]: [
      ERROR,
      {
        ignoreConditionalTests: false,
        ignoreMixedLogicalExpressions: false,
      },
    ],

    /**
     * Use `?.` instead of checking every property.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = bar && bar.baz && bar.baz.qux;
     *
     * // ✅ Correct
     * const foo = bar?.baz?.qux;
     * ```
     * @see [@typescript-eslint/prefer-optional-chain](https://typescript-eslint.io/rules/prefer-optional-chain/)
     */
    [`${typescriptNamespace}/prefer-optional-chain`]: ERROR,

    /**
     * In classes, private members should be read only.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Foo {
     *   private neverModified = "bar";
     * }
     *
     * // ✅ Correct
     * class Foo {
     *   private readonly neverModified = "bar";
     * }
     * ```
     * @see [@typescript-eslint/prefer-readonly](https://typescript-eslint.io/rules/prefer-readonly/)
     */
    [`${typescriptNamespace}/prefer-readonly`]: ERROR,

    /**
     * Enforce using type parameter when calling `Array#reduce` instead
     * of casting.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * [1, 2, 3].reduce((array, item) => [...array, item * 2], [] as ReadonlyArray<number>);
     *
     * // ✅ Correct
     * [1, 2, 3].reduce<ReadonlyArray<number>>((array, item) => [...array, item * 2], []);
     * ```
     * @see [@typescript-eslint/prefer-reduce-type-parameter](https://typescript-eslint.io/rules/prefer-reduce-type-parameter)
     */
    [`${typescriptNamespace}/prefer-reduce-type-parameter`]: ERROR,

    /**
     * Enforce `RegExp#exec` over `String#match` if no global flag is
     * provided.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * 'something'.match(/thing/);
     *
     * // ✅ Correct
     * /thing/.exec('something');
     * ```
     * @see [@typescript-eslint/prefer-regexp-exec](https://typescript-eslint.io/rules/prefer-regexp-exec)
     */
    [`${typescriptNamespace}/prefer-regexp-exec`]: ERROR,

    /**
     * Enforce that `this` is used when only `this` type is returned.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * class Example {
     *   someMethod(): Example {
     *     return this;
     *   }
     * }
     *
     * // ✅ Correct
     * class Example {
     *   someMethod(): this {
     *     return this;
     *   }
     * }
     * ```
     * @see [@typescript-eslint/prefer-return-this-type](https://typescript-eslint.io/rules/prefer-return-this-type)
     */
    [`${typescriptNamespace}/prefer-return-this-type`]: ERROR,

    /**
     * Enforce using `String#startsWith` and `String#endsWith` over
     * other equivalent methods of checking substrings.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * // starts with
     * foo[0] === 'b';
     * foo.charAt(0) === 'b';
     * foo.indexOf('bar') === 0;
     * foo.slice(0, 3) === 'bar';
     * foo.substring(0, 3) === 'bar';
     * foo.match(/^bar/) != null;
     * /^bar/.test(foo);
     *
     * // ends with
     * foo[foo.length - 1] === 'b';
     * foo.charAt(foo.length - 1) === 'b';
     * foo.lastIndexOf('bar') === foo.length - 3;
     * foo.slice(-3) === 'bar';
     * foo.substring(foo.length - 3) === 'bar';
     * foo.match(/bar$/) != null;
     * /bar$/.test(foo);
     *
     * // ✅ Correct
     * // starts with
     * foo.startsWith('bar');
     *
     * // ends with
     * foo.endsWith('bar');
     * ```
     * @see [@typescript-eslint/prefer-string-starts-ends-with](https://typescript-eslint.io/rules/prefer-string-starts-ends-with)
     */
    [`${typescriptNamespace}/prefer-string-starts-ends-with`]: ERROR,

    /**
     * Always use `Array#sort` with a comparing function.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * foo.sort();
     *
     * // ✅ Correct
     * foo.sort((a, z) => a - z);
     * ```
     * @see [@typescript-eslint/require-array-sort-compare](https://typescript-eslint.io/rules/require-array-sort-compare/)
     */
    [`${typescriptNamespace}/require-array-sort-compare`]: ERROR,

    /**
     * Use `await` if you are using `async`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = async () => "bar";
     *
     * // ✅ Correct
     * const foo = async () => await "bar";
     * ```
     * @see [@typescript-eslint/require-await](https://typescript-eslint.io/rules/require-await/)
     */
    [`${typescriptNamespace}/require-await`]: ERROR,

    /**
     * Use `+` with the same type (`number` or `string`).
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = "bar" + 42;
     *
     * // ✅ Correct
     * const foo = "bar" + "baz";
     * ```
     * @see [@typescript-eslint/restrict-plus-operands](https://typescript-eslint.io/rules/restrict-plus-operands/)
     */
    [`${typescriptNamespace}/restrict-plus-operands`]: ERROR,

    /**
     * Only use strings or numbers inside template expressions.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const foo = `bar${true}`;
     * const bar = `baz${undefined}`;
     *
     * // ✅ Correct
     * const foo = `bar${42}`;
     * const bar = `baz${"qux"}`;
     * const baz = `qux${undefined ?? "default"}`;
     * ```
     * @see [@typescript-eslint/restrict-template-expressions](https://typescript-eslint.io/rules/restrict-template-expressions/)
     */
    [`${typescriptNamespace}/restrict-template-expressions`]: [ERROR, { allowNumber: true }],

    /**
     * Comparisons should be applied to booleans only (not
     * falsy/truthy).
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * if (foo) // …
     * if (!foo) // …
     *
     * // ✅ Correct
     * if (foo !== "") // …
     * if (foo === undefined) // …
     * ```
     * @see [@typescript-eslint/strict-boolean-expressions](https://typescript-eslint.io/rules/strict-boolean-expressions/)
     */
    [`${typescriptNamespace}/strict-boolean-expressions`]: [ERROR, { allowNullableBoolean: true }],

    /**
     * If you'll use switch, make sure to cover every possible value.
     *
     * @see [@typescript-eslint/switch-exhaustiveness-check](https://typescript-eslint.io/rules/switch-exhaustiveness-check/)
     */
    [`${typescriptNamespace}/switch-exhaustiveness-check`]: ERROR,

    /**
     * Old TypeScript.
     *
     * @see [@typescript-eslint/triple-slash-reference](https://typescript-eslint.io/rules/triple-slash-reference/)
     */
    [`${typescriptNamespace}/triple-slash-reference`]: ERROR,

    /**
     * Unify signatures instead of overloading.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * function x(x: number): void;
     * function x(x: string): void;
     *
     * // ✅ Correct
     * function x(x: number | string): void;
     * ```
     * @see [@typescript-eslint/unified-signatures](https://typescript-eslint.io/rules/unified-signatures/)
     */
    [`${typescriptNamespace}/unified-signatures`]: ERROR,

    /**
     * Enforce typing arguments in `.catch()` callbacks as `unknown`.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * Promise.reject(new Error('I will reject!')).catch(error => {
     *   console.log(error);
     * });
     *
     * Promise.reject(new Error('I will reject!')).catch((error: any) => {
     *   console.log(error);
     * });
     *
     * Promise.reject(new Error('I will reject!')).catch((error: Error) => {
     *   console.log(error);
     * });
     *
     * // ✅ Correct
     * Promise.reject(new Error('I will reject!')).catch((error: unknown) => {
     *   console.log(error);
     * });
     * ```
     * @see [@typescript-eslint/use-unknown-in-catch-callback-variable](https://typescript-eslint.io/rules/use-unknown-in-catch-callback-variable/)
     */
    [`${typescriptNamespace}/use-unknown-in-catch-callback-variable`]: ERROR,

    // Rules covered by `@typescript-eslint` or by the language itself.
    ...off(
      "camelcase",
      "class-methods-use-this",
      "consistent-return",
      "constructor-super",
      "default-param-last",
      "dot-notation",
      "getter-return",
      "id-match",
      "init-declarations",
      "max-params",
      "new-cap",
      "no-array-constructor",
      "no-async-promise-executor",
      "no-case-declarations",
      "no-class-assign",
      "no-cond-assign",
      "no-const-assign",
      "no-constant-binary-expression",
      "no-constant-condition",
      "no-delete-var",
      "no-dupe-args",
      "no-dupe-class-members",
      "no-dupe-else-if",
      "no-dupe-keys",
      "no-extra-boolean-cast",
      "no-fallthrough",
      "no-func-assign",
      "no-global-assign",
      "no-implicit-globals",
      "no-implied-eval",
      "no-invalid-this",
      "no-loop-func",
      "no-nonoctal-decimal-escape",
      "no-obj-calls",
      "no-octal-escape",
      "no-promise-executor-return",
      "no-redeclare",
      "no-restricted-imports",
      "no-setter-return",
      "no-shadow",
      "no-this-before-super",
      "no-throw-literal",
      "no-undef-init",
      "no-undef",
      "no-undefined",
      "no-underscore-dangle",
      "no-unreachable",
      "no-unsafe-negation",
      "no-unsafe-optional-chaining",
      "no-unused-expressions",
      "no-unused-labels",
      "no-unused-private-class-members",
      "no-unused-vars",
      "no-use-before-define",
      "no-useless-backreference",
      "no-useless-constructor",
      "no-with",
      "prefer-destructuring",
      "require-await",
      "use-isnan",
      "valid-typeof",
      `${typescriptNamespace}/no-dupe-class-members`,
      `${typescriptNamespace}/no-redeclare`,
      `${typescriptNamespace}/no-unused-vars`,
    ),
  },
});
