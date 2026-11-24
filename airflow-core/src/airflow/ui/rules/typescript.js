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

import { ERROR, WARN } from "./levels.js";

/**
 * ESLint TypeScript namespace.
 */
export const typescriptNamespace = "@typescript-eslint";

/**
 * ESLint TypeScript rules.
 * @see [@typescript-eslint/eslint-plugin](https://typescript-eslint.io/rules/)
 */
export const typescriptRules = /** @type {const} @satisfies {FlatConfig.Config} */ ({
  files: ["**/*.ts", "**/*.tsx", "**/*.js", "**/*.jsx"],
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
       * oxlint runs the type-aware rules, so ESLint no longer loads a tsconfig. Without one, scope
       * analysis needs the app's `lib` to know globals such as `globalThis`; otherwise
       * `unicorn/consistent-function-scoping` flags functions that only use them.
       */
      lib: ["es2022", "dom", "dom.iterable"],

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
        format: null,
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
     * -   The `React` default and namespace import — the automatic JSX runtime
     *     makes it unnecessary.
     *
     * @see [@typescript-eslint/no-restricted-imports](https://typescript-eslint.io/rules/no-restricted-imports/)
     * @see [no-restricted-imports](https://eslint.org/docs/latest/rules/no-restricted-imports)
     */
    [`${typescriptNamespace}/no-restricted-imports`]: [
      ERROR,
      {
        paths: [
          {
            importNames: ["default"],
            message:
              'The automatic JSX runtime makes the React namespace import unnecessary. Import what you need by name, e.g. `import { forwardRef, type ReactNode } from "react"`.',
            name: "react",
          },
        ],
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
     * Do not type components with `FC` (or `FunctionComponent`, `VFC`).
     * Annotate the props parameter instead, and use `ComponentType` for a
     * component held in a value.
     *
     * @example
     * ```typescript
     * // ❌ Incorrect
     * const Foo: React.FC<FooProps> = ({ bar }) => <div>{bar}</div>;
     *
     * // ✅ Correct
     * const Foo = ({ bar }: FooProps) => <div>{bar}</div>;
     * ```
     * @see [@typescript-eslint/no-restricted-types](https://typescript-eslint.io/rules/no-restricted-types/)
     */
    [`${typescriptNamespace}/no-restricted-types`]: [
      ERROR,
      {
        types: {
          FC: "Do not type components with `FC`. Annotate the props parameter instead: `const Foo = ({ bar }: FooProps) => ...`, or use `ComponentType` for a component value.",
          FunctionComponent:
            "Do not type components with `FunctionComponent`. Annotate the props parameter instead: `const Foo = ({ bar }: FooProps) => ...`, or use `ComponentType` for a component value.",
          "React.FC":
            "Do not type components with `React.FC`. Annotate the props parameter instead: `const Foo = ({ bar }: FooProps) => ...`, or use `ComponentType` for a component value.",
          "React.FunctionComponent":
            "Do not type components with `React.FunctionComponent`. Annotate the props parameter instead: `const Foo = ({ bar }: FooProps) => ...`, or use `ComponentType` for a component value.",
          "React.VFC":
            "Do not type components with `React.VFC`. Annotate the props parameter instead: `const Foo = ({ bar }: FooProps) => ...`, or use `ComponentType` for a component value.",
          VFC: "Do not type components with `VFC`. Annotate the props parameter instead: `const Foo = ({ bar }: FooProps) => ...`, or use `ComponentType` for a component value.",
        },
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
     * Don't do `extends any` or `extends unknown`. That's the default.
     *
     * @see [@typescript-eslint/no-unnecessary-type-constraint](https://typescript-eslint.io/rules/no-unnecessary-type-constraint/)
     */
    [`${typescriptNamespace}/no-unnecessary-type-constraint`]: ERROR,

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
  },
});
