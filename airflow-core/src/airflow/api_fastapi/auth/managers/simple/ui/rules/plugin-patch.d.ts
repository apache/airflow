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

declare module "eslint-plugin-local-patch" {
  import type { FlatConfig } from "@typescript-eslint/utils/ts-eslint";

  /**
   * @deprecated This type was patched because it's currently broken.
   */
  const plugin: FlatConfig.Plugin;

  export default plugin;
}

declare module "@stylistic/eslint-plugin" {
  export { default } from "eslint-plugin-local-patch";
}

declare module "eslint-plugin-jsx-a11y" {
  export { default } from "eslint-plugin-local-patch";
}

declare module "eslint-plugin-react" {
  export { default } from "eslint-plugin-local-patch";
}

declare module "eslint-plugin-react-hooks" {
  export { default } from "eslint-plugin-local-patch";
}

declare module "eslint-plugin-react-refresh" {
  export { default } from "eslint-plugin-local-patch";
}
