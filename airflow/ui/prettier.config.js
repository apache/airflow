/**
 * @import { Config } from "prettier";
 * @import { PluginConfig } from "@trivago/prettier-plugin-sort-imports";
 */

/**
 * Prettier configuration.
 */
export default /** @type {const} @satisfies {Config & PluginConfig} */ ({
  endOfLine: "lf",
  importOrder: ["<THIRD_PARTY_MODULES>", "^(src|openapi)/", "^[./]"],
  importOrderSeparation: true,
  jsxSingleQuote: false,
  plugins: ["@trivago/prettier-plugin-sort-imports"],
  printWidth: 80,
  singleQuote: false,
  tabWidth: 2,
  trailingComma: "all",
  useTabs: false,
});
