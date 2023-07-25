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

const config = {
  verbose: true,
  transform: {
    "^.+\\.[jt]sx?$": "babel-jest",
  },
  testEnvironment: "jsdom",
  setupFilesAfterEnv: ["./jest-setup.js"],
  moduleDirectories: ["node_modules"],
  moduleNameMapper: {
    // Listing all aliases
    "^src/(.*)$": "<rootDir>/static/js/$1",
  },
  transformIgnorePatterns: [
    `node_modules/(?!${[
      // specify modules that needs to be transformed for jest. (esm modules)
      "react-markdown",
      "vfile",
      "vfile-message",
      "unist",
      "unified",
      "bail",
      "is-plain-obj",
      "trough",
      "remark-parse",
      "mdast",
      "micromark",
      "decode-named-character-reference",
      "character-entities",
      "remark-rehype",
      "trim-lines",
      "property-information",
      "hast",
      "space-separated-tokens",
      "comma-separated-tokens",
      "remark-gfm",
      "ccount",
      "escape-string-regexp",
      "markdown-table",
    ].join("|")})`,
  ],
};

module.exports = config;
