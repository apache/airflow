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
      "ansi_up",
      "axios",
      "bail",
      "ccount",
      "character-entities",
      "comma-separated-tokens",
      "decode-named-character-reference",
      "escape-string-regexp",
      "hast",
      "is-plain-obj",
      "markdown-table",
      "mdast",
      "micromark",
      "property-information",
      "react-markdown",
      "remark-gfm",
      "remark-parse",
      "remark-rehype",
      "space-separated-tokens",
      "trim-lines",
      "trough",
      "unified",
      "unist",
      "vfile",
      "vfile-message",
    ].join("|")})`,
  ],
};

module.exports = config;
