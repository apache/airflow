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

const { TextEncoder, TextDecoder } = require("util");

if (typeof globalThis.TextEncoder === "undefined") {
  globalThis.TextEncoder = TextEncoder;
}
if (typeof globalThis.TextDecoder === "undefined") {
  globalThis.TextDecoder = TextDecoder;
}

// Mock window.matchMedia for Chakra UI's useMediaQuery
if (typeof globalThis.window !== "undefined" && !globalThis.window.matchMedia) {
  // eslint-disable-next-line func-names
  globalThis.window.matchMedia = function (query) {
    return {
      matches: false,
      media: query,
      onchange: null,
      addListener() {},
      removeListener() {},
      addEventListener() {},
      removeEventListener() {},
      dispatchEvent() {
        return false;
      },
    };
  };
}

// Unwrap React 19 AggregateError for readable test failure messages
const OrigAggregateError = globalThis.AggregateError;
if (OrigAggregateError) {
  const patchedAE = function AggregateError(errors, message) {
    const ae = new OrigAggregateError(errors, message);
    if (errors && errors.length > 0) {
      ae.message = `${message || "AggregateError"}\n  Caused by:\n${errors
        .map((e) => `  - ${e.stack || e.message || e}`)
        .join("\n")}`;
    }
    return ae;
  };
  patchedAE.prototype = OrigAggregateError.prototype;
  globalThis.AggregateError = patchedAE;
}
