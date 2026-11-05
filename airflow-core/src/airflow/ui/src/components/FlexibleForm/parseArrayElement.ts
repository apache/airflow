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

export type ArrayElement = boolean | number | string;

/**
 * Parse one typed-in entry to the element type from `items.type`; `undefined` means invalid.
 * Without an `items.type`, JSON primitives keep their type ("1" stays a number) and
 * everything else is taken as a string.
 */
export const parseArrayElement = (raw: string, itemsType: unknown): ArrayElement | undefined => {
  const text = raw.trim();

  if (text === "") {
    return undefined;
  }
  switch (itemsType) {
    case "boolean":
      return text === "true" || text === "false" ? text === "true" : undefined;
    case "integer": {
      const num = Number(text);

      return Number.isInteger(num) ? num : undefined;
    }
    case "number": {
      const num = Number(text);

      return Number.isFinite(num) ? num : undefined;
    }
    case "string":
      return text;
    default: {
      try {
        const parsed = JSON.parse(text) as unknown;

        if (["boolean", "number", "string"].includes(typeof parsed)) {
          return parsed as ArrayElement;
        }
      } catch {
        // Not valid JSON, keep the raw text as a string.
      }

      return text;
    }
  }
};
