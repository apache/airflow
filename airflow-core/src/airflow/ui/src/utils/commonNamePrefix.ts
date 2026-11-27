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

const SEPARATORS = new Set(["-", ".", "_"]);

// Cut back to the last separator so the prefix never ends in the middle of a word.
const toBoundary = (prefix: string) => {
  let end = prefix.length;

  while (end > 0 && !SEPARATORS.has(prefix[end - 1] ?? "")) {
    end -= 1;
  }

  return prefix.slice(0, end);
};

const sharedPrefix = (first: string, second: string) => {
  let index = 0;

  while (index < first.length && first[index] === second[index]) {
    index += 1;
  }

  return first.slice(0, index);
};

/**
 * The longest separator-ended prefix shared by at least half (and at least two) of the names,
 * or "" when there is none. A Dag often has a few setup tasks named differently from the rest,
 * so requiring every name to share the prefix would almost never find one.
 */
export const commonNamePrefix = (names: Array<string>): string => {
  const sorted = [...new Set(names)].sort();
  const minCount = Math.max(2, Math.ceil(sorted.length / 2));
  let best = "";

  for (let index = 1; index < sorted.length; index += 1) {
    const candidate = toBoundary(sharedPrefix(sorted[index - 1] ?? "", sorted[index] ?? ""));

    if (
      candidate.length > best.length &&
      sorted.filter((name) => name.startsWith(candidate) && name.length > candidate.length).length >= minCount
    ) {
      best = candidate;
    }
  }

  return best;
};
