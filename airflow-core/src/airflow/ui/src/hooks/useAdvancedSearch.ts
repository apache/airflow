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
import { useLocalStorage } from "usehooks-ts";

import { advancedSearchKey } from "src/constants/localStorage";

// Toggle is intentionally NOT mirrored in the URL: shared links default to the
// fast prefix-search behavior, and recipients can opt back into substring search
// per searchbar if they want it.
export const useAdvancedSearch = (key: string) => {
  const [enabled, setEnabled] = useLocalStorage<boolean>(advancedSearchKey(key), false);

  return { enabled, onToggle: setEnabled };
};

type AdvancedSearchArgOptions<TPrefix extends string, TPattern extends string> = {
  patternApiKey: TPattern;
  prefixApiKey: TPrefix;
  storageKey: string;
  value: string | null | undefined;
};

// Build the right API arg object for a search field that supports both prefix
// (`*_prefix_pattern`) and full-substring (`*_pattern`) variants. The toggle
// state is read from localStorage via ``useAdvancedSearch``, so the pill in the
// FilterBar and the page query stay in sync.
export const useAdvancedSearchArg = <TPrefix extends string, TPattern extends string>({
  patternApiKey,
  prefixApiKey,
  storageKey,
  value,
}: AdvancedSearchArgOptions<TPrefix, TPattern>): Partial<Record<TPattern | TPrefix, string>> => {
  const { enabled } = useAdvancedSearch(storageKey);

  if (value === null || value === undefined || value === "") {
    return {};
  }

  return { [enabled ? patternApiKey : prefixApiKey]: value } as Partial<Record<TPattern | TPrefix, string>>;
};
