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
import { useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { advancedSearchKey } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";

// The "match anywhere" (substring) toggle is mirrored in the URL so a filtered search can be shared
// and reproduced in both directions. ``match_anywhere`` is a repeated param carrying each searchbar's
// explicit choice by key: ``key`` for on, ``-key`` for off
// (`?match_anywhere=dag_id&match_anywhere=-run_id`), keeping each searchbar independent. An explicit URL
// entry wins — a shared link reproduces the sender's on/off choices whatever the recipient's own
// preferences — and a key with no entry (e.g. landing through the nav) falls back to the per-searchbar
// localStorage preference. Toggling writes the explicit on/off entry and localStorage.
export const useAdvancedSearch = (key: string) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [storedEnabled, setStoredEnabled] = useLocalStorage<boolean>(advancedSearchKey(key), false);

  const urlValues = searchParams.getAll(SearchParamsKeys.MATCH_ANYWHERE);
  const enabled = urlValues.includes(key) ? true : urlValues.includes(`-${key}`) ? false : storedEnabled;

  const onToggle = (nextEnabled: boolean) => {
    setSearchParams((previous) => {
      const next = new URLSearchParams(previous);
      const retained = next
        .getAll(SearchParamsKeys.MATCH_ANYWHERE)
        .filter((value) => value !== key && value !== `-${key}`);

      next.delete(SearchParamsKeys.MATCH_ANYWHERE);
      retained.forEach((value) => next.append(SearchParamsKeys.MATCH_ANYWHERE, value));
      next.append(SearchParamsKeys.MATCH_ANYWHERE, nextEnabled ? key : `-${key}`);

      return next;
    });
    setStoredEnabled(nextEnabled);
  };

  return { enabled, onToggle };
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
