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
import { useEffect, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";

import { getNormalizedTagMatchMode, getUniqueSearchParamValues } from "./DagsFilters/normalizeDagsFilters";

const { OFFSET, TAGS, TAGS_MATCH_MODE }: SearchParamsKeysType = SearchParamsKeys;

type TagMatchMode = "all" | "any";

type UseTagFilterOptions = {
  readonly materializeSavedTags?: boolean;
};

export const useTagFilter = ({ materializeSavedTags = true }: UseTagFilterOptions = {}) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [savedTags, setSavedTags] = useLocalStorage<Array<string>>(TAGS, []);
  const [savedTagMatchMode, setSavedTagMatchMode] = useLocalStorage<TagMatchMode>(TAGS_MATCH_MODE, "any");

  const urlTags = getUniqueSearchParamValues(searchParams, TAGS);
  const urlMatchMode = searchParams.get(TAGS_MATCH_MODE);
  const uniqueSavedTags = [...new Set(savedTags.filter(Boolean))];
  const shouldRestoreSavedTags = useRef(urlTags.length === 0 && uniqueSavedTags.length > 0);

  if (urlTags.length > 0) {
    shouldRestoreSavedTags.current = false;
  }

  // Saved preferences are materialized once so later Back/Forward navigation can treat an absent URL as empty.
  const selectedTags = urlTags.length > 0 ? urlTags : shouldRestoreSavedTags.current ? uniqueSavedTags : [];
  const tagFilterMode: TagMatchMode =
    urlMatchMode === null
      ? urlTags.length === 0
        ? getNormalizedTagMatchMode(savedTagMatchMode)
        : "any"
      : getNormalizedTagMatchMode(urlMatchMode);

  useEffect(() => {
    if (shouldRestoreSavedTags.current && materializeSavedTags) {
      shouldRestoreSavedTags.current = false;
      const materializedParams = new URLSearchParams(searchParams);

      [...new Set(savedTags.filter(Boolean))].forEach((tag) => materializedParams.append(TAGS, tag));
      materializedParams.set(TAGS_MATCH_MODE, getNormalizedTagMatchMode(savedTagMatchMode));
      setSearchParams(materializedParams, { replace: true });
    }
  }, [materializeSavedTags, savedTagMatchMode, savedTags, searchParams, setSearchParams]);

  const setSelectedTags = (tags: Array<string>) => {
    const uniqueTags = [...new Set(tags.filter(Boolean))];

    searchParams.delete(TAGS);
    uniqueTags.forEach((tag) => {
      searchParams.append(TAGS, tag);
    });
    if (uniqueTags.length === 0) {
      searchParams.delete(TAGS_MATCH_MODE);
    }
    searchParams.delete(OFFSET);
    setSearchParams(searchParams);
    setSavedTags(uniqueTags);
  };

  const setTagFilterMode = (mode: TagMatchMode) => {
    searchParams.set(TAGS_MATCH_MODE, mode);
    searchParams.delete(OFFSET);
    setSearchParams(searchParams);
    setSavedTagMatchMode(mode);
  };

  const resetSavedTagFilter = () => {
    setSavedTags([]);
    setSavedTagMatchMode("any");
  };

  return { resetSavedTagFilter, selectedTags, setSelectedTags, setTagFilterMode, tagFilterMode };
};
