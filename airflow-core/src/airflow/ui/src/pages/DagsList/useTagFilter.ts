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

import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";

const { OFFSET, TAGS, TAGS_MATCH_MODE }: SearchParamsKeysType = SearchParamsKeys;

type TagMatchMode = "all" | "any";

export const useTagFilter = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const selectedTags = searchParams.getAll(TAGS);
  const urlMatchMode = searchParams.get(TAGS_MATCH_MODE);
  const tagFilterMode: TagMatchMode = urlMatchMode === null ? "any" : (urlMatchMode as TagMatchMode);

  const setSelectedTags = (tags: Array<string>) => {
    searchParams.delete(TAGS);
    tags.forEach((tag) => {
      searchParams.append(TAGS, tag);
    });
    searchParams.delete(OFFSET);
    setSearchParams(searchParams);
  };

  const setTagFilterMode = (mode: TagMatchMode) => {
    searchParams.set(TAGS_MATCH_MODE, mode);
    searchParams.delete(OFFSET);
    setSearchParams(searchParams);
  };

  return { selectedTags, setSelectedTags, setTagFilterMode, tagFilterMode };
};
