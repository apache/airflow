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
import { FiTag } from "react-icons/fi";
import { useMatch, useSearchParams } from "react-router-dom";

import type { DagTagResponse } from "openapi/requests/types.gen";

import { RouterLink } from "src/system-components";

import { LimitedItemsList } from "src/components/LimitedItemsList";

import { SearchParamsKeys } from "src/constants/searchParams";

const MAX_TAGS = 3;

const { OFFSET, TAGS, TAGS_MATCH_MODE } = SearchParamsKeys;

// Adds the tag to the current filters, or removes it if already selected. The match mode is
// left as the user set it, and only dropped once no tag remains.
const buildTagFilterSearch = (searchParams: URLSearchParams, tag: string): string => {
  const next = new URLSearchParams(searchParams);
  const selected = next.getAll(TAGS);
  const tags = selected.includes(tag) ? selected.filter((name) => name !== tag) : [...selected, tag];

  next.delete(TAGS);
  next.delete(OFFSET);
  for (const name of tags) {
    next.append(TAGS, name);
  }
  if (tags.length === 0) {
    next.delete(TAGS_MATCH_MODE);
  }

  return next.toString();
};

type Props = {
  readonly hideIcon?: boolean;
  readonly tags: Array<DagTagResponse>;
};

export const DagTags = ({ hideIcon = false, tags }: Props) => {
  const [searchParams] = useSearchParams();
  // Other pages showing tags, like the Dag header above every Dag tab, have their own params
  // (e.g. a Runs table `sort`) that the Dags list would misread, so only the list keeps them.
  const keepListFilters = useMatch("/dags") !== null;
  const selectedTags = keepListFilters ? searchParams.getAll(TAGS) : [];

  return (
    <LimitedItemsList
      icon={hideIcon ? undefined : <FiTag data-testid="dag-tag" />}
      interactive
      items={tags.map(({ name }) => (
        <RouterLink
          fontWeight={selectedTags.includes(name) ? "bold" : undefined}
          key={name}
          to={
            keepListFilters
              ? { pathname: "/dags", search: buildTagFilterSearch(searchParams, name) }
              : `/dags?${TAGS}=${encodeURIComponent(name)}`
          }
        >
          {name}
        </RouterLink>
      ))}
      maxItems={MAX_TAGS}
    />
  );
};
