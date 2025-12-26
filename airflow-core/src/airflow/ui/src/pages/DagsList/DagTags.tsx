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
import { Link as RouterLink } from "react-router-dom";

import type { DagTagResponse } from "openapi/requests/types.gen";
import { LimitedItemsList } from "src/components/LimitedItemsList";
import { SearchParamsKeys } from "src/constants/searchParams";

const MAX_TAGS = 3;

type Props = {
  readonly hideIcon?: boolean;
  readonly tags: Array<DagTagResponse>;
};

export const DagTags = ({ hideIcon = false, tags }: Props) => (
  <LimitedItemsList
    icon={hideIcon ? undefined : <FiTag data-testid="dag-tag" />}
    interactive
    items={tags.map(({ name }) => (
      <RouterLink key={name} to={`/dags?${SearchParamsKeys.TAGS}=${encodeURIComponent(name)}`}>
        {name}
      </RouterLink>
    ))}
    maxItems={MAX_TAGS}
  />
);
