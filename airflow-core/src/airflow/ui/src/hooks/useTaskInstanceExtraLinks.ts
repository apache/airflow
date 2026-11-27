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
import { useTaskInstanceServiceGetExtraLinks } from "openapi/queries";

import { getSafeExternalUrl } from "src/utils/links";

export type ExtraLink = {
  readonly name: string;
  readonly url: string;
};

type Props = {
  readonly dagId: string;
  readonly dagRunId: string;
  readonly enabled?: boolean;
  readonly mapIndex?: number;
  readonly regionId?: string | null;
  readonly regionIndex?: number | null;
  readonly taskId: string;
  readonly tryNumber?: number;
};

export const useTaskInstanceExtraLinks = ({
  dagId,
  dagRunId,
  enabled = false,
  mapIndex,
  regionId,
  regionIndex,
  taskId,
  tryNumber,
}: Props): Array<ExtraLink> => {
  const { data } = useTaskInstanceServiceGetExtraLinks(
    {
      dagId,
      dagRunId,
      mapIndex,
      regionId: regionId ?? undefined,
      regionIndex: regionIndex ?? undefined,
      taskId,
      tryNumber,
    },
    undefined,
    { enabled },
  );

  const links: Array<ExtraLink> = [];

  Object.entries(data?.extra_links ?? {}).forEach(([name, url]) => {
    if (url === null) {
      return;
    }

    const safeUrl = getSafeExternalUrl(url);

    if (safeUrl !== undefined) {
      links.push({ name, url: safeUrl });
    }
  });

  return links;
};
