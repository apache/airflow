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
import { Skeleton } from "@chakra-ui/react";

import type { MetaColumn } from "./types";

export const createSkeletonMock = <TData,>(
  mode: "card" | "table",
  skeletonCount: number,
  columnDefs: Array<MetaColumn<TData>>,
) => {
  const colDefs = columnDefs.map((colDef) => ({
    ...colDef,
    cell: () => {
      if (mode === "table") {
        return (
          colDef.meta?.customSkeleton ?? (
            <Skeleton
              data-testid="skeleton"
              display="inline-block"
              height="16px"
              width={colDef.meta?.skeletonWidth ?? 200}
            />
          )
        );
      }

      return undefined;
    },
  }));

  const data = [...Array<TData>(skeletonCount)].map(() => ({}));

  return { columns: colDefs, data };
};
