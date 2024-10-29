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
import { Box, SimpleGrid, Skeleton } from "@chakra-ui/react";
import { flexRender, type Table as TanStackTable } from "@tanstack/react-table";

import type { CardDef } from "./types";

type DataTableProps<TData> = {
  readonly cardDef: CardDef<TData>;
  readonly isLoading?: boolean;
  readonly table: TanStackTable<TData>;
};

export const CardList = <TData,>({
  cardDef,
  isLoading,
  table,
}: DataTableProps<TData>) => {
  const defaultGridProps = { column: { base: 1 }, spacing: 2 };

  return (
    <Box overflow="auto" width="100%">
      <SimpleGrid {...{ ...defaultGridProps, ...cardDef.gridProps }}>
        {table.getRowModel().rows.map((row) => (
          <Box key={row.id}>
            {Boolean(isLoading) &&
              (cardDef.meta?.customSkeleton ?? (
                <Skeleton
                  data-testid="skeleton"
                  display="inline-block"
                  height={80}
                  width="100%"
                />
              ))}
            {!Boolean(isLoading) &&
              flexRender(cardDef.card, { row: row.original })}
          </Box>
        ))}
      </SimpleGrid>
    </Box>
  );
};
