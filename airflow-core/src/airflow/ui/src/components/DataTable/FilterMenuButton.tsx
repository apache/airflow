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
import { IconButton } from "@chakra-ui/react";
import { flexRender, type Header, type Table } from "@tanstack/react-table";
import { MdFilterList } from "react-icons/md";

import { Menu } from "src/components/ui";
import { Checkbox } from "src/components/ui/Checkbox";

type Props<TData> = {
  readonly table: Table<TData>;
};

const FilterMenuButton = <TData,>({ table }: Props<TData>) => (
  <Menu.Root closeOnSelect={false} positioning={{ placement: "bottom" }}>
    <Menu.Trigger asChild>
      <IconButton
        aria-label="Filter table columns"
        margin={1}
        padding={0}
        title="Filter table columns"
        variant="ghost"
      >
        <MdFilterList size="1" />
      </IconButton>
    </Menu.Trigger>
    <Menu.Content>
      {table.getAllLeafColumns().map((column) => {
        const text = flexRender(column.columnDef.header, {
          column,
          header: { column } as Header<TData, unknown>,
          table,
        });

        return text?.toString ? (
          <Menu.Item asChild key={column.id} value={column.id}>
            <Checkbox
              checked={column.getIsVisible()}
              // At least one item needs to be visible
              disabled={table.getVisibleFlatColumns().length < 2 && column.getIsVisible()}
              onChange={() => {
                column.toggleVisibility();
              }}
            >
              {text}
            </Checkbox>
          </Menu.Item>
        ) : undefined;
      })}
    </Menu.Content>
  </Menu.Root>
);

export default FilterMenuButton;
