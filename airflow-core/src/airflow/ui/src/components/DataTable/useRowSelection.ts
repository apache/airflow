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
import { useState } from "react";

type UseRowSelectionProps<T> = {
  data?: Array<T>;
  getKey: (item: T) => string;
};

export type GetColumnsParams = {
  multiTeam: boolean;
};

export const useRowSelection = <T>({ data = [], getKey }: UseRowSelectionProps<T>) => {
  const [selectedItems, setSelectedItems] = useState<Map<string, T>>(new Map());
  const currentItems = new Map(data.map((item) => [getKey(item), item]));
  const selectedRows = new Map([...selectedItems.keys()].map((key) => [key, true]));

  const handleRowSelect = (key: string, isChecked: boolean) => {
    setSelectedItems((prev) => {
      const isAlreadySelected = prev.has(key);

      if (isChecked && !isAlreadySelected) {
        const item = currentItems.get(key);

        return item === undefined ? prev : new Map(prev).set(key, item);
      } else if (!isChecked && isAlreadySelected) {
        const newMap = new Map(prev);

        newMap.delete(key);

        return newMap;
      }

      return prev;
    });
  };

  const handleSelectAll = (isChecked: boolean) => {
    setSelectedItems((prev) => {
      const newMap = new Map(prev);

      if (isChecked) {
        data.forEach((item) => newMap.set(getKey(item), item));
      } else {
        data.forEach((item) => newMap.delete(getKey(item)));
      }

      return newMap;
    });
  };

  const allRowsSelected = data.length > 0 && data.every((item) => selectedRows.has(getKey(item)));

  const clearSelections = () => {
    setSelectedItems(new Map());
  };

  const deselectKeys = (keys: Array<string>) => {
    if (keys.length === 0) {
      return;
    }
    setSelectedItems((prev) => {
      const newMap = new Map(prev);

      keys.forEach((key) => newMap.delete(key));

      return newMap;
    });
  };

  return {
    allRowsSelected,
    clearSelections,
    deselectKeys,
    handleRowSelect,
    handleSelectAll,
    selectedItems: [...selectedItems].map(([key, item]) => currentItems.get(key) ?? item),
    selectedRows,
  };
};
