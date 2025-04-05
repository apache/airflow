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
import { useState, useCallback, useMemo } from "react";

type UseRowSelectionProps<T> = {
  data?: Array<T>;
  getKey: (item: T) => string;
};

export type GetColumnsParams = {
  allRowsSelected: boolean;
  onRowSelect: (key: string, isChecked: boolean) => void;
  onSelectAll: (isChecked: boolean) => void;
  selectedRows: Map<string, boolean>;
};

export const useRowSelection = <T>({ data = [], getKey }: UseRowSelectionProps<T>) => {
  const [selectedRows, setSelectedRows] = useState<Map<string, boolean>>(new Map());

  const handleRowSelect = useCallback((key: string, isChecked: boolean) => {
    setSelectedRows((prev) => {
      const isAlreadySelected = prev.has(key);

      if (isChecked && !isAlreadySelected) {
        return new Map(prev).set(key, true);
      } else if (!isChecked && isAlreadySelected) {
        const newMap = new Map(prev);

        newMap.delete(key);

        return newMap;
      }

      return prev;
    });
  }, []);

  const handleSelectAll = useCallback(
    (isChecked: boolean) => {
      setSelectedRows((prev) => {
        const newMap = new Map(prev);

        if (isChecked) {
          data.forEach((item) => newMap.set(getKey(item), true));
        } else {
          data.forEach((item) => newMap.delete(getKey(item)));
        }

        return newMap;
      });
    },
    [data, getKey],
  );

  const allRowsSelected = useMemo(
    () => data.length > 0 && data.every((item) => selectedRows.has(getKey(item))),
    [data, selectedRows, getKey],
  );

  const clearSelections = useCallback(() => {
    setSelectedRows(new Map());
  }, []);

  return {
    allRowsSelected,
    clearSelections,
    handleRowSelect,
    handleSelectAll,
    selectedRows,
  };
};
