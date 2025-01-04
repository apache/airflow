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

export type RowSelectionHookParams<T> = {
  data: Array<T> | undefined;
  getKey: (item: T) => string;
};

export type GetColumnsParams = {
  allRowsSelected: boolean;
  onRowSelect: (key: string, isChecked: boolean) => void;
  onSelectAll: (isChecked: boolean) => void;
  selectedRows: Record<string, boolean>;
};

export const useRowSelection = <T>({ data, getKey }: RowSelectionHookParams<T>) => {
  const [selectedRows, setSelectedRows] = useState<Set<string>>(new Set());

  const handleSelectAll = useCallback(
    (isChecked: boolean) => {
      setSelectedRows((prev) => {
        const newSelected = new Set(prev);

        if (isChecked) {
          data?.forEach((item) => newSelected.add(getKey(item)));
        } else {
          data?.forEach((item) => newSelected.delete(getKey(item)));
        }

        return newSelected;
      });
    },
    [data, getKey],
  );

  const handleRowSelect = useCallback((key: string, isChecked: boolean) => {
    setSelectedRows((prev) => {
      const newSet = new Set(prev);

      if (isChecked) {
        newSet.add(key);
      } else {
        newSet.delete(key);
      }

      return newSet;
    });
  }, []);

  const allRowsSelected = useMemo(() => {
    if (!data || data.length === 0) {
      return false;
    }

    return data.every((item) => selectedRows.has(getKey(item)));
  }, [data, selectedRows, getKey]);

  return {
    allRowsSelected,
    handleRowSelect,
    handleSelectAll,
    selectedRows,
  };
};
