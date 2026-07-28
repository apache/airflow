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
import { createContext, useContext, useState, type ReactNode } from "react";

import { Checkbox } from "src/components/ui/Checkbox";

type UseRowSelectionProps<T> = {
  data?: Array<T>;
  getKey: (item: T) => string;
};

export type GetColumnsParams = {
  multiTeam: boolean;
};

type SelectionContextValue = {
  allRowsSelected: boolean;
  onRowSelect: (key: string, isChecked: boolean) => void;
  onSelectAll: (isChecked: boolean) => void;
  selectedRows: Map<string, boolean>;
};

const SelectionContext = createContext<SelectionContextValue | undefined>(undefined);

const useSelectionContext = () => {
  const ctx = useContext(SelectionContext);

  if (ctx === undefined) {
    throw new Error("SelectionRowCheckbox / SelectionHeaderCheckbox must be used inside <SelectionProvider>");
  }

  return ctx;
};

type SelectionProviderProps = {
  readonly allRowsSelected: boolean;
  readonly children: ReactNode;
  readonly onRowSelect: (key: string, isChecked: boolean) => void;
  readonly onSelectAll: (isChecked: boolean) => void;
  readonly selectedRows: Map<string, boolean>;
};

export const SelectionProvider = ({
  allRowsSelected,
  children,
  onRowSelect,
  onSelectAll,
  selectedRows,
}: SelectionProviderProps) => {
  const value: SelectionContextValue = { allRowsSelected, onRowSelect, onSelectAll, selectedRows };

  return <SelectionContext.Provider value={value}>{children}</SelectionContext.Provider>;
};

type SelectionRowCheckboxProps = {
  readonly colorPalette?: string;
  readonly rowKey: string;
};

export const SelectionRowCheckbox = ({ colorPalette, rowKey }: SelectionRowCheckboxProps) => {
  const { onRowSelect, selectedRows } = useSelectionContext();

  return (
    <Checkbox
      borderWidth={1}
      checked={selectedRows.has(rowKey)}
      colorPalette={colorPalette}
      onCheckedChange={(event) => onRowSelect(rowKey, Boolean(event.checked))}
    />
  );
};

type SelectionHeaderCheckboxProps = {
  readonly colorPalette?: string;
};

export const SelectionHeaderCheckbox = ({ colorPalette }: SelectionHeaderCheckboxProps) => {
  const { allRowsSelected, onSelectAll } = useSelectionContext();

  return (
    <Checkbox
      borderWidth={1}
      checked={allRowsSelected}
      colorPalette={colorPalette}
      onCheckedChange={(event) => onSelectAll(Boolean(event.checked))}
    />
  );
};

export const useRowSelection = <T,>({ data = [], getKey }: UseRowSelectionProps<T>) => {
  const [selectedRows, setSelectedRows] = useState<Map<string, boolean>>(new Map());

  const handleRowSelect = (key: string, isChecked: boolean) => {
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
  };

  const handleSelectAll = (isChecked: boolean) => {
    setSelectedRows((prev) => {
      const newMap = new Map(prev);

      if (isChecked) {
        data.forEach((item) => newMap.set(getKey(item), true));
      } else {
        data.forEach((item) => newMap.delete(getKey(item)));
      }

      return newMap;
    });
  };

  const allRowsSelected = data.length > 0 && data.every((item) => selectedRows.has(getKey(item)));

  const clearSelections = () => {
    setSelectedRows(new Map());
  };

  const deselectKeys = (keys: Array<string>) => {
    if (keys.length === 0) {
      return;
    }
    setSelectedRows((prev) => {
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
    selectedRows,
  };
};
