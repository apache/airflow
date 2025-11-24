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
import { createContext, useCallback, useContext, useMemo } from "react";

import type { GridInteractionLayerHandle, HoverMode } from "./GridInteractionLayer";

type GridHoverContextType = {
  clearHover: () => void;
  setHover: (rowIndex: number, colIndex: number, mode: HoverMode) => void;
};

const noop = () => {
  // Intentionally empty - no-op function for graceful degradation
};

const defaultContextValue: GridHoverContextType = {
  clearHover: noop,
  setHover: noop,
};

const GridHoverContext = createContext<GridHoverContextType>(defaultContextValue);

// eslint-disable-next-line react-refresh/only-export-components
export const useGridHover = () => useContext(GridHoverContext);

type Props = {
  readonly children: React.ReactNode;
  readonly interactionLayerRef: React.RefObject<GridInteractionLayerHandle | null>;
};

export const GridHoverProvider = ({ children, interactionLayerRef }: Props) => {
  const setHover = useCallback(
    (rowIndex: number, colIndex: number, mode: HoverMode) => {
      interactionLayerRef.current?.setHover({ colIndex, rowIndex }, mode);
    },
    [interactionLayerRef],
  );

  const clearHover = useCallback(() => {
    interactionLayerRef.current?.clearHover();
  }, [interactionLayerRef]);

  const value = useMemo(() => ({ clearHover, setHover }), [clearHover, setHover]);

  return <GridHoverContext.Provider value={value}>{children}</GridHoverContext.Provider>;
};
