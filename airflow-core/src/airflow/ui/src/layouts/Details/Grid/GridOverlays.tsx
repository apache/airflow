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
import { Box } from "@chakra-ui/react";
import type { RefObject } from "react";

import { CELL_WIDTH, CELL_HEIGHT, BAR_HEADER_HEIGHT, GRID_PADDING_TOP } from "./utils";

type Props = {
  readonly gridHeight: number;
  readonly hoverColRef: RefObject<HTMLDivElement>;
  readonly hoverRowRef: RefObject<HTMLDivElement>;
  readonly navCellRef: RefObject<HTMLDivElement>;
  readonly navColRef: RefObject<HTMLDivElement>;
  readonly navRowRef: RefObject<HTMLDivElement>;
};

export const GridOverlays = ({
  gridHeight,
  hoverColRef,
  hoverRowRef,
  navCellRef,
  navColRef,
  navRowRef,
}: Props) => {
  const cellsTop = GRID_PADDING_TOP + BAR_HEADER_HEIGHT;

  return (
    <Box
      bottom={0}
      isolation="isolate"
      left={0}
      pointerEvents="none"
      position="absolute"
      right={0}
      top={0}
      zIndex={0}
    >
      <Box
        bg="blue.solid/20"
        height={`${CELL_HEIGHT}px`}
        left={0}
        opacity={0}
        pointerEvents="none"
        position="absolute"
        ref={hoverRowRef}
        top={`${cellsTop}px`}
        transition="transform 0.05s linear, opacity 0.1s ease-out"
        width="100%"
        willChange="transform, opacity"
        zIndex={0}
      />
      <Box
        bg="blue.solid/20"
        height={`${gridHeight + BAR_HEADER_HEIGHT}px`}
        opacity={0}
        pointerEvents="none"
        position="absolute"
        ref={hoverColRef}
        right={0}
        top={`${GRID_PADDING_TOP}px`}
        transition="transform 0.05s linear, opacity 0.1s ease-out"
        width={`${CELL_WIDTH}px`}
        willChange="transform, opacity"
        zIndex={0}
      />

      <Box
        bg="blue.solid/20"
        height={`${CELL_HEIGHT}px`}
        left={0}
        opacity={0}
        pointerEvents="none"
        position="absolute"
        ref={navRowRef}
        top={`${cellsTop}px`}
        transition="transform 0.05s linear, opacity 0.1s ease-out"
        width="100%"
        willChange="transform, opacity"
        zIndex={0}
      />
      <Box
        bg="blue.solid/20"
        height={`${gridHeight + BAR_HEADER_HEIGHT}px`}
        opacity={0}
        pointerEvents="none"
        position="absolute"
        ref={navColRef}
        right={0}
        top={`${GRID_PADDING_TOP}px`}
        transition="transform 0.05s linear, opacity 0.1s ease-out"
        width={`${CELL_WIDTH}px`}
        willChange="transform, opacity"
        zIndex={0}
      />
      <Box
        bg="blue.solid/20"
        height={`${CELL_HEIGHT}px`}
        opacity={0}
        pointerEvents="none"
        position="absolute"
        ref={navCellRef}
        right={0}
        top={`${cellsTop}px`}
        transition="transform 0.05s linear, opacity 0.1s ease-out"
        width={`${CELL_WIDTH}px`}
        willChange="transform, opacity"
        zIndex={0}
      />
    </Box>
  );
};
