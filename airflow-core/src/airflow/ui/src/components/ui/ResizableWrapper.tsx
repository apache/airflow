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
import { forwardRef } from "react";
import type { ReactNode } from "react";
import { ResizableBox } from "react-resizable";
import "react-resizable/css/styles.css";

import { usePersistentResizableState } from "src/utils/usePersistentResizableState";

const ResizeHandle = forwardRef<HTMLDivElement>((props, ref) => (
  <Box
    background="linear-gradient(-45deg, transparent 6px, #ccc 6px, #ccc 8px, transparent 8px, transparent 12px, #ccc 12px, #ccc 14px, transparent 14px)"
    bottom={0}
    cursor="se-resize"
    height={5}
    position="absolute"
    ref={ref}
    right={0}
    width={5}
    {...props}
  />
));

type ResizableWrapperProps = {
  readonly children: ReactNode;
  readonly defaultSize?: { height: number; width: number };
  readonly maxConstraints?: [width: number, height: number];
  readonly storageKey: string;
};

const DEFAULT_SIZE = { height: 400, width: 500 };
const MAX_SIZE: [number, number] = [1200, 800];

export const ResizableWrapper = ({
  children,
  defaultSize = DEFAULT_SIZE,
  maxConstraints = MAX_SIZE,
  storageKey,
}: ResizableWrapperProps) => {
  const { handleResize, handleResizeStop, size } = usePersistentResizableState(storageKey, defaultSize);

  return (
    <ResizableBox
      handle={<ResizeHandle />}
      height={size.height}
      maxConstraints={maxConstraints}
      minConstraints={[DEFAULT_SIZE.width, DEFAULT_SIZE.height]}
      onResize={handleResize}
      onResizeStop={handleResizeStop}
      resizeHandles={["se"]}
      style={{
        backgroundColor: "inherit",
        borderRadius: "inherit",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        position: "relative",
      }}
      width={size.width}
    >
      {children}
    </ResizableBox>
  );
};
