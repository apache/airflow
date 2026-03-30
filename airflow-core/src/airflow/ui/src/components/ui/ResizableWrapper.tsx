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
import type { PropsWithChildren } from "react";
import { useEffect, useRef } from "react";
import { useLocalStorage } from "usehooks-ts";

type ResizableWrapperProps = {
  readonly defaultSize?: { height: number; width: number };
  readonly maxConstraints?: [width: number, height: number];
  readonly storageKey: string;
} & PropsWithChildren;

const DEFAULT_SIZE = { height: 400, width: 500 };
const MAX_SIZE: [number, number] = [1200, 800];

export const MARKDOWN_DIALOG_STORAGE_KEY = "airflow-markdown-dialog-size";

export const ResizableWrapper = ({
  children,
  defaultSize = DEFAULT_SIZE,
  maxConstraints = MAX_SIZE,
  storageKey,
}: ResizableWrapperProps) => {
  const ref = useRef<HTMLDivElement>(null);
  const [storedSize, setStoredSize] = useLocalStorage(storageKey, defaultSize);

  useEffect(() => {
    const el = ref.current;

    if (!el) {
      return undefined;
    }

    let timeoutId: ReturnType<typeof setTimeout>;

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { height, width } = entry.contentRect;

        if (width > 0 && height > 0) {
          clearTimeout(timeoutId);
          timeoutId = setTimeout(() => {
            setStoredSize({ height: Math.round(height), width: Math.round(width) });
          }, 300);
        }
      }
    });

    observer.observe(el);

    return () => {
      observer.disconnect();
      clearTimeout(timeoutId);
    };
  }, [setStoredSize]);

  return (
    <Box
      css={{
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        resize: "both",
      }}
      height={`${storedSize.height}px`}
      maxHeight={`${maxConstraints[1]}px`}
      maxWidth={`${maxConstraints[0]}px`}
      minHeight={`${DEFAULT_SIZE.height}px`}
      minWidth={`${DEFAULT_SIZE.width}px`}
      ref={ref}
      width={`${storedSize.width}px`}
    >
      {children}
    </Box>
  );
};
